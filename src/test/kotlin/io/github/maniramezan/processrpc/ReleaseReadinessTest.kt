package io.github.maniramezan.processrpc

import java.util.concurrent.atomic.AtomicReference
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put

class ReleaseReadinessTest {
	private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
	private val clients = mutableListOf<ProcessRpcClient>()

	@AfterTest
	fun tearDown() {
		clients.forEach(ProcessRpcClient::close)
		scope.cancel()
	}

	@Test
	fun `invalid raw and typed params fail locally without reaching the peer`() = runBlocking<Unit> {
		val client = server()
		client.start()
		for (params in listOf(JsonNull, JsonPrimitive(true), JsonPrimitive(7), JsonPrimitive("text"))) {
			assertEquals(RpcFailureKind.INVALID_PARAMS, assertFailsWith<ProcessRpcException> { client.call("remember", params) }.kind)
			assertEquals(RpcFailureKind.INVALID_PARAMS, assertFailsWith<ProcessRpcException> { client.notify("remember", params) }.kind)
		}
		assertEquals(RpcFailureKind.INVALID_PARAMS, assertFailsWith<ProcessRpcException> { client.callAs<String, String>("remember", "scalar") }.kind)
		assertEquals(RpcFailureKind.INVALID_PARAMS, assertFailsWith<ProcessRpcException> { client.notifyWith("remember", 42) }.kind)
		assertEquals(JsonNull, client.call("recall"))
		val positional = buildJsonArray { add(JsonPrimitive(42)) }
		assertEquals(positional, client.call("echo", positional))
	}

	@Test
	fun `batch request rejections share one response frame and omit notifications`() = runBlocking<Unit> {
		val client = server()
		val tick = async(start = CoroutineStart.UNDISPATCHED) { client.notifications.first() }
		client.start()
		val responses = withTimeout(5_000) { client.call("askClientBatch") }.jsonArray
		assertEquals(2, responses.size)
		assertTrue(responses.all { it.jsonObject.getValue("error").jsonObject["code"] == JsonPrimitive(-32601) })
		assertEquals("tick", tick.await().method)
		client.call("echo")
	}

	@Test
	fun `local remote and lifecycle failures have distinct categories`() = runBlocking<Unit> {
		val client = server(maxMessageBytes = 1_024)
		assertEquals(RpcFailureKind.NOT_RUNNING, assertFailsWith<ProcessRpcException> { client.call("echo") }.kind)
		client.start()
		assertEquals(RpcFailureKind.REMOTE_ERROR, assertFailsWith<ProcessRpcException> { client.call("fail") }.kind)
		assertEquals(RpcFailureKind.DECODING, assertFailsWith<ProcessRpcException> { client.callAs<Int>("echo") }.kind)
		assertEquals(RpcFailureKind.TIMEOUT, assertFailsWith<ProcessRpcException> {
			client.call("slow", buildJsonObject { put("millis", 200) }, timeoutMs = 30)
		}.kind)
		assertEquals(RpcFailureKind.MESSAGE_TOO_LARGE, assertFailsWith<ProcessRpcException> {
			client.notify("large", buildJsonObject { put("value", "x".repeat(2_000)) })
		}.kind)
		assertEquals(RpcFailureKind.PROTOCOL, assertFailsWith<ProcessRpcException> { client.call("badFrame") }.kind)
		assertEquals(RpcFailureKind.PROTOCOL, assertIs<ProcessRpcState.Unavailable>(client.state.value).kind)
		assertEquals(RpcFailureKind.PROTOCOL, assertFailsWith<ProcessRpcException> { client.call("echo") }.kind)
	}

	@Test
	fun `body encoding runs away from the caller dispatcher`() = runBlocking<Unit> {
		val client = server()
		client.start()
		client.call("echo")
		val callerThread = Thread.currentThread()
		val encodingThread = AtomicReference<Thread>()
		val content = mapOf<String, JsonElement>("value" to JsonPrimitive("encoded"))
		val observed = JsonObject(object : Map<String, JsonElement> by content {
			override val entries: Set<Map.Entry<String, JsonElement>>
				get() {
					encodingThread.set(Thread.currentThread())
					return content.entries
				}
		})
		client.notify("remember", observed)
		assertTrue(encodingThread.get() != null && encodingThread.get() !== callerThread)
		assertEquals(JsonPrimitive("encoded"), client.call("recall").jsonObject["value"])
	}

	@Test
	fun `full write queues reject work before serializing its params`() = runBlocking<Unit> {
		val client = server(maxQueuedWrites = 1)
		client.start()
		client.call("pauseReading")
		val writing = async { runCatching { client.notify("large", buildJsonObject { put("value", "x".repeat(2_000_000)) }) } }
		try {
			val current = activeSession(client)
			withTimeout(5_000) { while (current.writeBudget.retainedBytes < 2_000_000 || current.writeBudget.queuedReservations != 0) delay(5) }
			val queued = async { runCatching { client.notify("remember", buildJsonObject { put("value", "queued") }) } }
			withTimeout(5_000) { while (current.writeBudget.queuedReservations == 0) delay(5) }
			val untouched = JsonObject(object : Map<String, JsonElement> by emptyMap() {
				override val entries: Set<Map.Entry<String, JsonElement>>
					get() = error("Rejected params must not be serialized")
			})
			assertEquals(RpcFailureKind.OVERLOADED, assertFailsWith<ProcessRpcException> { client.notify("remember", untouched) }.kind)
			client.close()
			withTimeout(5_000) { queued.await(); writing.await() }
		} finally {
			client.close()
			writing.await()
		}
	}

	@Test
	fun `encoding byte limits count escaped unicode and release rejected reservations`() = runBlocking<Unit> {
		val message = buildJsonObject { put("value", "é\n\"".repeat(100)) }
		val expected = message.toString().encodeToByteArray()
		val budget = RpcWriteBudget(1, expected.size.toLong())
		val first = budget.reserve()
		assertEquals(RpcFailureKind.OVERLOADED, assertFailsWith<ProcessRpcException> { budget.reserve() }.kind)
		assertTrue(expected.contentEquals(encodeRpcMessage(Json, message, expected.size, first, currentCoroutineContext())))
		first.releaseSlot()
		assertEquals(RpcFailureKind.OVERLOADED, assertFailsWith<ProcessRpcException> { budget.reserve() }.kind)
		first.close()
		first.close()
		val third = budget.reserve()
		assertEquals(RpcFailureKind.MESSAGE_TOO_LARGE, assertFailsWith<ProcessRpcException> {
			encodeRpcMessage(Json, message, expected.size - 1, third, currentCoroutineContext())
		}.kind)
		third.close()
		val recovered = budget.reserve()
		assertTrue(expected.contentEquals(encodeRpcMessage(Json, message, expected.size, recovered, currentCoroutineContext())))
		recovered.close()
	}

	@Test
	fun `blocked writers retain their bytes and cancelled queued writes release theirs`() = runBlocking<Unit> {
		val client = server(maxQueuedWriteBytes = 2_100_000)
		client.start()
		client.call("pauseReading")
		val writing = async { runCatching { client.notify("large", buildJsonObject { put("value", "x".repeat(2_000_000)) }) } }
		try {
			val current = activeSession(client)
			withTimeout(5_000) {
				while (current.writeBudget.retainedBytes < 2_000_000) delay(5)
			}
			val queued = async { client.notify("remember", buildJsonObject { put("value", "x".repeat(50_000)) }) }
			withTimeout(5_000) {
				while (current.writeBudget.retainedBytes < 2_050_000) delay(5)
			}
			assertEquals(RpcFailureKind.OVERLOADED, assertFailsWith<ProcessRpcException> {
				client.notify("large", buildJsonObject { put("value", "x".repeat(60_000)) })
			}.kind)
			queued.cancelAndJoin()
			assertTrue(current.writeBudget.retainedBytes < 2_050_000)
			assertIs<ProcessRpcState.Ready>(client.state.value)
			client.close()
			withTimeout(5_000) { writing.await() }
			withTimeout(5_000) { while (current.writeBudget.retainedBytes != 0L) delay(5) }
			client.start()
			client.call("echo")
		} finally {
			client.close()
			writing.await()
		}
	}

	@Test
	fun `notification gaps are reported per collector and exclude earlier notifications`() = runBlocking<Unit> {
		val client = server(notificationBufferCapacity = 2)
		client.start()
		client.call("emit", buildJsonObject { put("count", 5) })
		val entered = CompletableDeferred<Unit>()
		val release = CompletableDeferred<Unit>()
		val events = mutableListOf<RpcNotificationEvent>()
		val stalled = launch(start = CoroutineStart.UNDISPATCHED) {
			client.notificationEvents.collect { event ->
				events += event
				if (events.size == 1) {
					entered.complete(Unit)
					release.await()
				}
			}
		}
		try {
			client.call("emit", buildJsonObject { put("count", 1) })
			withTimeout(5_000) { entered.await() }
			val fast = async(start = CoroutineStart.UNDISPATCHED) { client.notificationEvents.take(2).toList() }
			client.call("emit", buildJsonObject { put("count", 1) })
			client.call("emit", buildJsonObject { put("count", 1) })
			assertTrue(fast.await().all { it.droppedBefore == 0L })
			client.call("emit", buildJsonObject { put("count", 8) })
			release.complete(Unit)
			withTimeout(5_000) { while (events.size < 3) delay(5) }
			assertEquals(listOf(0L, 8L, 0L), events.map { it.droppedBefore })
			client.close()
			client.start()
			client.call("emit", buildJsonObject { put("count", 1) })
			withTimeout(5_000) { while (events.size < 4) delay(5) }
			assertEquals(0L, events.last().droppedBefore)
		} finally {
			release.complete(Unit)
			stalled.cancelAndJoin()
		}
	}

	@Test
	fun `typed serialization failures are classified without blocking the caller`() = runBlocking<Unit> {
		val client = server()
		client.start()
		assertEquals(RpcFailureKind.ENCODING, assertFailsWith<ProcessRpcException> { client.notifyWith("remember", Unencodable()) }.kind)
		assertEquals(JsonNull, client.call("recall"))
	}

	@kotlinx.serialization.Serializable(with = UnencodableSerializer::class)
	private class Unencodable

	private object UnencodableSerializer : KSerializer<Unencodable> {
		override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("Unencodable", PrimitiveKind.STRING)
		override fun serialize(encoder: Encoder, value: Unencodable): Unit = throw SerializationException("scripted encoding failure")
		override fun deserialize(decoder: Decoder): Unencodable = throw SerializationException("scripted decoding failure")
	}

	private fun activeSession(client: ProcessRpcClient): RpcSession =
		ProcessRpcClient::class.java.getDeclaredField("session").apply { isAccessible = true }.get(client) as RpcSession

	private fun server(maxQueuedWrites: Int = 256, maxMessageBytes: Int = ContentLengthFraming.DEFAULT_MAX_MESSAGE_BYTES, maxQueuedWriteBytes: Long = 64L * 1024 * 1024, notificationBufferCapacity: Int = 1_024): ProcessRpcClient {
		val java = ProcessHandle.current().info().command().orElseThrow()
		return ProcessRpcClient(
			command = { listOf(java, "-cp", System.getProperty("java.class.path"), "io.github.maniramezan.processrpc.FakeRpcServerKt") },
			scope = scope,
			maxQueuedWrites = maxQueuedWrites,
			maxMessageBytes = maxMessageBytes,
			maxQueuedWriteBytes = maxQueuedWriteBytes,
			notificationBufferCapacity = notificationBufferCapacity,
			shutdownTimeoutMs = 100,
		).also(clients::add)
	}
}
