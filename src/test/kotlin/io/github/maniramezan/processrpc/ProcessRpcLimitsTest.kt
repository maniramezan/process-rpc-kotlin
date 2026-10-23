package io.github.maniramezan.processrpc

import java.util.concurrent.atomic.AtomicInteger
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
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.put

class ProcessRpcLimitsTest {
	private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
	private val clients = mutableListOf<ProcessRpcClient>()

	@AfterTest
	fun tearDown() {
		clients.forEach(ProcessRpcClient::close)
		scope.cancel()
	}

	@Test
	fun `malformed response envelopes never complete a call successfully`() = runBlocking<Unit> {
		val invalid = listOf(
			"""{"jsonrpc":"2.0","id":1}""",
			"""{"jsonrpc":"2.0","id":1,"result":null,"error":{"code":-1,"message":"bad"}}""",
			"""{"jsonrpc":"2.0","id":1,"error":false}""",
			"""{"jsonrpc":"2.0","id":1,"error":{"code":"-1","message":"bad"}}""",
			"""{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":7}}""",
			"""{"jsonrpc":2.0,"id":1,"result":null}""",
			"""{"id":1,"result":null}""",
			"""{"jsonrpc":"2.0","result":null}""",
			"""{"jsonrpc":"2.0","id":true,"result":null}""",
			"""{"jsonrpc":"2.0","id":{},"result":null}""",
			"""{"jsonrpc":"2.0","id":1,"method":5}""",
			"""{"jsonrpc":"2.0","method":"tick","params":true}""",
			"""{"jsonrpc":"2.0","id":1,"method":"tick","result":null}""",
			"[]",
			"[7]",
			"null",
		)
		for (body in invalid) {
			val client = server()
			client.start()
			val failure = withTimeout(5_000) {
				assertFailsWith<ProcessRpcException> { client.call("respondWith", buildJsonObject { put("body", Json.parseToJsonElement(body)) }) }
			}
			assertTrue(failure.message.orEmpty().contains("Invalid JSON-RPC envelope"), body)
			assertIs<ProcessRpcState.Unavailable>(client.state.value)
		}
	}

	@Test
	fun `explicit null results and response batches remain valid`() = runBlocking<Unit> {
		val client = server()
		client.start()
		val batch = Json.parseToJsonElement("""[{"jsonrpc":"2.0","id":999,"result":"unknown"},{"jsonrpc":"2.0","id":1,"result":null}]""")
		assertEquals(JsonNull, client.call("respondWith", buildJsonObject { put("body", batch) }))
		assertEquals(buildJsonArray { add("healthy") }, client.call("echo", buildJsonArray { add("healthy") }))
	}

	@Test
	fun `string ids cannot complete requests with numeric ids`() = runBlocking<Unit> {
		val client = server()
		client.start()
		val batch = Json.parseToJsonElement("""[{"jsonrpc":"2.0","id":"1","result":"wrong"},{"jsonrpc":"2.0","id":1,"result":"right"}]""")
		assertEquals(JsonPrimitive("right"), client.call("respondWith", buildJsonObject { put("body", batch) }))
	}

	@Test
	fun `null request ids are rejected as requests rather than emitted as notifications`() = runBlocking<Unit> {
		val client = server()
		client.start()
		val error = withTimeout(5_000) { client.call("askClientNull") }.jsonObject
		assertEquals(-32601, error.getValue("code").jsonPrimitive.int)
		assertEquals(buildJsonArray { add("healthy") }, client.call("echo", buildJsonArray { add("healthy") }))
	}

	@Test
	fun `late responses after abandonment cannot complete the next call`() = runBlocking<Unit> {
		val abandoned = CompletableDeferred<Long>()
		val client = server(onCallAbandoned = { id, _ -> abandoned.complete(id) })
		client.start()
		client.call("echo")
		assertFailsWith<ProcessRpcException> { client.call("slow", buildJsonObject { put("millis", 300) }, timeoutMs = 50) }
		withTimeout(2_000) { abandoned.await() }
		assertEquals(buildJsonArray { add("next") }, client.call("echo", buildJsonArray { add("next") }))
	}

	@Test
	fun `concurrent responses are correlated when returned out of order`() = runBlocking<Unit> {
		val client = server()
		client.start()
		client.call("echo")
		val awaitingSecond = async(start = CoroutineStart.UNDISPATCHED) { client.notifications.first { it.method == "awaitingSecond" } }
		val first = async(start = CoroutineStart.UNDISPATCHED) { client.call("outOfOrder", buildJsonArray { add("first") }) }
		withTimeout(5_000) { awaitingSecond.await() }
		val second = async { client.call("echo", buildJsonArray { add("second") }) }
		assertEquals(buildJsonArray { add("second") }, withTimeout(5_000) { second.await() })
		assertEquals(buildJsonArray { add("first") }, first.await())
	}

	@Test
	fun `pending call limit rejects excess work and releases capacity after timeout`() = runBlocking<Unit> {
		val client = server(maxPendingCalls = 1)
		client.start()
		client.call("echo")
		val first = async(start = CoroutineStart.UNDISPATCHED) {
			runCatching { client.call("slow", buildJsonObject { put("millis", 300) }, timeoutMs = 150) }
		}
		val failure = assertFailsWith<ProcessRpcException> { client.call("echo") }
		assertTrue(failure.message.orEmpty().contains("pending call limit"))
		assertTrue(first.await().isFailure)
		assertEquals(buildJsonArray { add("next") }, client.call("echo", buildJsonArray { add("next") }))
	}

	@Test
	fun `write deadline stops a child that does not consume stdin`() = runBlocking<Unit> {
		val client = server(writeTimeoutMs = 150)
		client.start()
		client.call("pauseReading")
		val pid = (client.state.value as ProcessRpcState.Ready).pid
		val failure = withTimeout(2_000) {
			assertFailsWith<ProcessRpcException> { client.notify("large", buildJsonObject { put("payload", "x".repeat(2_000_000)) }) }
		}
		assertTrue(failure.message.orEmpty().contains("write"))
		eventually { !ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false) }
		assertIs<ProcessRpcState.Unavailable>(client.state.value)
	}

	@Test
	fun `oversized outgoing messages fail locally without damaging the process`() = runBlocking<Unit> {
		val client = server(maxMessageBytes = 64)
		client.start()
		client.call("echo")
		val failure = assertFailsWith<ProcessRpcException> { client.notify("large", buildJsonObject { put("payload", "x".repeat(100)) }) }
		assertTrue(failure.message.orEmpty().contains("exceeds"))
		assertIs<ProcessRpcState.Ready>(client.state.value)
		client.call("echo")
	}

	@Test
	fun `call timeout also bounds sending a request`() = runBlocking<Unit> {
		val client = server(writeTimeoutMs = 10_000)
		client.start()
		client.call("pauseReading")
		withTimeout(2_000) {
			assertFailsWith<ProcessRpcException> { client.call("large", buildJsonObject { put("payload", "x".repeat(2_000_000)) }, timeoutMs = 100) }
		}
		assertIs<ProcessRpcState.Unavailable>(client.state.value)
	}

	@Test
	fun `bounded write queue rejects excess notifications`() = runBlocking<Unit> {
		val client = server(maxQueuedWrites = 1)
		client.start()
		client.call("pauseReading")
		val attempts = List(16) {
			async(Dispatchers.IO) { runCatching { client.notify("large", buildJsonObject { put("payload", "x".repeat(500_000)) }) } }
		}
		try {
			eventually { attempts.any { it.isCompleted } }
			val failures = attempts.filter { it.isCompleted }.mapNotNull { it.await().exceptionOrNull() }
			assertTrue(failures.any { it.message.orEmpty().contains("write queue limit") })
		} finally {
			client.close()
			withTimeout(5_000) { attempts.forEach { it.await() } }
		}
	}

	@Test
	fun `cancelling a queued message skips it without stopping the process`() = runBlocking<Unit> {
		val client = server()
		client.start()
		client.call("echo")
		val slow = async(start = CoroutineStart.UNDISPATCHED) { client.call("slow", buildJsonObject { put("millis", 1_000) }) }
		val writing = async(start = CoroutineStart.UNDISPATCHED) { client.notify("large", buildJsonObject { put("payload", "x".repeat(2_000_000)) }) }
		val queued = async(start = CoroutineStart.UNDISPATCHED) { client.notify("remember", buildJsonObject { put("cancelled", true) }) }
		queued.cancelAndJoin()
		withTimeout(5_000) { slow.await(); writing.await() }
		assertEquals(JsonNull, client.call("recall"))
		assertIs<ProcessRpcState.Ready>(client.state.value)
	}

	@Test
	fun `child request floods are bounded without blocking the reader`() = runBlocking<Unit> {
		val client = server(maxQueuedWrites = 1)
		client.start()
		withTimeout(5_000) {
			val failure = assertFailsWith<ProcessRpcException> { client.call("floodRequests") }
			assertTrue(failure.message.orEmpty().contains("write queue limit"))
		}
		assertIs<ProcessRpcState.Unavailable>(client.state.value)
	}

	@Test
	fun `oversized stderr lines are truncated and following lines are preserved`() = runBlocking<Unit> {
		val client = server(maxStandardErrorLineBytes = 8)
		val lines = async(start = CoroutineStart.UNDISPATCHED) { client.standardError.take(3).toList() }
		client.start()
		client.call("stderr", buildJsonObject { put("text", "x".repeat(100_000) + "\r\nnext\rlast\n") })
		assertEquals(listOf("xxxxxxxx [truncated]", "next", "last"), withTimeout(5_000) { lines.await() })
	}

	@Test
	fun `invalid resource limits fail at construction`() {
		assertFailsWith<IllegalArgumentException> { server(maxPendingCalls = 0) }
		assertFailsWith<IllegalArgumentException> { server(maxQueuedWrites = 0) }
		assertFailsWith<IllegalArgumentException> { server(writeTimeoutMs = 0) }
		assertFailsWith<IllegalArgumentException> { server(maxStandardErrorLineBytes = 0) }
		assertFailsWith<IllegalArgumentException> { RestartPolicy.Backoff(stableAfterMs = 0) }
	}

	@Test
	fun `blocked abandonment hooks have a bounded queue`() = runBlocking<Unit> {
		val entered = CompletableDeferred<Unit>()
		val release = CompletableDeferred<Unit>()
		val hooks = AtomicInteger()
		val client = server(maxQueuedWrites = 1, onCallAbandoned = { _, _ ->
			hooks.incrementAndGet()
			entered.complete(Unit)
			release.await()
		})
		client.start()
		client.call("echo")
		try {
			assertFailsWith<ProcessRpcException> { client.call("slow", buildJsonObject { put("millis", 100) }, timeoutMs = 30) }
			withTimeout(5_000) { entered.await() }
			repeat(6) {
				assertFailsWith<ProcessRpcException> { client.call("slow", buildJsonObject { put("millis", 100) }, timeoutMs = 30) }
			}
			release.complete(Unit)
			eventually { hooks.get() == 2 }
			client.call("echo")
			assertEquals(2, hooks.get())
		} finally {
			release.complete(Unit)
		}
	}

	private suspend fun eventually(condition: () -> Boolean) {
		withTimeout(5_000) { while (!condition()) delay(10) }
	}

	private fun server(
		writeTimeoutMs: Long = 5_000,
		maxPendingCalls: Int = 1_024,
		maxQueuedWrites: Int = 256,
		maxStandardErrorLineBytes: Int = 8_192,
		maxMessageBytes: Int = ContentLengthFraming.DEFAULT_MAX_MESSAGE_BYTES,
		onCallAbandoned: (suspend ProcessRpcClient.(Long, String) -> Unit)? = null,
	): ProcessRpcClient {
		val java = ProcessHandle.current().info().command().orElse("java")
		return ProcessRpcClient(
			command = { listOf(java, "-cp", System.getProperty("java.class.path"), "io.github.maniramezan.processrpc.FakeRpcServerKt") },
			scope = scope,
			shutdownTimeoutMs = 100,
			writeTimeoutMs = writeTimeoutMs,
			maxPendingCalls = maxPendingCalls,
			maxQueuedWrites = maxQueuedWrites,
			maxStandardErrorLineBytes = maxStandardErrorLineBytes,
			maxMessageBytes = maxMessageBytes,
			onCallAbandoned = onCallAbandoned,
		).also(clients::add)
	}
}
