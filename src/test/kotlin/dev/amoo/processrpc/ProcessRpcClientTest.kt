package io.github.maniramezan.processrpc

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals

class ProcessRpcClientTest {
	private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
	private val clients = mutableListOf<ProcessRpcClient>()

	@AfterTest
	fun tearDown() {
		clients.forEach(ProcessRpcClient::close)
		scope.cancel()
	}

	@Test
	fun `notifications sent before a response are delivered in order`() = runBlocking<Unit> {
		val client = fakeServer()
		val ticks = scope.async { client.notifications.take(3).toList() }
		yield()
		client.start()

		val result = client.call("emit", buildJsonObject { put("count", 3) })

		assertEquals(3, result.jsonObject.getValue("sent").jsonPrimitive.int)
		val received = withTimeout(5_000) { ticks.await() }
		assertEquals(listOf("tick", "tick", "tick"), received.map { it.method })
		assertEquals(listOf(0, 1, 2), received.map { it.params.jsonObject.getValue("index").jsonPrimitive.int })
	}

	@Test
	fun `a stalled collector does not block responses`() = runBlocking<Unit> {
		val client = fakeServer(notificationBufferCapacity = 4)
		val gate = CompletableDeferred<Unit>()
		scope.launch { client.notifications.collect { gate.await() } }
		yield()
		client.start()

		val result = withTimeout(5_000) { client.call("emit", buildJsonObject { put("count", 100) }) }

		assertEquals(100, result.jsonObject.getValue("sent").jsonPrimitive.int)
		gate.complete(Unit)
	}

	@Test
	fun `requests from the child are rejected as method not found`() = runBlocking<Unit> {
		val client = fakeServer()
		client.start()

		val error = client.call("askClient").jsonObject

		assertEquals(-32601, error.getValue("code").jsonPrimitive.int)
	}

	@Test
	fun `notify reaches the child without a response`() = runBlocking<Unit> {
		val client = fakeServer()
		client.start()

		client.notify("remember", buildJsonObject { put("runId", "run-7") })

		assertEquals("run-7", client.call("recall").jsonObject.getValue("runId").jsonPrimitive.content)
	}

	@Test
	fun `existing collectors keep receiving after a restart`() = runBlocking<Unit> {
		val client = fakeServer()
		val ticks = scope.async { client.notifications.take(2).toList() }
		yield()
		client.start()
		client.call("emit", buildJsonObject { put("count", 1) })

		client.close()
		client.start()
		client.call("emit", buildJsonObject { put("count", 1) })

		assertEquals(2, withTimeout(5_000) { ticks.await() }.size)
	}

	private fun fakeServer(notificationBufferCapacity: Int = 1_024): ProcessRpcClient {
		val java = ProcessHandle.current().info().command().orElse("java")
		val classPath = System.getProperty("java.class.path")
		return ProcessRpcClient(
			command = { listOf(java, "-cp", classPath, "io.github.maniramezan.processrpc.FakeRpcServerKt") },
			scope = scope,
			callTimeoutMs = 10_000,
			notificationBufferCapacity = notificationBufferCapacity,
		).also(clients::add)
	}
}
