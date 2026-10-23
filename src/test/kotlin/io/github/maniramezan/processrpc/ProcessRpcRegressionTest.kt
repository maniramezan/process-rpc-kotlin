package io.github.maniramezan.processrpc

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.assertEquals
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.flow.first
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long

class ProcessRpcRegressionTest {
	private fun server(scope: CoroutineScope, arguments: List<String> = emptyList(), shutdownTimeoutMs: Long = 100): ProcessRpcClient {
		val java = ProcessHandle.current().info().command().orElse("java")
		return ProcessRpcClient(
			command = { listOf(java, "-cp", System.getProperty("java.class.path"), "io.github.maniramezan.processrpc.FakeRpcServerKt") + arguments },
			scope = scope,
			shutdownTimeoutMs = shutdownTimeoutMs,
			callTimeoutMs = 5_000,
		)
	}

	private suspend fun ready(client: ProcessRpcClient): Long = withTimeout(5_000) {
		(client.state.first { it is ProcessRpcState.Ready } as ProcessRpcState.Ready).pid
	}

	private fun alive(pid: Long): Boolean = ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false)

	private fun kill(pid: Long) {
		ProcessHandle.of(pid).ifPresent { it.destroyForcibly() }
	}

	@Test
	fun callTimeoutDuringBlockedWriteKeepsChild() = runBlocking {
		val clientScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
		val client = server(clientScope)
		client.start()
		val pid = ready(client)
		try {
			val busy = clientScope.async { client.call("slow", buildJsonObject { put("millis", 1_500) }) }
			delay(200)

			val error = runCatching { client.call("echo", buildJsonObject { put("blob", "x".repeat(2_000_000)) }, timeoutMs = 300) }.exceptionOrNull()

			assertEquals(RpcFailureKind.TIMEOUT, (error as? ProcessRpcException)?.kind)
			busy.await()
			assertEquals("after", client.call("echo", buildJsonObject { put("value", "after") }).jsonObject.getValue("value").jsonPrimitive.content)
			assertEquals(ProcessRpcState.Ready(pid), client.state.value)
		} finally {
			client.close()
			clientScope.cancel()
		}
	}

	@Test
	fun cancellingCallerDuringBlockedWriteKeepsChild() = runBlocking {
		val clientScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
		val client = server(clientScope)
		client.start()
		val pid = ready(client)
		try {
			val busy = clientScope.async { client.call("slow", buildJsonObject { put("millis", 1_500) }) }
			delay(200)
			val caller = clientScope.launch { client.call("echo", buildJsonObject { put("blob", "x".repeat(2_000_000)) }) }
			delay(300)

			caller.cancelAndJoin()

			busy.await()
			assertEquals("after", client.call("echo", buildJsonObject { put("value", "after") }).jsonObject.getValue("value").jsonPrimitive.content)
			assertEquals(ProcessRpcState.Ready(pid), client.state.value)
		} finally {
			client.close()
			clientScope.cancel()
		}
	}

	@Test
	fun stopFromCancelledFinallyMustKillChild() = runBlocking {
		val clientScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
		val client = server(clientScope)
		client.start()
		val pid = ready(client)
		try {
			val entered = CompletableDeferred<Unit>()
			val owner = launch {
				try {
					entered.complete(Unit)
					awaitCancellation()
				} finally {
					client.stop()
				}
			}
			entered.await()
			owner.cancelAndJoin()
			delay(300)
			assertFalse(alive(pid), "stop() detached the process before cancellation skipped its cleanup; child is alive")
		} finally {
			kill(pid)
			client.close()
			clientScope.cancel()
		}
	}

	@Test
	fun scopeCancellationMustKillChild() = runBlocking {
		val clientScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
		val client = server(clientScope)
		client.start()
		val pid = ready(client)
		try {
			client.call("echo")
			clientScope.cancel()
			delay(300)
			assertFalse(alive(pid), "cancelled drain.join bypassed terminate(), and completion cleared the process handle")
		} finally {
			kill(pid)
			client.close()
			clientScope.cancel()
		}
	}

	@Test
	fun cancelledWriteMustFinishPromptly() = runBlocking {
		val clientScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
		val client = server(clientScope, listOf("sleep"))
		client.start()
		val pid = ready(client)
		val caller = CoroutineScope(SupervisorJob() + Dispatchers.IO)
		val call = caller.launch { runCatching { client.call("echo", buildJsonObject { put("payload", "x".repeat(2_000_000)) }, timeoutMs = 5_000) } }
		try {
			delay(500)
			call.cancel()
			delay(300)
			assertTrue(call.isCompleted, "cancelled call is still blocked writing to a full child stdin pipe")
		} finally {
			kill(pid)
			withTimeout(5_000) { call.join() }
			client.close()
			clientScope.cancel()
			caller.cancel()
		}
	}

	@Test
	fun closeMustEscalateEvenWhenWriteIsBlocked() = runBlocking {
		val clientScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
		if (System.getProperty("os.name").startsWith("Windows")) return@runBlocking
		val client = server(clientScope)
		client.start()
		val pid = ready(client)
		client.call("ignoreTerm")
		val caller = CoroutineScope(SupervisorJob() + Dispatchers.IO)
		val call = caller.launch { runCatching { client.call("echo", buildJsonObject { put("payload", "x".repeat(2_000_000)) }) } }
		delay(500)
		val closer = thread(isDaemon = true) { client.close() }
		try {
			delay(500)
			assertFalse(alive(pid), "close() never scheduled its forcible kill; closer stack: ${closer.stackTrace.joinToString()}")
			assertFalse(closer.isAlive, "close() is blocking despite its nonblocking contract")
		} finally {
			kill(pid)
			closer.join(5_000)
			withTimeout(5_000) { call.join() }
			clientScope.cancel()
			caller.cancel()
		}
	}

	@Test
	fun newGenerationCallsMustSurviveOldGenerationFailure() = runBlocking {
		val clientScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
		val client = server(clientScope)
		client.start()
		ready(client)
		val sessionField = ProcessRpcClient::class.java.getDeclaredField("session").apply { isAccessible = true }
		val pending = (sessionField.get(client) as RpcSession).pending
		val idsField = ProcessRpcClient::class.java.getDeclaredField("ids").apply { isAccessible = true }
		(idsField.get(client) as AtomicLong).set(100)
		val startedFailure = CountDownLatch(1)
		val releaseFailure = CountDownLatch(1)
		val first = CompletableDeferred<JsonElement>()
		first.invokeOnCompletion {
			startedFailure.countDown()
			releaseFailure.await(10, TimeUnit.SECONDS)
		}
		pending[0] = first
		pending[1] = CompletableDeferred()
		val closer = thread { client.close() }
		var newPid: Long? = null
		try {
			assertTrue(startedFailure.await(5, TimeUnit.SECONDS))
			client.start()
			newPid = ready(client)
			val call = async(Dispatchers.IO) {
				runCatching { client.call("slow", buildJsonObject { put("millis", 1_000) }) }
			}
			withTimeout(5_000) { while (!(sessionField.get(client) as RpcSession).pending.containsKey(100)) delay(5) }
			releaseFailure.countDown()
			closer.join(5_000)
			val result = call.await()
			assertTrue(result.isSuccess, "new process call was removed by old process failPending: ${result.exceptionOrNull()}")
		} finally {
			releaseFailure.countDown()
			closer.join(5_000)
			newPid?.let(::kill)
			client.close()
			clientScope.cancel()
		}
	}
	@Test
	fun `new launch writes do not wait for an old blocked writer`() = runBlocking {
		if (System.getProperty("os.name").startsWith("Windows")) return@runBlocking
		val clientScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
		val client = server(clientScope, shutdownTimeoutMs = 5_000)
		client.start()
		val oldPid = ready(client)
		client.call("ignoreTerm")
		val writing = async(Dispatchers.IO) { runCatching { client.notify("large", buildJsonObject { put("payload", "x".repeat(2_000_000)) }) } }
		try {
			delay(300)
			client.close()
			client.start()
			assertEquals(buildJsonArray { add("new") }, withTimeout(2_000) { client.call("echo", buildJsonArray { add("new") }) })
			assertTrue(alive(oldPid))
		} finally {
			kill(oldPid)
			withTimeout(5_000) { writing.await() }
			client.stop()
			clientScope.cancel()
		}
	}

	@Test
	fun `closing from a starting state collector cannot attach a later process`() = runBlocking {
		val clientScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
		val client = server(clientScope)
		val observer = clientScope.launch(Dispatchers.Unconfined, start = CoroutineStart.UNDISPATCHED) {
			client.state.collect { if (it == ProcessRpcState.Starting) client.close() }
		}
		try {
			client.start()
			delay(300)
			assertEquals(ProcessRpcState.Stopped, client.state.value)
		} finally {
			observer.cancel()
			client.close()
			clientScope.cancel()
		}
	}

	@Test
	fun `cancelling stop during its graceful wait still kills the process tree`() = runBlocking {
		if (System.getProperty("os.name").startsWith("Windows")) return@runBlocking
		val clientScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
		val client = server(clientScope)
		client.start()
		val pid = ready(client)
		val descendant = client.call("spawnSleeper").jsonObject.getValue("pid").jsonPrimitive.long
		try {
			client.call("ignoreTerm")
			val stopping = launch(start = CoroutineStart.UNDISPATCHED) { client.stop() }
			delay(50)
			withTimeout(5_000) { stopping.cancelAndJoin() }
			assertFalse(alive(pid))
			assertFalse(alive(descendant))
		} finally {
			kill(pid)
			kill(descendant)
			client.close()
			clientScope.cancel()
		}
	}

}
