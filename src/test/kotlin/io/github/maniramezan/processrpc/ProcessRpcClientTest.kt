package io.github.maniramezan.processrpc

import java.io.File
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ProcessRpcClientTest {
	private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
	private val clients = mutableListOf<ProcessRpcClient>()
	private val cleanups = mutableListOf<() -> Unit>()

	@AfterTest
	fun tearDown() {
		clients.forEach(ProcessRpcClient::close)
		cleanups.forEach { it() }
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

	@Test
	fun `a per-call timeout outlasts the client default for slow operations`() = runBlocking<Unit> {
		val client = fakeServer(callTimeoutMs = 300)
		client.start()

		val slow = client.call("slow", buildJsonObject { put("millis", 800) }, timeoutMs = 5_000)

		assertEquals(true, slow.jsonObject.getValue("done").jsonPrimitive.content.toBoolean())
		val error = assertFailsWith<ProcessRpcException> { client.call("slow", buildJsonObject { put("millis", 800) }) }
		assertTrue(error.message.orEmpty().contains("slow"))
	}

	@Test
	fun `error responses carry the RPC code`() = runBlocking<Unit> {
		val client = fakeServer()
		client.start()

		val error = assertFailsWith<ProcessRpcException> { client.call("fail") }

		assertEquals(-32000, error.rpcCode)
		assertEquals("failed on purpose", error.message)
		assertEquals("scripted", error.data?.jsonObject?.getValue("detail")?.jsonPrimitive?.content)
	}

	@Test
	fun `calling before start fails fast`() = runBlocking<Unit> {
		val client = fakeServer(callTimeoutMs = 10_000)

		val error = withTimeout(2_000) { assertFailsWith<ProcessRpcException> { client.call("echo") } }

		assertTrue(error.message.orEmpty().contains("start()"))
	}

	@Test
	fun `cancelling the caller propagates cancellation instead of an RPC error`() = runBlocking<Unit> {
		val client = fakeServer()
		client.start()
		client.call("echo")
		val thrown = CompletableDeferred<Throwable>()
		val job = scope.launch {
			try {
				client.call("slow", buildJsonObject { put("millis", 5_000) })
			} catch (error: Throwable) {
				thrown.complete(error)
				throw error
			}
		}
		delay(300)

		job.cancel()

		assertIs<CancellationException>(withTimeout(5_000) { thrown.await() })
	}

	@Test
	fun `an outer timeout is not reported as an RPC failure`() = runBlocking<Unit> {
		val client = fakeServer()
		client.start()

		val result = withTimeoutOrNull(300) { client.call("slow", buildJsonObject { put("millis", 5_000) }) }

		assertNull(result)
	}

	@Test
	fun `concurrent starts launch one process`() = runBlocking<Unit> {
		val launches = AtomicInteger()
		val client = fakeServer(onLaunch = { launches.incrementAndGet() })

		List(16) { thread { client.start() } }.forEach(Thread::join)
		client.call("echo")

		assertEquals(1, launches.get())
	}

	@Test
	fun `closing while starting stops the launched process`() = runBlocking<Unit> {
		val launching = CountDownLatch(1)
		val release = CountDownLatch(1)
		val client = fakeServer(onLaunch = { launching.countDown(); release.await() })
		client.start()
		launching.await()

		client.close()
		release.countDown()
		delay(500)

		assertEquals(ProcessRpcState.Stopped, client.state.value)
		eventually { fakeChildren().isEmpty() }
	}

	@Test
	fun `a malformed message body is skipped`() = runBlocking<Unit> {
		val client = fakeServer()
		client.start()

		val result = client.call("garbage")

		assertEquals(true, result.jsonObject.getValue("survived").jsonPrimitive.content.toBoolean())
	}

	@Test
	fun `a framing error marks the client unavailable and stops the child`() = runBlocking<Unit> {
		val client = fakeServer()
		client.start()
		val pid = readyPid(client)

		val error = withTimeout(5_000) { assertFailsWith<ProcessRpcException> { client.call("badFrame") } }

		assertTrue(error.message.orEmpty().contains("protocol error"))
		assertIs<ProcessRpcState.Unavailable>(client.state.value)
		eventually { !isAlive(pid) }
	}

	@Test
	fun `a crashed child fails pending calls`() = runBlocking<Unit> {
		val client = fakeServer()
		client.start()

		val error = withTimeout(5_000) { assertFailsWith<ProcessRpcException> { client.call("exit") } }

		assertTrue(error.message.orEmpty().contains("exited"))
	}

	@Test
	fun `closing fails in-flight calls`() = runBlocking<Unit> {
		val client = fakeServer()
		client.start()
		client.call("echo")
		val call = scope.async { runCatching { client.call("slow", buildJsonObject { put("millis", 5_000) }) } }
		delay(300)

		client.close()

		val error = withTimeout(2_000) { call.await() }.exceptionOrNull()
		assertIs<ProcessRpcException>(error)
		assertTrue(error.message.orEmpty().contains("stopped"))
	}

	@Test
	fun `a single-threaded scope is not blocked by the reader`() = runBlocking<Unit> {
		val executor = Executors.newSingleThreadExecutor()
		cleanups += { executor.shutdownNow() }
		val client = fakeServer(scope = CoroutineScope(SupervisorJob() + executor.asCoroutineDispatcher()))
		client.start()

		val error = withTimeout(5_000) { client.call("askClient") }.jsonObject

		assertEquals(-32601, error.getValue("code").jsonPrimitive.int)
	}

	@Test
	fun `closing stops the child's descendants`() = runBlocking<Unit> {
		val client = fakeServer()
		client.start()
		val sleeper = client.call("spawnSleeper").jsonObject.getValue("pid").jsonPrimitive.long

		client.close()

		eventually { !isAlive(sleeper) }
	}

	@Test
	fun `stop lets the child exit after closing its input`() = runBlocking<Unit> {
		val client = fakeServer(shutdownTimeoutMs = 5_000)
		client.start()
		val pid = readyPid(client)

		val started = System.nanoTime()
		client.stop()

		assertTrue((System.nanoTime() - started) / 1_000_000 < 4_000)
		assertEquals(ProcessRpcState.Stopped, client.state.value)
		assertTrue(!isAlive(pid))
	}

	@Test
	fun `closing kills a child that ignores termination`() = runBlocking<Unit> {
		if (System.getProperty("os.name").startsWith("Windows")) return@runBlocking
		val client = fakeServer(shutdownTimeoutMs = 500)
		client.start()
		val pid = readyPid(client)
		client.call("ignoreTerm")

		client.close()

		eventually { !isAlive(pid) }
	}

	@Test
	fun `standard error is published and explains an exit`() = runBlocking<Unit> {
		val client = fakeServer()
		val lines = scope.async { client.standardError.take(2).toList() }
		yield()
		client.start()

		assertFailsWith<ProcessRpcException> { client.call("crash") }

		assertEquals(listOf("first problem", "fatal: scripted crash"), withTimeout(5_000) { lines.await() })
		val unavailable = assertIs<ProcessRpcState.Unavailable>(client.state.value)
		assertEquals(3, unavailable.exitCode)
		assertTrue(unavailable.reason.contains("exited with code 3"))
		assertTrue(unavailable.reason.contains("fatal: scripted crash"))
	}

	@Test
	fun `typed calls encode params and decode results`() = runBlocking<Unit> {
		val client = fakeServer()
		val tick = scope.async { client.notifications.first().paramsAs<Tick>() }
		yield()
		client.start()

		val echoed = client.callAs<Greeting, Greeting>("echo", Greeting("Ada"))
		client.notifyWith("remember", Greeting("Grace"))
		val recalled = client.callAs<Greeting>("recall")
		client.call("emit", buildJsonObject { put("count", 1) })

		assertEquals(Greeting("Ada"), echoed)
		assertEquals(Greeting("Grace"), recalled)
		assertEquals(Tick(0), withTimeout(5_000) { tick.await() })
	}

	@Test
	fun `a result that does not decode is an RPC failure`() = runBlocking<Unit> {
		val client = fakeServer()
		client.start()

		val error = assertFailsWith<ProcessRpcException> { client.callAs<Greeting>("echo", buildJsonObject { put("other", 1) }) }

		assertTrue(error.message.orEmpty().contains("decode"))
	}

	@Test
	fun `the child runs in the given directory and environment`() = runBlocking<Unit> {
		val directory = Files.createTempDirectory("process-rpc").toFile()
		cleanups += { directory.deleteRecursively() }
		val systemRoot = System.getenv("SystemRoot")?.let { mapOf("SystemRoot" to it) }.orEmpty()
		val client = fakeServer(workingDirectory = directory, inheritEnvironment = false, environment = mapOf("ONLY" to "1") + systemRoot)
		client.start()

		val cwd = client.call("cwd").jsonPrimitive.content
		val env = client.call("env", buildJsonObject { put("names", buildJsonArray { add("ONLY"); add("PATH") }) }).jsonObject

		assertEquals(directory.canonicalPath, File(cwd).canonicalPath)
		assertEquals("1", env.getValue("ONLY").jsonPrimitive.content)
		assertEquals(JsonNull, env.getValue("PATH"))
	}

	@Test
	fun `the restart policy relaunches a child that exits`() = runBlocking<Unit> {
		val client = fakeServer(restartPolicy = RestartPolicy.Backoff(initialDelayMs = 100))
		client.start()
		val first = readyPid(client)

		assertFailsWith<ProcessRpcException> { client.call("exit") }
		eventually { (client.state.value as? ProcessRpcState.Ready)?.pid.let { it != null && it != first } }

		assertEquals("hi", client.call("echo", buildJsonObject { put("value", "hi") }).jsonObject.getValue("value").jsonPrimitive.content)
	}

	@Test
	fun `the restart policy gives up after its limit`() = runBlocking<Unit> {
		val launches = AtomicInteger()
		val client = ProcessRpcClient(
			command = { launches.incrementAndGet(); listOf("/nonexistent/process-rpc-test") },
			scope = scope,
			restartPolicy = RestartPolicy.Backoff(maxRestarts = 2, initialDelayMs = 50),
		).also(clients::add)
		client.start()

		eventually { launches.get() == 3 }
		delay(500)

		assertEquals(3, launches.get())
		assertIs<ProcessRpcState.Unavailable>(client.state.value)
	}

	@Test
	fun `closing cancels a pending restart`() = runBlocking<Unit> {
		val client = fakeServer(restartPolicy = RestartPolicy.Backoff(initialDelayMs = 300))
		client.start()
		assertFailsWith<ProcessRpcException> { client.call("exit") }

		client.close()
		delay(800)

		assertEquals(ProcessRpcState.Stopped, client.state.value)
	}

	@Test
	fun `an abandoned call is reported so the child can be told`() = runBlocking<Unit> {
		val abandoned = CompletableDeferred<Pair<Long, String>>()
		val client = fakeServer(onCallAbandoned = { id, method -> abandoned.complete(id to method) })
		client.start()

		assertFailsWith<ProcessRpcException> { client.call("slow", buildJsonObject { put("millis", 2_000) }, timeoutMs = 200) }

		assertEquals("slow", withTimeout(5_000) { abandoned.await() }.second)
	}

	@Test
	fun `a message over the size limit is a protocol error`() = runBlocking<Unit> {
		val client = fakeServer(maxMessageBytes = 64)
		client.start()

		val error = assertFailsWith<ProcessRpcException> { client.call("large") }

		assertTrue(error.message.orEmpty().contains("exceeds"))
		assertIs<ProcessRpcState.Unavailable>(client.state.value)
	}

	@Serializable
	private data class Greeting(val name: String)

	@Serializable
	private data class Tick(val index: Int)

	private suspend fun readyPid(client: ProcessRpcClient): Long {
		client.call("echo")
		return (client.state.value as ProcessRpcState.Ready).pid
	}

	private fun isAlive(pid: Long): Boolean = ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false)

	private fun fakeChildren(): List<ProcessHandle> = ProcessHandle.current().children()
		.filter { it.info().commandLine().orElse("").contains("FakeRpcServerKt") }
		.toList()

	private suspend fun eventually(timeoutMs: Long = 5_000, condition: () -> Boolean) {
		withTimeout(timeoutMs) {
			while (!condition()) delay(50)
		}
	}

	private fun fakeServer(
		notificationBufferCapacity: Int = 1_024,
		callTimeoutMs: Long = 10_000,
		shutdownTimeoutMs: Long = 2_000,
		scope: CoroutineScope = this.scope,
		onLaunch: () -> Unit = {},
		workingDirectory: File? = null,
		inheritEnvironment: Boolean = true,
		environment: Map<String, String> = emptyMap(),
		restartPolicy: RestartPolicy = RestartPolicy.Never,
		maxMessageBytes: Int = ContentLengthFraming.DEFAULT_MAX_MESSAGE_BYTES,
		onCallAbandoned: (suspend ProcessRpcClient.(Long, String) -> Unit)? = null,
	): ProcessRpcClient {
		val java = ProcessHandle.current().info().command().orElse("java")
		val classPath = System.getProperty("java.class.path")
		return ProcessRpcClient(
			command = {
				onLaunch()
				listOf(java, "-cp", classPath, "io.github.maniramezan.processrpc.FakeRpcServerKt")
			},
			scope = scope,
			callTimeoutMs = callTimeoutMs,
			notificationBufferCapacity = notificationBufferCapacity,
			shutdownTimeoutMs = shutdownTimeoutMs,
			workingDirectory = workingDirectory,
			inheritEnvironment = inheritEnvironment,
			environment = environment,
			restartPolicy = restartPolicy,
			maxMessageBytes = maxMessageBytes,
			onCallAbandoned = onCallAbandoned,
		).also(clients::add)
	}
}
