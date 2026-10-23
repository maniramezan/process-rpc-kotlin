package io.github.maniramezan.processrpc

import java.io.BufferedInputStream
import java.io.File
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.pow
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.put

/** The lifecycle of the child process behind a [ProcessRpcClient], published on [ProcessRpcClient.state]. */
public sealed interface ProcessRpcState {
	/** No child is running: the client was never started, or was closed or stopped. Calls fail immediately. */
	public data object Stopped : ProcessRpcState

	/** [ProcessRpcClient.start] was called and the child is being launched. Calls wait for it to settle. */
	public data object Starting : ProcessRpcState

	/**
	 * The child process [pid] was launched and its pipes are connected. This does not mean the child has
	 * finished its own initialization; protocols that need that should make a handshake call first.
	 */
	public data class Ready(val pid: Long) : ProcessRpcState

	/**
	 * The child failed to launch, exited, or broke the wire protocol. [reason] includes the last lines the
	 * child wrote to standard error, and [exitCode] is set when the child exited and its code is known.
	 * Calls fail immediately until [ProcessRpcClient.start] is called again or the [RestartPolicy] restarts it.
	 */
	public data class Unavailable(val reason: String, val exitCode: Int? = null) : ProcessRpcState
}

/**
 * A failed call or notification. [rpcCode] and [data] carry the JSON-RPC `error.code` and `error.data`
 * when the child answered with an error response; both are `null` for transport failures, such as a
 * timeout or a child that stopped.
 */
public class ProcessRpcException(
	public val rpcCode: Int?,
	message: String,
	cause: Throwable? = null,
	public val data: JsonElement? = null,
) : Exception(message, cause)

/** A JSON-RPC notification (a message with a `method` and no `id`) sent by the child process. */
public data class RpcNotification(val method: String, val params: JsonElement)

/** What a [ProcessRpcClient] does when its child exits or fails without [ProcessRpcClient.close] or [ProcessRpcClient.stop]. */
public sealed interface RestartPolicy {
	/** Stay [ProcessRpcState.Unavailable] until the application calls [ProcessRpcClient.start]. */
	public data object Never : RestartPolicy

	/**
	 * Relaunch after `initialDelayMs`, doubling the delay after each consecutive failure up to `maxDelayMs`,
	 * and give up after `maxRestarts` consecutive restarts. A child that stays up for `stableAfterMs`
	 * resets the count, as does calling [ProcessRpcClient.start]. The client reports
	 * [ProcessRpcState.Unavailable] during each delay, so calls made then fail rather than wait.
	 */
	public data class Backoff(
		val maxRestarts: Int = 5,
		val initialDelayMs: Long = 500,
		val maxDelayMs: Long = 30_000,
		val stableAfterMs: Long = 60_000,
	) : RestartPolicy {
		init {
			require(maxRestarts >= 0) { "maxRestarts must not be negative" }
			require(initialDelayMs >= 0 && maxDelayMs >= initialDelayMs) { "Delays must satisfy 0 <= initialDelayMs <= maxDelayMs" }
		}

		internal fun delayBefore(restart: Int): Long =
			(initialDelayMs * 2.0.pow(restart)).coerceAtMost(maxDelayMs.toDouble()).toLong()
	}
}

/**
 * Supervises a child process that speaks JSON-RPC 2.0 over Content-Length-framed stdio.
 *
 * Requests are correlated to responses by ID. Notifications from the child are published on
 * [notifications]; when collectors fall more than `notificationBufferCapacity` messages behind, the
 * oldest notifications are dropped, so protocols that cannot tolerate loss should carry sequence
 * numbers and offer a replay method.
 *
 * While a child runs, the client blocks two threads from [Dispatchers.IO]: one reads standard output
 * and one drains standard error. Neither runs on [scope]'s own dispatcher, so a UI scope is safe.
 *
 * @param command builds the command line. It is invoked on every launch, so it can re-resolve the binary,
 *   for example with [BundledBinaryLocator].
 * @param environment variables added to the child's environment, overriding inherited ones.
 * @param scope runs the client's background work. The client never cancels it, so a closed client can
 *   be started again; cancel it yourself when the client is no longer needed.
 * @param callTimeoutMs how long [call] waits for a response, and how long calls wait for a starting child.
 * @param notificationBufferCapacity how far collectors of [notifications] may fall behind before the
 *   oldest notifications are dropped.
 * @param shutdownTimeoutMs how long [close] and [stop] give the child at each shutdown step before
 *   escalating to a forcible kill.
 * @param workingDirectory the child's working directory, or `null` to inherit this process's.
 * @param inheritEnvironment whether the child starts from this process's environment. When `false`, it
 *   sees only [environment].
 * @param restartPolicy what to do when the child exits or fails on its own.
 * @param maxMessageBytes the largest message body accepted from the child. A larger Content-Length is
 *   treated as a protocol error.
 * @param onCallAbandoned invoked when a [call] whose request was sent times out or its caller is
 *   cancelled, with the request ID and method, so the protocol can tell the child to stop working on
 *   it, for example by sending an LSP-style `$/cancelRequest` notification. It runs on [scope] and its
 *   failures are ignored.
 */
public class ProcessRpcClient(
	private val command: () -> List<String>,
	private val environment: Map<String, String> = emptyMap(),
	private val scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
	private val callTimeoutMs: Long = 30_000,
	notificationBufferCapacity: Int = 1_024,
	private val shutdownTimeoutMs: Long = 2_000,
	private val workingDirectory: File? = null,
	private val inheritEnvironment: Boolean = true,
	private val restartPolicy: RestartPolicy = RestartPolicy.Never,
	private val maxMessageBytes: Int = ContentLengthFraming.DEFAULT_MAX_MESSAGE_BYTES,
	private val onCallAbandoned: (suspend ProcessRpcClient.(requestId: Long, method: String) -> Unit)? = null,
) : AutoCloseable {
	private val json = Json { ignoreUnknownKeys = true }
	private val ids = AtomicLong(1)
	private val pending = ConcurrentHashMap<Long, CompletableDeferred<JsonElement>>()
	private val writeLock = Mutex()
	private val lifecycleLock = Any()
	private var generation = 0L
	private var restarts = 0
	private val mutableState = MutableStateFlow<ProcessRpcState>(ProcessRpcState.Stopped)

	/** The child's lifecycle. Starts as [ProcessRpcState.Stopped]. */
	public val state: StateFlow<ProcessRpcState> = mutableState.asStateFlow()
	private val mutableNotifications = MutableSharedFlow<RpcNotification>(
		extraBufferCapacity = notificationBufferCapacity,
		onBufferOverflow = BufferOverflow.DROP_OLDEST,
	)

	/** Notifications sent by the child process, in arrival order. Hot: subscribe before calling [start] to see every message. */
	public val notifications: SharedFlow<RpcNotification> = mutableNotifications.asSharedFlow()
	private val mutableStandardError = MutableSharedFlow<String>(
		extraBufferCapacity = STANDARD_ERROR_BUFFER_LINES,
		onBufferOverflow = BufferOverflow.DROP_OLDEST,
	)

	/**
	 * Lines the child writes to standard error, across restarts. Hot and lossy like [notifications]: lines
	 * written while no one collects are dropped, and slow collectors lose the oldest lines.
	 */
	public val standardError: SharedFlow<String> = mutableStandardError.asSharedFlow()

	@Volatile
	private var process: Process? = null

	/**
	 * Launches the child in the background without blocking; observe [state] or just [call], which waits
	 * for the launch. Does nothing while the child is starting or ready.
	 */
	public fun start() {
		launch(restartOf = null)
	}

	/**
	 * Calls [method] and waits for its response. [timeoutMs] overrides the client's `callTimeoutMs` for
	 * this call, for operations known to take minutes, such as builds; it bounds the wait for the
	 * response, not for the process to become ready.
	 *
	 * @throws ProcessRpcException when the child answers with an error, the call times out, or the child
	 *   is not running or stops before answering. Cancelling the caller cancels the call as usual.
	 */
	public suspend fun call(method: String, params: JsonElement = JsonObject(emptyMap()), timeoutMs: Long? = null): JsonElement {
		val current = awaitProcess()
		val id = ids.getAndIncrement()
		val deferred = CompletableDeferred<JsonElement>()
		pending[id] = deferred
		val request = buildJsonObject {
			put("jsonrpc", "2.0")
			put("id", id)
			put("method", method)
			put("params", params)
		}
		var sent = false
		try {
			if (process !== current) throw ProcessRpcException(null, "RPC process stopped")
			write(current, request)
			sent = true
			val timeout = timeoutMs ?: callTimeoutMs
			return withTimeoutOrNull(timeout) { deferred.await() }
				?: throw ProcessRpcException(null, "RPC call '$method' timed out after $timeout ms")
		} catch (error: CancellationException) {
			throw error
		} catch (error: ProcessRpcException) {
			throw error
		} catch (error: Exception) {
			throw ProcessRpcException(null, "RPC call '$method' failed: ${error.message}", error)
		} finally {
			val abandoned = pending.remove(id) != null && sent && !deferred.isCompleted
			val hook = onCallAbandoned
			if (abandoned && hook != null) {
				scope.launch(Dispatchers.IO) { runCatching { hook(this@ProcessRpcClient, id, method) } }
			}
		}
	}

	/**
	 * Sends a JSON-RPC notification, which has no ID and receives no response.
	 *
	 * @throws ProcessRpcException when the child is not running or the message cannot be written.
	 */
	public suspend fun notify(method: String, params: JsonElement = JsonObject(emptyMap())) {
		val current = awaitProcess()
		val notification = buildJsonObject {
			put("jsonrpc", "2.0")
			put("method", method)
			put("params", params)
		}
		try {
			write(current, notification)
		} catch (error: CancellationException) {
			throw error
		} catch (error: Exception) {
			throw ProcessRpcException(null, "RPC notification '$method' failed: ${error.message}", error)
		}
	}

	/**
	 * Stops the child without waiting: fails outstanding calls, signals the child and its descendants to
	 * terminate, and forcibly kills any that are still alive after `shutdownTimeoutMs`. Existing
	 * collectors of [notifications] and [standardError] stay subscribed, and [start] may be called again.
	 */
	override fun close() {
		val child = detach() ?: return
		if (writeLock.tryLock()) {
			try {
				runCatching { child.outputStream.close() }
			} finally {
				writeLock.unlock()
			}
		}
		terminate(child)
	}

	/**
	 * Stops the child gracefully and waits for it to exit: closes its standard input, gives it
	 * `shutdownTimeoutMs` to exit, then signals it to terminate, and finally kills it and its
	 * descendants forcibly.
	 */
	public suspend fun stop() {
		val child = detach() ?: return
		withContext(Dispatchers.IO) {
			val descendants = child.descendants().toList()
			withTimeoutOrNull(shutdownTimeoutMs) {
				writeLock.withLock { runCatching { child.outputStream.close() } }
				child.onExit().await()
			}
			if (child.isAlive) child.destroy()
			descendants.forEach(ProcessHandle::destroy)
			withTimeoutOrNull(shutdownTimeoutMs) { exits(child.toHandle(), descendants).await() }
			if (child.isAlive) child.destroyForcibly()
			descendants.filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly)
		}
	}

	// restartOf is null for an explicit start(), or the generation whose failure the restart policy is replacing.
	private fun launch(restartOf: Long?) {
		val launched = synchronized(lifecycleLock) {
			if (restartOf == null) {
				val current = mutableState.value
				if (current is ProcessRpcState.Starting || current is ProcessRpcState.Ready) return
				restarts = 0
			} else if (generation != restartOf) {
				return
			}
			mutableState.value = ProcessRpcState.Starting
			++generation
		}
		scope.launch(Dispatchers.IO) { runProcess(launched) }.invokeOnCompletion { cause ->
			if (cause != null) settle(launched, "RPC process launch was cancelled", exitCode = null)
		}
	}

	private fun detach(): Process? {
		val child = synchronized(lifecycleLock) {
			generation++
			mutableState.value = ProcessRpcState.Stopped
			process.also { process = null }
		}
		failPending("RPC process stopped")
		return child
	}

	private fun terminate(child: Process) {
		val descendants = child.descendants().toList()
		child.destroy()
		descendants.forEach(ProcessHandle::destroy)
		exits(child.toHandle(), descendants)
			.completeOnTimeout(null, shutdownTimeoutMs, TimeUnit.MILLISECONDS)
			.thenRun { (descendants + child.toHandle()).filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly) }
	}

	private fun exits(child: ProcessHandle, descendants: List<ProcessHandle>): CompletableFuture<Void?> =
		CompletableFuture.allOf(*(descendants + child).map(ProcessHandle::onExit).toTypedArray())

	private suspend fun awaitProcess(): Process {
		val settled = withTimeoutOrNull(callTimeoutMs) { state.first { it !is ProcessRpcState.Starting } }
			?: throw ProcessRpcException(null, "RPC process did not start within $callTimeoutMs ms")
		return when (settled) {
			is ProcessRpcState.Ready -> process ?: throw ProcessRpcException(null, "RPC process is not running")
			is ProcessRpcState.Unavailable -> throw ProcessRpcException(null, settled.reason)
			else -> throw ProcessRpcException(null, "RPC process is not running; call start() first")
		}
	}

	private suspend fun runProcess(launched: Long) {
		val child = try {
			val invocation = command()
			require(invocation.isNotEmpty()) { "Process command must not be empty" }
			ProcessBuilder(invocation).redirectErrorStream(false).apply {
				workingDirectory?.let(::directory)
				environment().apply {
					if (!inheritEnvironment) clear()
					putAll(this@ProcessRpcClient.environment)
				}
			}.start()
		} catch (error: Exception) {
			if (settle(launched, "Failed to start RPC process: ${error.message}", exitCode = null)) scheduleRestart(launched, uptimeMs = 0)
			return
		}
		val attached = synchronized(lifecycleLock) {
			if (generation == launched) {
				process = child
				mutableState.value = ProcessRpcState.Ready(child.pid())
			}
			generation == launched
		}
		if (!attached) {
			terminate(child)
			return
		}
		val startedAt = System.nanoTime()
		val tail = ArrayDeque<String>()
		val drain = scope.launch(Dispatchers.IO) {
			child.errorStream.bufferedReader().useLines { lines ->
				lines.forEach { line ->
					synchronized(tail) {
						if (tail.size == STANDARD_ERROR_TAIL_LINES) tail.removeFirst()
						tail.addLast(line)
					}
					mutableStandardError.tryEmit(line)
				}
			}
		}
		val protocolError = try {
			readMessages(child, BufferedInputStream(child.inputStream))
			null
		} catch (error: Exception) {
			error
		}
		val exitCode = if (protocolError == null && child.waitFor(EXIT_GRACE_MS, TimeUnit.MILLISECONDS)) child.exitValue() else null
		withTimeoutOrNull(EXIT_GRACE_MS) { drain.join() }
		val summary = when {
			protocolError != null -> "RPC protocol error: ${protocolError.message}"
			exitCode != null -> "RPC process exited with code $exitCode"
			else -> "RPC process closed its output unexpectedly"
		}
		val output = synchronized(tail) { tail.joinToString("\n") }
		val reason = if (output.isEmpty()) summary else "$summary; last standard error output:\n$output"
		if (settle(launched, reason, exitCode)) {
			terminate(child)
			scheduleRestart(launched, uptimeMs = (System.nanoTime() - startedAt) / 1_000_000)
		}
	}

	// Marks the client unavailable unless a newer start() or a close() has already replaced this run.
	private fun settle(launched: Long, reason: String, exitCode: Int?): Boolean {
		synchronized(lifecycleLock) {
			if (generation != launched) return false
			process = null
			mutableState.value = ProcessRpcState.Unavailable(reason, exitCode)
		}
		failPending(reason)
		return true
	}

	private fun scheduleRestart(launched: Long, uptimeMs: Long) {
		val policy = restartPolicy as? RestartPolicy.Backoff ?: return
		val delayMs = synchronized(lifecycleLock) {
			if (generation != launched) return
			if (uptimeMs >= policy.stableAfterMs) restarts = 0
			if (restarts >= policy.maxRestarts) return
			policy.delayBefore(restarts++)
		}
		scope.launch(Dispatchers.IO) {
			delay(delayMs)
			launch(restartOf = launched)
		}
	}

	private suspend fun write(process: Process, message: JsonObject) {
		val body = json.encodeToString(JsonObject.serializer(), message).encodeToByteArray()
		writeLock.withLock {
			withContext(Dispatchers.IO) { ContentLengthFraming.writeMessage(body, process.outputStream) }
		}
	}

	// Framing errors desynchronize the stream and propagate; a well-framed body that is not valid JSON is skipped.
	private fun readMessages(child: Process, input: BufferedInputStream) {
		while (true) {
			val bytes = ContentLengthFraming.readMessage(input, maxMessageBytes) ?: return
			val message = try {
				json.parseToJsonElement(bytes.decodeToString())
			} catch (error: SerializationException) {
				continue
			}
			when (message) {
				is JsonObject -> dispatch(child, message)
				is JsonArray -> message.filterIsInstance<JsonObject>().forEach { dispatch(child, it) }
				else -> Unit
			}
		}
	}

	private fun dispatch(child: Process, message: JsonObject) {
		val method = (message["method"] as? JsonPrimitive)?.contentOrNull
		val idElement = message["id"]
		if (method != null) {
			if (idElement == null || idElement is JsonNull) {
				mutableNotifications.tryEmit(RpcNotification(method, message["params"] ?: JsonNull))
			} else {
				rejectServerRequest(child, idElement, method)
			}
			return
		}
		val id = (idElement as? JsonPrimitive)?.contentOrNull?.toLongOrNull() ?: return
		val deferred = pending[id] ?: return
		val error = message["error"] as? JsonObject
		if (error != null) {
			val code = (error["code"] as? JsonPrimitive)?.contentOrNull?.toIntOrNull()
			val text = (error["message"] as? JsonPrimitive)?.contentOrNull ?: "RPC error"
			deferred.completeExceptionally(ProcessRpcException(code, text, data = error["data"]))
		} else {
			deferred.complete(message["result"] ?: JsonNull)
		}
	}

	// The client does not serve requests; answer them so the child is never left waiting.
	private fun rejectServerRequest(child: Process, id: JsonElement, method: String) {
		val response = buildJsonObject {
			put("jsonrpc", "2.0")
			put("id", id)
			put("error", buildJsonObject {
				put("code", -32601)
				put("message", "Client does not handle method '$method'")
			})
		}
		scope.launch(Dispatchers.IO) { runCatching { write(child, response) } }
	}

	private fun failPending(reason: String) {
		pending.keys.forEach { id -> pending.remove(id)?.completeExceptionally(ProcessRpcException(null, reason)) }
	}

	private companion object {
		const val STANDARD_ERROR_BUFFER_LINES = 256
		const val STANDARD_ERROR_TAIL_LINES = 20
		const val EXIT_GRACE_MS = 1_000L
	}
}
