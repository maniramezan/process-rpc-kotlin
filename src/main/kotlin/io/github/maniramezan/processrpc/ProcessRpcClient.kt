package io.github.maniramezan.processrpc

import java.io.BufferedInputStream
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.BufferOverflow
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

public sealed interface ProcessRpcState {
	public data object Stopped : ProcessRpcState
	public data object Starting : ProcessRpcState
	public data class Ready(val pid: Long) : ProcessRpcState
	public data class Unavailable(val reason: String) : ProcessRpcState
}

public class ProcessRpcException(
	public val rpcCode: Int?,
	message: String,
	cause: Throwable? = null,
) : Exception(message, cause)

/** A JSON-RPC notification (a message with a `method` and no `id`) sent by the child process. */
public data class RpcNotification(val method: String, val params: JsonElement)

/**
 * Supervises a child process that speaks JSON-RPC 2.0 over Content-Length-framed stdio.
 *
 * Requests are correlated to responses by ID. Notifications from the child are published on
 * [notifications]; when collectors fall more than [notificationBufferCapacity] messages behind, the
 * oldest notifications are dropped, so protocols that cannot tolerate loss should carry sequence
 * numbers and offer a replay method.
 */
public class ProcessRpcClient(
	private val command: () -> List<String>,
	private val environment: Map<String, String> = emptyMap(),
	private val scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
	private val callTimeoutMs: Long = 30_000,
	notificationBufferCapacity: Int = 1_024,
	private val shutdownTimeoutMs: Long = 2_000,
) : AutoCloseable {
	private val json = Json { ignoreUnknownKeys = true }
	private val ids = AtomicLong(1)
	private val pending = ConcurrentHashMap<Long, CompletableDeferred<JsonElement>>()
	private val writeLock = Mutex()
	private val lifecycleLock = Any()
	private var generation = 0L
	private val mutableState = MutableStateFlow<ProcessRpcState>(ProcessRpcState.Stopped)
	public val state: StateFlow<ProcessRpcState> = mutableState.asStateFlow()
	private val mutableNotifications = MutableSharedFlow<RpcNotification>(
		extraBufferCapacity = notificationBufferCapacity,
		onBufferOverflow = BufferOverflow.DROP_OLDEST,
	)

	/** Notifications sent by the child process, in arrival order. Hot: subscribe before calling [start] to see every message. */
	public val notifications: SharedFlow<RpcNotification> = mutableNotifications.asSharedFlow()

	@Volatile
	private var process: Process? = null

	public fun start() {
		val launched = synchronized(lifecycleLock) {
			val current = mutableState.value
			if (current is ProcessRpcState.Starting || current is ProcessRpcState.Ready) return
			mutableState.value = ProcessRpcState.Starting
			++generation
		}
		scope.launch(Dispatchers.IO) { runProcess(launched) }.invokeOnCompletion { cause ->
			if (cause != null) settle(launched, "RPC process launch was cancelled")
		}
	}

	/**
	 * Calls [method] and waits for its response. [timeoutMs] overrides the client's `callTimeoutMs` for
	 * this call, for operations known to take minutes, such as builds; it bounds the wait for the
	 * response, not for the process to become ready.
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
		try {
			if (process !== current) throw ProcessRpcException(null, "RPC process stopped")
			write(current, request)
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
			pending.remove(id)
		}
	}

	/** Sends a JSON-RPC notification, which has no ID and receives no response. */
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
	 * terminate, and forcibly kills any that are still alive after `shutdownTimeoutMs`.
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

	private fun runProcess(launched: Long) {
		val child = try {
			val invocation = command()
			require(invocation.isNotEmpty()) { "Process command must not be empty" }
			ProcessBuilder(invocation).redirectErrorStream(false).apply {
				environment().putAll(this@ProcessRpcClient.environment)
			}.start()
		} catch (error: Exception) {
			settle(launched, "Failed to start RPC process: ${error.message}")
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
		scope.launch(Dispatchers.IO) { child.errorStream.bufferedReader().useLines { lines -> lines.forEach { } } }
		val reason = try {
			readMessages(child, BufferedInputStream(child.inputStream))
			"RPC process exited unexpectedly"
		} catch (error: Exception) {
			"RPC protocol error: ${error.message}"
		}
		if (settle(launched, reason)) terminate(child)
	}

	// Marks the client unavailable unless a newer start() or a close() has already replaced this run.
	private fun settle(launched: Long, reason: String): Boolean {
		synchronized(lifecycleLock) {
			if (generation != launched) return false
			process = null
			mutableState.value = ProcessRpcState.Unavailable(reason)
		}
		failPending(reason)
		return true
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
			val bytes = ContentLengthFraming.readMessage(input) ?: return
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
			deferred.completeExceptionally(ProcessRpcException(code, text))
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
}
