package io.github.maniramezan.processrpc

import java.io.BufferedInputStream
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
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
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
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
) : AutoCloseable {
	private val json = Json { ignoreUnknownKeys = true }
	private val ids = AtomicLong(1)
	private val pending = ConcurrentHashMap<Long, CompletableDeferred<JsonElement>>()
	private val writeLock = Mutex()
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
		if (process?.isAlive == true || mutableState.value == ProcessRpcState.Starting) return
		mutableState.value = ProcessRpcState.Starting
		scope.launch { runProcess() }
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
			write(current, request)
			return withTimeout(timeoutMs ?: callTimeoutMs) { deferred.await() }
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
		} catch (error: Exception) {
			throw ProcessRpcException(null, "RPC notification '$method' failed: ${error.message}", error)
		}
	}

	override fun close() {
		process?.destroy()
		process = null
		mutableState.value = ProcessRpcState.Stopped
		failPending("RPC process stopped")
	}

	private suspend fun awaitProcess(): Process {
		val settled = withTimeout(callTimeoutMs) {
			state.first { it is ProcessRpcState.Ready || it is ProcessRpcState.Unavailable }
		}
		if (settled is ProcessRpcState.Unavailable) throw ProcessRpcException(null, settled.reason)
		return process ?: throw ProcessRpcException(null, "RPC process is not running")
	}

	private fun runProcess() {
		try {
			val invocation = command()
			require(invocation.isNotEmpty()) { "Process command must not be empty" }
			val child = ProcessBuilder(invocation).redirectErrorStream(false).apply {
				environment().putAll(this@ProcessRpcClient.environment)
			}.start()
			process = child
			mutableState.value = ProcessRpcState.Ready(child.pid())
			scope.launch { child.errorStream.bufferedReader().useLines { lines -> lines.forEach { } } }
			readResponses(child, BufferedInputStream(child.inputStream))
			if (process === child) {
				process = null
				mutableState.value = ProcessRpcState.Unavailable("RPC process exited unexpectedly")
				failPending("RPC process exited unexpectedly")
			}
		} catch (error: Exception) {
			process = null
			val reason = "Failed to start RPC process: ${error.message}"
			mutableState.value = ProcessRpcState.Unavailable(reason)
			failPending(reason)
		}
	}

	private suspend fun write(process: Process, message: JsonObject) {
		writeLock.withLock {
			ContentLengthFraming.writeMessage(json.encodeToString(JsonObject.serializer(), message).encodeToByteArray(), process.outputStream)
		}
	}

	private fun readResponses(child: Process, input: BufferedInputStream) {
		while (true) {
			val bytes = ContentLengthFraming.readMessage(input) ?: return
			val message = json.parseToJsonElement(bytes.decodeToString()).jsonObject
			val method = (message["method"] as? JsonPrimitive)?.contentOrNull
			val idElement = message["id"]
			if (method != null) {
				if (idElement == null || idElement is JsonNull) {
					mutableNotifications.tryEmit(RpcNotification(method, message["params"] ?: JsonNull))
				} else {
					rejectServerRequest(child, idElement, method)
				}
				continue
			}
			val id = (idElement as? JsonPrimitive)?.contentOrNull?.toLongOrNull() ?: continue
			val deferred = pending[id] ?: continue
			val error = message["error"]?.jsonObject
			if (error != null) {
				val code = (error["code"] as? JsonPrimitive)?.contentOrNull?.toIntOrNull()
				val message = (error["message"] as? JsonPrimitive)?.contentOrNull ?: "RPC error"
				deferred.completeExceptionally(ProcessRpcException(code, message))
			} else {
				deferred.complete(message["result"] ?: JsonNull)
			}
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
		scope.launch { runCatching { write(child, response) } }
	}

	private fun failPending(reason: String) {
		pending.values.forEach { it.completeExceptionally(ProcessRpcException(null, reason)) }
		pending.clear()
	}
}
