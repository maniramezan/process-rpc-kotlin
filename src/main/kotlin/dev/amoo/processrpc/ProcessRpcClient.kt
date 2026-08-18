package io.github.maniramezan.processrpc

import java.io.BufferedInputStream
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
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
import kotlinx.serialization.json.long
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

public class ProcessRpcClient(
	private val command: () -> List<String>,
	private val environment: Map<String, String> = emptyMap(),
	private val scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
	private val callTimeoutMs: Long = 30_000,
) : AutoCloseable {
	private val json = Json { ignoreUnknownKeys = true }
	private val ids = AtomicLong(1)
	private val pending = ConcurrentHashMap<Long, CompletableDeferred<JsonElement>>()
	private val writeLock = Mutex()
	private val mutableState = MutableStateFlow<ProcessRpcState>(ProcessRpcState.Stopped)
	public val state: StateFlow<ProcessRpcState> = mutableState.asStateFlow()

	@Volatile
	private var process: Process? = null

	public fun start() {
		if (process?.isAlive == true || mutableState.value == ProcessRpcState.Starting) return
		mutableState.value = ProcessRpcState.Starting
		scope.launch { runProcess() }
	}

	public suspend fun call(method: String, params: JsonElement = JsonObject(emptyMap())): JsonElement {
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
			writeLock.withLock {
				ContentLengthFraming.writeMessage(json.encodeToString(JsonObject.serializer(), request).encodeToByteArray(), current.outputStream)
			}
			return withTimeout(callTimeoutMs) { deferred.await() }
		} catch (error: ProcessRpcException) {
			throw error
		} catch (error: Exception) {
			throw ProcessRpcException(null, "RPC call '$method' failed: ${error.message}", error)
		} finally {
			pending.remove(id)
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
			readResponses(BufferedInputStream(child.inputStream))
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

	private fun readResponses(input: BufferedInputStream) {
		while (true) {
			val bytes = ContentLengthFraming.readMessage(input) ?: return
			val response = json.parseToJsonElement(bytes.decodeToString()).jsonObject
			val id = (response["id"] as? JsonPrimitive)?.long ?: continue
			val deferred = pending[id] ?: continue
			val error = response["error"]?.jsonObject
			if (error != null) {
				val code = (error["code"] as? JsonPrimitive)?.contentOrNull?.toIntOrNull()
				val message = (error["message"] as? JsonPrimitive)?.contentOrNull ?: "RPC error"
				deferred.completeExceptionally(ProcessRpcException(code, message))
			} else {
				deferred.complete(response["result"] ?: JsonNull)
			}
		}
	}

	private fun failPending(reason: String) {
		pending.values.forEach { it.completeExceptionally(ProcessRpcException(null, reason)) }
		pending.clear()
	}
}
