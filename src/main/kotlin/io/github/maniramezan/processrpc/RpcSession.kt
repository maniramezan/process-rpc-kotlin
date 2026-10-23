package io.github.maniramezan.processrpc

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.serialization.json.JsonElement

internal class RpcSession(val generation: Long, val child: Process, queueCapacity: Int, parentContext: CoroutineContext) {
	private val startedAtNanos = System.nanoTime()
	val uptimeMs: Long get() = (System.nanoTime() - startedAtNanos) / 1_000_000
	val scope = CoroutineScope(parentContext + SupervisorJob() + Dispatchers.IO)
	val closed = CompletableDeferred<Unit>()
	val pending = ConcurrentHashMap<Long, CompletableDeferred<JsonElement>>()
	val writes = Channel<RpcWrite>(queueCapacity)
	val abandoned = Channel<Pair<Long, String>>(queueCapacity)
	var cancellationWatcher: Job? = null

	fun finish(reason: String) {
		closed.completeExceptionally(ProcessRpcException(null, reason))
		writes.cancel()
		abandoned.cancel()
		scope.cancel()
		cancellationWatcher?.cancel()
		pending.keys.forEach { pending.remove(it)?.completeExceptionally(ProcessRpcException(null, reason)) }
	}
}

internal class RpcWrite(val body: ByteArray, val sent: AtomicBoolean = AtomicBoolean()) {
	val queuedAtNanos = System.nanoTime()
	val completed = CompletableDeferred<Unit>()
	var started = false
	var abandoned = false
}
