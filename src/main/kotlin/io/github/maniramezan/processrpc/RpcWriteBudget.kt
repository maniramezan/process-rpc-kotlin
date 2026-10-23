package io.github.maniramezan.processrpc

import java.io.ByteArrayOutputStream
import java.io.OutputStream
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.encodeToStream

internal class RpcWriteBudget(private val maxQueuedWrites: Int, private val maxBytes: Long) {
	private var queued = 0
	private var bytes = 0L
	val retainedBytes: Long get() = synchronized(this) { bytes }
	val queuedReservations: Int get() = synchronized(this) { queued }

	@Synchronized
	fun reserve(): RpcWriteReservation {
		if (bytes >= maxBytes) throw ProcessRpcException(null, "RPC write byte limit of $maxBytes reached", kind = RpcFailureKind.OVERLOADED)
		if (queued >= maxQueuedWrites) throw ProcessRpcException(null, "RPC write queue limit of $maxQueuedWrites reached", kind = RpcFailureKind.OVERLOADED)
		queued++
		return RpcWriteReservation(this)
	}

	@Synchronized
	fun grow(reservation: RpcWriteReservation, count: Int) {
		if (count > maxBytes - bytes) throw ProcessRpcException(null, "RPC write byte limit of $maxBytes reached", kind = RpcFailureKind.OVERLOADED)
		bytes += count
		reservation.bytes += count
	}

	@Synchronized
	fun releaseSlot(reservation: RpcWriteReservation) {
		if (reservation.queued) {
			reservation.queued = false
			queued--
		}
	}

	@Synchronized
	fun release(reservation: RpcWriteReservation) {
		releaseSlot(reservation)
		bytes -= reservation.bytes
		reservation.bytes = 0
	}
}

internal class RpcWriteReservation(private val budget: RpcWriteBudget) : AutoCloseable {
	val queuedAtNanos = System.nanoTime()
	var queued = true
	var bytes = 0L

	fun grow(count: Int) = budget.grow(this, count)
	fun releaseSlot() = budget.releaseSlot(this)
	override fun close() = budget.release(this)
}

@OptIn(ExperimentalSerializationApi::class)
internal fun encodeRpcMessage(
	json: Json,
	message: JsonElement,
	maxMessageBytes: Int,
	reservation: RpcWriteReservation,
	context: CoroutineContext,
): ByteArray {
	val body = ByteArrayOutputStream(minOf(8_192, maxMessageBytes))
	val bounded = object : OutputStream() {
		override fun write(value: Int) {
			reserve(1)
			body.write(value)
		}

		override fun write(buffer: ByteArray, offset: Int, length: Int) {
			reserve(length)
			body.write(buffer, offset, length)
		}

		private fun reserve(count: Int) {
			context.ensureActive()
			if (count > maxMessageBytes - body.size()) {
				throw ProcessRpcException(null, "RPC message exceeds the $maxMessageBytes byte limit", kind = RpcFailureKind.MESSAGE_TOO_LARGE)
			}
			reservation.grow(count)
		}
	}
	json.encodeToStream(JsonElement.serializer(), message, bounded)
	return body.toByteArray()
}
