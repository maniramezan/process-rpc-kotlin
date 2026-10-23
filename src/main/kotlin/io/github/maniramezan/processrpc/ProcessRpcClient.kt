package io.github.maniramezan.processrpc

import java.io.BufferedInputStream
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.pow
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
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
import kotlinx.serialization.json.put
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.doubleOrNull

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
	 * [kind] classifies the failure without parsing [reason].
	 * Calls fail immediately until [ProcessRpcClient.start] is called again or the [RestartPolicy] restarts it.
	 */
	public data class Unavailable(val reason: String, val exitCode: Int? = null, val kind: RpcFailureKind = RpcFailureKind.TRANSPORT) : ProcessRpcState
}

/**
 * A failed call or notification. [rpcCode] and [data] carry the JSON-RPC `error.code` and `error.data`
 * when the child answered with an error response; both are `null` for transport failures, such as a
 * timeout or a child that stopped. [kind] distinguishes local failure categories without parsing messages.
 */
public class ProcessRpcException(
	public val rpcCode: Int?,
	message: String,
	cause: Throwable? = null,
	public val data: JsonElement? = null,
	public val kind: RpcFailureKind = if (rpcCode == null) RpcFailureKind.TRANSPORT else RpcFailureKind.REMOTE_ERROR,
) : Exception(message, cause)

/**
 * A JSON-RPC notification sent by the child. [sequenceNumber] increases across launches of this client,
 * allowing collectors to detect gaps; manually constructed notifications default to zero.
 */
public data class RpcNotification(val method: String, val params: JsonElement, val sequenceNumber: Long = 0)

/** A notification and the number of messages this collector missed immediately before it. */
public data class RpcNotificationEvent(val notification: RpcNotification, val droppedBefore: Long)

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
			require(stableAfterMs > 0) { "stableAfterMs must be positive" }
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
 *   be started again. Cancelling it terminates the child and fails outstanding calls.
 * @param callTimeoutMs bounds sending and receiving a call, and separately waiting for a starting child.
 * @param notificationBufferCapacity how far collectors of [notifications] may fall behind before the
 *   oldest notifications are dropped.
 * @param shutdownTimeoutMs how long [close] and [stop] give the child at each shutdown step before
 *   escalating to a forcible kill.
 * @param workingDirectory the child's working directory, or `null` to inherit this process's.
 * @param inheritEnvironment whether the child starts from this process's environment. When `false`, it
 *   sees only [environment].
 * @param restartPolicy what to do when the child exits or fails on its own.
 * @param maxMessageBytes the largest incoming or outgoing message body. A larger Content-Length is
 *   treated as a protocol error.
 * @param writeTimeoutMs bounds queueing and writing any message, independently of a call's timeout.
 * @param maxPendingCalls maximum concurrent calls per launch; excess calls fail immediately.
 * @param maxQueuedWrites maximum messages waiting for the writer; excess sends fail immediately.
 * @param maxQueuedWriteBytes maximum encoded body bytes retained by encoders, queued writes, and the active writer.
 * @param maxStandardErrorLineBytes maximum bytes retained from each stderr line; excess bytes are
 *   discarded until the next line, and the emitted prefix is marked `[truncated]`.
 * @param onCallAbandoned invoked when a [call] whose request was sent times out or its caller is
 *   cancelled, with the request ID and method, so the protocol can tell the child to stop working on
 *   it, for example by sending an LSP-style `$/cancelRequest` notification. It runs on [Dispatchers.IO] and its
 *   failures are ignored. Hooks run sequentially per launch, with a queue bounded by maxQueuedWrites;
 *   excess hooks are dropped, and hooks are cancelled when their launch stops.
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
	private val writeTimeoutMs: Long = 10_000,
	private val maxPendingCalls: Int = 1_024,
	private val maxQueuedWrites: Int = 256,
	private val maxStandardErrorLineBytes: Int = 8_192,
	private val onCallAbandoned: (suspend ProcessRpcClient.(requestId: Long, method: String) -> Unit)? = null,
	private val maxQueuedWriteBytes: Long = 64L * 1024 * 1024,
) : AutoCloseable {
	init {
		require(callTimeoutMs > 0) { "callTimeoutMs must be positive" }
		require(shutdownTimeoutMs > 0 && shutdownTimeoutMs <= Long.MAX_VALUE / 2) { "shutdownTimeoutMs must be positive and allow two shutdown steps" }
		require(writeTimeoutMs > 0) { "writeTimeoutMs must be positive" }
		require(maxMessageBytes > 0) { "maxMessageBytes must be positive" }
		require(maxPendingCalls > 0) { "maxPendingCalls must be positive" }
		require(maxQueuedWriteBytes > 0) { "maxQueuedWriteBytes must be positive" }
		require(maxQueuedWrites > 0) { "maxQueuedWrites must be positive" }
		require(maxStandardErrorLineBytes > 0) { "maxStandardErrorLineBytes must be positive" }
		require(notificationBufferCapacity > 0) { "notificationBufferCapacity must be positive" }
	}

	private val json = Json { ignoreUnknownKeys = true }
	private val ids = AtomicLong(1)
	private val notificationSequence = AtomicLong()
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

	/**
	 * Notifications with loss reported separately for each collector. A slow collector learns how many
	 * messages it missed when it resumes. Messages from before collection began are excluded.
	 */
	public val notificationEvents: Flow<RpcNotificationEvent> = flow {
		var previous = notificationSequence.get()
		notifications.collect { notification ->
			val dropped = (notification.sequenceNumber - previous - 1).coerceAtLeast(0)
			previous = notification.sequenceNumber
			emit(RpcNotificationEvent(notification, dropped))
		}
	}
	private val mutableStandardError = MutableSharedFlow<String>(
		extraBufferCapacity = STANDARD_ERROR_BUFFER_LINES,
		onBufferOverflow = BufferOverflow.DROP_OLDEST,
	)

	/**
	 * Lines the child writes to standard error, across restarts. Hot and lossy like [notifications]: lines
	 * written while no one collects are dropped, and slow collectors lose the oldest lines.
	 */
	public val standardError: SharedFlow<String> = mutableStandardError.asSharedFlow()

	private var session: RpcSession? = null

	/**
	 * Launches the child in the background without blocking; observe [state] or just [call], which waits
	 * for the launch. Does nothing while the child is starting or ready.
	 */
	public fun start() {
		begin(restartOf = null)
	}

	/**
	 * Calls [method] and waits for its response. [timeoutMs] overrides the client's `callTimeoutMs` for
	 * this call, for operations known to take minutes, such as builds; it bounds queued writes and the
	 * response wait after the process becomes ready.
	 *
	 * Params must be a JSON object or array. Other values fail locally with [RpcFailureKind.INVALID_PARAMS].
	 *
	 * @throws ProcessRpcException when the child answers with an error, the call times out, or the child
	 *   is not running or stops before answering. Cancelling the caller cancels the call as usual.
	 */
	public suspend fun call(method: String, params: JsonElement = JsonObject(emptyMap()), timeoutMs: Long? = null): JsonElement {
		validateParams(params)
		val timeout = timeoutMs ?: callTimeoutMs
		require(timeout > 0) { "timeoutMs must be positive" }
		val current = awaitSession()
		val id = ids.getAndIncrement()
		val deferred = CompletableDeferred<JsonElement>()
		synchronized(lifecycleLock) {
			checkSession(current)
			if (current.pending.size >= maxPendingCalls) throw ProcessRpcException(null, "RPC pending call limit of $maxPendingCalls reached", kind = RpcFailureKind.OVERLOADED)
			current.pending[id] = deferred
		}
		val sent = AtomicBoolean()
		try {
			val request = buildJsonObject {
				put("jsonrpc", "2.0")
				put("id", id)
				put("method", method)
				put("params", params)
			}
			return withTimeoutOrNull(timeout) {
				write(current, request, sent)
				deferred.await()
			} ?: throw ProcessRpcException(null, "RPC call '$method' timed out after $timeout ms", kind = RpcFailureKind.TIMEOUT)
		} catch (error: CancellationException) {
			throw error
		} catch (error: ProcessRpcException) {
			throw error
		} catch (error: Exception) {
			throw ProcessRpcException(null, "RPC call '$method' failed: ${error.message}", error)
		} finally {
			if (current.pending.remove(id) != null && sent.get() && !deferred.isCompleted && onCallAbandoned != null) {
				current.abandoned.trySend(id to method)
			}
		}
	}

	/**
	 * Sends a JSON-RPC notification, which has no ID and receives no response. Params must be an object or array.
	 *
	 * @throws ProcessRpcException when the child is not running or the message cannot be written.
	 */
	public suspend fun notify(method: String, params: JsonElement = JsonObject(emptyMap())) {
		validateParams(params)
		val current = awaitSession()
		val notification = buildJsonObject {
			put("jsonrpc", "2.0")
			put("method", method)
			put("params", params)
		}
		try {
			write(current, notification)
		} catch (error: CancellationException) {
			throw error
		} catch (error: ProcessRpcException) {
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
		val current = detach() ?: return
		ProcessShutdown.terminate(current.handles, shutdownTimeoutMs)
	}

	/**
	 * Stops the child gracefully and waits for it to exit, even when called from a cancelled coroutine.
	 * Closes standard input, waits `shutdownTimeoutMs`, signals termination, then waits another
	 * `shutdownTimeoutMs` before forcibly killing the child and its captured descendants.
	 */
	public suspend fun stop() {
		val current = detach() ?: return
		val child = current.session.child
		val handles = current.handles
		val deadline = ProcessShutdown.arm(handles, shutdownTimeoutMs * 2)
		withContext(NonCancellable + Dispatchers.IO) {
			try {
				val closed = CompletableDeferred<Unit>()
				cleanupScope.launch { runCatching { child.outputStream.close() }; closed.complete(Unit) }
				withTimeoutOrNull(shutdownTimeoutMs) {
					closed.await()
					child.toHandle().onExit().await()
				}
				ProcessShutdown.signal(handles)
				withTimeoutOrNull(shutdownTimeoutMs) { ProcessShutdown.exits(handles).await() }
			} finally {
				ProcessShutdown.signal(handles, forcibly = true)
				withTimeoutOrNull(EXIT_GRACE_MS) { ProcessShutdown.exits(handles).await() }
				deadline.close()
			}
		}
	}

	private fun begin(restartOf: Long?) {
		val launched = synchronized(lifecycleLock) {
			if (restartOf == null) {
				val current = mutableState.value
				if (current is ProcessRpcState.Starting || current is ProcessRpcState.Ready) return
				restarts = 0
			} else if (generation != restartOf) return
			val nextGeneration = ++generation
			mutableState.value = ProcessRpcState.Starting
			nextGeneration
		}
		scope.launch(Dispatchers.IO) { runProcess(launched) }.invokeOnCompletion { cause ->
			if (cause != null) settle(launched, "RPC process launch was cancelled", exitCode = null)
		}
	}

	private class DetachedSession(val session: RpcSession, val handles: List<ProcessHandle>)

	private fun detach(): DetachedSession? {
		val current = synchronized(lifecycleLock) {
			generation++
			val previous = session?.let { DetachedSession(it, ProcessShutdown.capture(it.child)) }
			session = null
			mutableState.value = ProcessRpcState.Stopped
			previous
		}
		current?.session?.finish("RPC process stopped", RpcFailureKind.NOT_RUNNING)
		return current
	}

	private suspend fun awaitSession(): RpcSession {
		val settled = withTimeoutOrNull(callTimeoutMs) { state.first { it !is ProcessRpcState.Starting } }
			?: throw ProcessRpcException(null, "RPC process did not start within $callTimeoutMs ms", kind = RpcFailureKind.TIMEOUT)
		return synchronized(lifecycleLock) {
			when (settled) {
				is ProcessRpcState.Ready -> session?.takeIf { it.child.pid() == settled.pid }
					?: throw ProcessRpcException(null, "RPC process stopped", kind = RpcFailureKind.NOT_RUNNING)
				is ProcessRpcState.Unavailable -> throw ProcessRpcException(null, settled.reason, kind = settled.kind)
				else -> throw ProcessRpcException(null, "RPC process is not running; call start() first", kind = RpcFailureKind.NOT_RUNNING)
			}
		}
	}

	private fun checkSession(current: RpcSession) {
		if (session !== current) throw ProcessRpcException(null, "RPC process stopped", kind = RpcFailureKind.NOT_RUNNING)
	}

	private suspend fun runProcess(launched: Long) {
		if (synchronized(lifecycleLock) { generation != launched }) return
		val child = try {
			val invocation = command()
			currentCoroutineContext().ensureActive()
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
		val current = RpcSession(launched, child, maxQueuedWrites, maxQueuedWriteBytes, scope.coroutineContext)
		val launchContext = currentCoroutineContext()
		val attached = synchronized(lifecycleLock) {
			if (generation == launched && launchContext.isActive) {
				session = current
				mutableState.value = ProcessRpcState.Ready(child.pid())
				true
			} else false
		}
		if (!attached) {
			current.finish("RPC process stopped")
			ProcessShutdown.terminate(child, shutdownTimeoutMs)
			return
		}
		current.cancellationWatcher = scope.launch(start = CoroutineStart.UNDISPATCHED) {
			try {
				awaitCancellation()
			} finally {
				failSession(current, "RPC client scope was cancelled", forcibly = true, restart = false)
			}
		}
		startWriter(current)
		val tail = ArrayDeque<String>()
		val drain = current.scope.launch {
			runCatching {
				readStandardError(child.errorStream, maxStandardErrorLineBytes) { line ->
					synchronized(tail) {
						if (tail.size == STANDARD_ERROR_TAIL_LINES) tail.removeFirst()
						tail.addLast(line)
					}
					mutableStandardError.tryEmit(line)
				}
			}.onFailure { failSession(current, "RPC standard error read failed: ${it.message}") }
		}
		try {
			val protocolError = try {
				readMessages(current, BufferedInputStream(child.inputStream))
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
			val kind = when (protocolError) {
				null -> RpcFailureKind.TRANSPORT
				is ProcessRpcException -> protocolError.kind
				else -> RpcFailureKind.PROTOCOL
			}
			if (settle(launched, reason, exitCode, kind)) {
				scheduleRestart(launched, uptimeMs = current.uptimeMs)
			}
		} finally {
			failSession(current, "RPC process reader was cancelled", forcibly = true, restart = false)
			current.cancellationWatcher?.cancel()
		}
	}

	private fun settle(launched: Long, reason: String, exitCode: Int?, kind: RpcFailureKind = RpcFailureKind.TRANSPORT): Boolean {
		val previous = synchronized(lifecycleLock) {
			if (generation != launched || mutableState.value is ProcessRpcState.Stopped || mutableState.value is ProcessRpcState.Unavailable) return false
			val current = session?.let { DetachedSession(it, ProcessShutdown.capture(it.child)) }
			session = null
			mutableState.value = ProcessRpcState.Unavailable(reason, exitCode, kind)
			current
		}
		if (previous != null) {
			ProcessShutdown.terminate(previous.handles, shutdownTimeoutMs)
			previous.session.finish(reason, kind)
		}
		return true
	}

	private fun failSession(current: RpcSession, reason: String, forcibly: Boolean = false, restart: Boolean = true, kind: RpcFailureKind = RpcFailureKind.TRANSPORT) {
		if (settle(current.generation, reason, exitCode = null, kind = kind)) {
			if (forcibly) ProcessShutdown.terminate(current.child, shutdownTimeoutMs, forcibly = true)
			if (restart) scheduleRestart(current.generation, uptimeMs = current.uptimeMs)
		}
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
			begin(restartOf = launched)
		}
	}

	private suspend fun enqueue(current: RpcSession, message: JsonElement, sent: AtomicBoolean = AtomicBoolean()): RpcWrite {
		val reservation = synchronized(lifecycleLock) {
			checkSession(current)
			current.writeBudget.reserve()
		}
		try {
			val body = withTimeoutOrNull(writeTimeoutMs) {
				withContext(Dispatchers.Default) {
					encodeRpcMessage(json, message, maxMessageBytes, reservation, currentCoroutineContext())
				}
			} ?: throw ProcessRpcException(null, "RPC encoding timed out after $writeTimeoutMs ms", kind = RpcFailureKind.TIMEOUT)
			val outgoing = RpcWrite(body, sent, reservation)
			synchronized(lifecycleLock) {
				checkSession(current)
				if (!current.writes.trySend(outgoing).isSuccess) throw ProcessRpcException(null, "RPC write queue limit of $maxQueuedWrites reached", kind = RpcFailureKind.OVERLOADED)
			}
			return outgoing
		} catch (error: Throwable) {
			reservation.close()
			throw error
		}
	}

	private suspend fun write(current: RpcSession, message: JsonObject, sent: AtomicBoolean = AtomicBoolean()) {
		var outgoing: RpcWrite? = null
		try {
			withTimeoutOrNull(writeTimeoutMs) {
				val queued = enqueue(current, message, sent)
				outgoing = queued
				select<Unit> {
					queued.completed.onAwait { }
					current.closed.onAwait { }
				}
			} ?: throw ProcessRpcException(null, "RPC write timed out after $writeTimeoutMs ms", kind = RpcFailureKind.TIMEOUT)
		} finally {
			outgoing?.let { queued ->
				val partial = synchronized(queued) {
					queued.abandoned = true
					if (!queued.started) {
						queued.body = ByteArray(0)
						queued.reservation.close()
					}
					queued.started && !queued.completed.isCompleted
				}
				if (partial) failSession(current, "RPC write was interrupted; the stream may contain a partial frame", forcibly = true)
			}
		}
	}

	private fun startWriter(current: RpcSession) {
		current.scope.launch {
			try {
				for (outgoing in current.writes) {
					outgoing.reservation.releaseSlot()
					try {
						val abandoned = synchronized(outgoing) {
							if (!outgoing.abandoned) outgoing.started = true
							outgoing.abandoned
						}
						if (abandoned) continue
						val remainingMs = writeTimeoutMs - (System.nanoTime() - outgoing.reservation.queuedAtNanos) / 1_000_000
						if (remainingMs <= 0) {
							failSession(current, "RPC write timed out in the queue after $writeTimeoutMs ms", kind = RpcFailureKind.TIMEOUT)
							break
						}
						val deadline = ProcessShutdown.schedule(remainingMs) {
							if (!outgoing.completed.isCompleted) {
								ProcessShutdown.terminate(current.child, shutdownTimeoutMs, forcibly = true)
								cleanupScope.launch { failSession(current, "RPC write timed out after $writeTimeoutMs ms", forcibly = true, kind = RpcFailureKind.TIMEOUT) }
							}
						}
						try {
							ContentLengthFraming.writeMessage(outgoing.body, current.child.outputStream)
							outgoing.sent.set(true)
							outgoing.body = ByteArray(0)
							outgoing.reservation.close()
							outgoing.completed.complete(Unit)
						} finally {
							deadline.close()
						}
					} finally {
						outgoing.reservation.close()
					}
				}
			} catch (error: Exception) {
				failSession(current, "RPC write failed: ${error.message}")
			}
		}
		if (onCallAbandoned != null) current.scope.launch {
			for ((id, method) in current.abandoned) {
				runCatching { onCallAbandoned.invoke(this@ProcessRpcClient, id, method) }
			}
		}
	}

	private suspend fun readMessages(current: RpcSession, input: BufferedInputStream) {
		while (true) {
			val bytes = ContentLengthFraming.readMessage(input, maxMessageBytes) ?: return
			val message = try {
				json.parseToJsonElement(bytes.decodeToString())
			} catch (error: SerializationException) {
				continue
			}
			when (message) {
				is JsonObject -> dispatch(current, message)?.let { enqueue(current, it) }
				is JsonArray -> {
					if (message.isEmpty()) protocolError("empty batch")
					val rejections = message.mapNotNull { dispatch(current, it as? JsonObject ?: protocolError("batch element is not an object")) }
					if (rejections.isNotEmpty()) enqueue(current, JsonArray(rejections))
				}
				else -> protocolError("message is not an object or batch")
			}
		}
	}

	private fun dispatch(current: RpcSession, message: JsonObject): JsonObject? {
		if (synchronized(lifecycleLock) { session !== current }) return null
		if (message["jsonrpc"] != JsonPrimitive("2.0")) protocolError("jsonrpc must be the string 2.0")
		val idElement = message["id"]
		if (idElement != null && idElement !is JsonNull && (idElement !is JsonPrimitive || (!idElement.isString && idElement.doubleOrNull?.isFinite() != true))) {
			protocolError("id must be a string, number, or null")
		}
		if ("method" in message) {
			val method = (message["method"] as? JsonPrimitive)?.takeIf { it.isString }?.content ?: protocolError("method must be a string")
			if ("result" in message || "error" in message) protocolError("request contains response fields")
			val params = message["params"]
			if (params != null && params !is JsonObject && params !is JsonArray) protocolError("params must be an object or array")
			if (idElement != null) return rejectServerRequest(idElement, method)
			mutableNotifications.tryEmit(RpcNotification(method, params ?: JsonNull, notificationSequence.incrementAndGet()))
			return null
		}
		if (idElement == null) protocolError("response has no id")
		if (("result" in message) == ("error" in message)) protocolError("response must have exactly one of result or error")
		val error = if ("error" in message) message["error"] as? JsonObject ?: protocolError("error must be an object") else null
		val code = error?.let { (it["code"] as? JsonPrimitive)?.takeUnless { value -> value.isString }?.intOrNull ?: protocolError("error.code must be an integer") }
		val text = error?.let { (it["message"] as? JsonPrimitive)?.takeIf { value -> value.isString }?.content ?: protocolError("error.message must be a string") }
		val id = idElement.takeUnless { it.isString }?.longOrNull ?: return null
		val deferred = current.pending[id] ?: return null
		if (error != null) deferred.completeExceptionally(ProcessRpcException(code, text!!, data = error["data"]))
		else deferred.complete(message.getValue("result"))
		return null
	}

	private fun rejectServerRequest(id: JsonElement, method: String): JsonObject = buildJsonObject {
		put("jsonrpc", "2.0")
		put("id", id)
		put("error", buildJsonObject {
			put("code", -32601)
			put("message", "Client does not handle method '$method'")
		})
	}

	private fun validateParams(params: JsonElement) {
		if (params !is JsonObject && params !is JsonArray) {
			throw ProcessRpcException(null, "RPC params must be an object or array", kind = RpcFailureKind.INVALID_PARAMS)
		}
	}

	private fun protocolError(detail: String): Nothing = throw java.io.IOException("Invalid JSON-RPC envelope: $detail")

	private companion object {
		val cleanupScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
		const val STANDARD_ERROR_BUFFER_LINES = 256
		const val STANDARD_ERROR_TAIL_LINES = 20
		const val EXIT_GRACE_MS = 1_000L
	}
}
