package io.github.maniramezan.processrpc

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

internal object ProcessShutdown {
	private val deadlines = ScheduledThreadPoolExecutor(1) { task ->
		Thread(task, "process-rpc-shutdown").apply { isDaemon = true }
	}.apply { removeOnCancelPolicy = true }

	fun capture(child: Process): List<ProcessHandle> = child.descendants().use { it.toList() } + child.toHandle()

	fun exits(handles: List<ProcessHandle>): CompletableFuture<Void> =
		CompletableFuture.allOf(*handles.map { it.onExit() }.toTypedArray())

	fun arm(handles: List<ProcessHandle>, timeoutMs: Long): AutoCloseable {
		return schedule(timeoutMs) { signal(handles, forcibly = true) }
	}

	fun schedule(timeoutMs: Long, action: () -> Unit): AutoCloseable {
		val deadline = deadlines.schedule(action, timeoutMs, TimeUnit.MILLISECONDS)
		return AutoCloseable { deadline.cancel(false) }
	}

	fun signal(handles: List<ProcessHandle>, forcibly: Boolean = false) {
		handles.forEach { handle ->
			runCatching {
				if (handle.isAlive) {
					if (forcibly) handle.destroyForcibly() else handle.destroy()
				}
			}
		}
	}

	fun terminate(child: Process, timeoutMs: Long, forcibly: Boolean = false) {
		terminate(capture(child), timeoutMs, forcibly)
	}

	fun terminate(handles: List<ProcessHandle>, timeoutMs: Long, forcibly: Boolean = false) {
		if (forcibly) {
			signal(handles, forcibly = true)
		} else {
			val deadline = arm(handles, timeoutMs)
			signal(handles)
			exits(handles).whenComplete { _, _ -> deadline.close() }
		}
	}
}
