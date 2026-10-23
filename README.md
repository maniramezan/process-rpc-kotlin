# Process RPC for Kotlin

[![CI](https://github.com/maniramezan/process-rpc-kotlin/actions/workflows/ci.yml/badge.svg)](https://github.com/maniramezan/process-rpc-kotlin/actions/workflows/ci.yml)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://maniramezan.github.io/process-rpc-kotlin/)
[![JitPack](https://jitpack.io/v/maniramezan/process-rpc-kotlin.svg)](https://jitpack.io/#maniramezan/process-rpc-kotlin)

Product-neutral Kotlin/JVM infrastructure for launching a bundled native executable and calling
it through JSON-RPC 2.0 over Content-Length-framed stdio.

This module was extracted from the MIT-licensed moqserver Studio process boundary. It deliberately
contains no Amoo, moqserver, Compose, or application-domain types, so it can become a standalone
open-source library without either product depending on the other.

## Usage

Requires JVM 17 or newer.

```kotlin
repositories {
    maven("https://jitpack.io")
}

dependencies {
    implementation("com.github.maniramezan:process-rpc-kotlin:0.3.0")
}
```

Locate the executable and launch it. `command` runs on every launch, so it can re-resolve the binary:

```kotlin
val locator = BundledBinaryLocator("my-service", overrideEnvironmentVariable = "MY_SERVICE_PATH")
val client = ProcessRpcClient(
    command = { listOf(locator.locate(), "serve") },
)
client.start()
val handshake = client.call("system.handshake")
```

`start()` returns immediately; `call()` waits for the child to launch. Observe the lifecycle to drive
UI, for example to show why the service is unavailable:

```kotlin
scope.launch {
    client.state.collect { state ->
        when (state) {
            is ProcessRpcState.Ready -> println("running as ${state.pid}")
            is ProcessRpcState.Unavailable -> println("unavailable: ${state.reason}")
            ProcessRpcState.Starting, ProcessRpcState.Stopped -> Unit
        }
    }
}
```

### Calls and errors

A slow operation, such as a build, can wait longer than the client's `callTimeoutMs`:

```kotlin
client.call("project.build", params, timeoutMs = 10 * 60_000L)
```

Failures throw `ProcessRpcException`. When the child answered with a JSON-RPC error, `rpcCode` and
`data` carry its `error.code` and `error.data`; both are `null` for timeouts and stopped processes.
Cancelling the calling coroutine cancels the call as usual rather than throwing this exception.
`kind: RpcFailureKind` distinguishes timeouts, overload, stopped clients, invalid params, oversized
messages, protocol violations, transport failures, and typed encoding/decoding failures. Remote
JSON-RPC errors have `kind = RpcFailureKind.REMOTE_ERROR`.

Outgoing `params` must be a JSON object or array. Scalars and explicit null fail locally with
`RpcFailureKind.INVALID_PARAMS`; wrap scalar arguments in an array or DTO.

```kotlin
try {
    client.call("project.open", params)
} catch (error: ProcessRpcException) {
    if (error.rpcCode == -32602) showInvalidProject(error.data) else throw error
}
```

### Typed calls

The library exchanges `kotlinx.serialization.json.JsonElement` values and does not prescribe
application methods or DTOs. The typed helpers encode and decode your own `@Serializable` types:

```kotlin
@Serializable data class OpenProject(val path: String)
@Serializable data class Project(val id: String, val name: String)

val project = client.callAs<OpenProject, Project>("project.open", OpenProject("/work/app"))
client.notifyWith("project.focus", OpenProject("/work/app"))
```

### Notifications and standard error

Subscribe to notifications pushed by the child, such as progress or streamed events. Both flows are
hot: subscribe before `start()` to see every message.

```kotlin
scope.launch {
    client.notifications.collect { notification ->
        println("${notification.method}: ${notification.params}")
    }
}
scope.launch {
    client.standardError.collect { line -> logger.debug(line) }
}
```

When the child exits, `ProcessRpcState.Unavailable` carries its exit code, failure kind, and its last
lines of standard error.

Collect `notificationEvents` to detect loss separately for each subscriber:

```kotlin
scope.launch {
    client.notificationEvents.collect { event ->
        if (event.droppedBefore > 0) refreshFromServer()
        handleNotification(event.notification)
    }
}
```

Loss is reported on the next delivered notification after a collector falls behind. Messages emitted
before collection began are excluded. `RpcNotification.sequenceNumber` increases across process
restarts, so existing collectors retain continuity.

### Restarts, cancellation, and shutdown

```kotlin
val client = ProcessRpcClient(
    command = { listOf(locator.locate(), "serve") },
    restartPolicy = RestartPolicy.Backoff(maxRestarts = 5),
    onCallAbandoned = { requestId, _ ->
        notify("\$/cancelRequest", buildJsonObject { put("id", requestId) })
    },
)
```

`RestartPolicy.Backoff` relaunches a child that exits on its own. `onCallAbandoned` runs when a sent
call times out or its caller is cancelled, so the protocol can tell the child to stop the work.

`close()` signals the child and its descendants to terminate and kills them after `shutdownTimeoutMs`.
`stop()` suspends while it shuts down gracefully: it closes the child's standard input, waits for it to
exit, and escalates only when needed. Its cleanup also runs when called from a cancelled coroutine's
`finally` block. Both fail outstanding calls, and the client can be started again.

Cancelling the supplied `scope` terminates the child and fails outstanding calls. The client never
cancels that scope itself.

### Timeouts and resource limits

A call's timeout covers queueing, writing, and waiting for its response after the process becomes
ready. Waiting for a starting process has a separate `callTimeoutMs` budget. Notifications and calls
also have a `writeTimeoutMs` deadline, which defaults to 10 seconds and includes body encoding.
Cancelling a call, or letting its timeout expire, leaves the process running: a message that has
started writing is always finished, and one still queued is skipped. Only a write that cannot finish
within `writeTimeoutMs` stops the process, because the stream would then hold a partial frame.

The constructor exposes these limits:

| Option | Default | Behavior at the limit |
| --- | --- | --- |
| `maxPendingCalls` | 1,024 | Additional calls fail immediately. |
| `maxQueuedWrites` | 256 | Reserves capacity before body encoding; additional sends fail immediately. A flood of child requests stops the process. |
| `maxQueuedWriteBytes` | 64 MiB | Caps encoded body bytes held by encoders, queued messages, and the active writer; excess sends fail locally with `OVERLOADED`. |
| `maxMessageBytes` | 64 MiB | Oversized outgoing messages fail locally; oversized incoming frames stop the process. |
| `maxStandardErrorLineBytes` | 8 KiB | The line is truncated, marked `[truncated]`, and drained through its next newline. |

Body encoding runs on `Dispatchers.Default` and reserves bytes incrementally, stopping at the limit.
The byte budget measures encoded body lengths; caller-owned JSON trees, encoder buffers, and
transient buffer copies add memory overhead. Queued cancellations release their body and budget,
and completed writes release capacity before resuming their callers.

Each launch owns its pending calls and write queue. Closing or failing one launch cannot fail calls
on a later launch. Abandonment hooks run sequentially per launch, with at most `maxQueuedWrites`
waiting hooks; excess hooks are dropped, and stopping that launch cancels its hooks.

Incoming JSON-RPC envelopes are validated, including the version, IDs, and error fields. A response
must contain exactly one of `result` or `error`; an explicit `"result": null` is valid. Invalid envelopes
stop the process. Well-framed bodies that cannot be parsed as JSON are skipped. Child requests are
rejected with `-32601`; requests inside a batch receive one response array, excluding notifications.

## Modules

The repository publishes one JVM artifact, which JitPack serves under the repository name,
`com.github.maniramezan:process-rpc-kotlin`. Process execution is inherently host-specific, while the
products consuming it may still use Compose Multiplatform for their UI.

## Origin

This library was extracted from the MIT-licensed moqserver Studio process boundary and generalized
while building Amoo Studio. Both projects are intended consumers.

## Documentation

- [API reference](https://maniramezan.github.io/process-rpc-kotlin/)
- [Architecture](docs/architecture.md)
- [Contributing](CONTRIBUTING.md)
