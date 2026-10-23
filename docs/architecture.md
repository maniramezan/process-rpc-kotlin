# Architecture

Process RPC is deliberately limited to the transport and process boundary. Applications define
their own JSON-RPC methods and serializable DTOs.

## Lifecycle

1. `BundledBinaryLocator` resolves an explicit environment override, packaged Compose resource,
   or executable on `PATH` (with `PATHEXT` on Windows).
2. `ProcessRpcClient.start()` launches the command without blocking the caller. Concurrent calls
   launch one process.
3. `ProcessRpcState` reports starting, ready, unavailable, and stopped states. `Ready` means the
   process launched and its pipes are connected, not that it finished initializing.
4. `call()` assigns a numeric request ID and correlates the framed response. Calls made while the
   client is `Stopped` or `Unavailable` fail immediately; calls made while it is `Starting` wait.
5. `notify()` sends a JSON-RPC notification, which has no ID and gets no response.
6. `notifications` publishes messages from the child that carry a `method` and no `id`.
7. When the child exits or breaks the protocol, the client becomes `Unavailable` with the exit
   code and the last lines of standard error, fails outstanding calls, and consults its
   `RestartPolicy`.
8. `close()` fails outstanding calls, signals the child and its descendants to terminate, and kills
   them forcibly after `shutdownTimeoutMs`. `stop()` does the same gracefully: it closes the child's
   standard input first and waits for each step. Existing `notifications` and `standardError`
   collectors stay subscribed across restarts.

Each launch has a generation number and its own session, pending-call map, write queue, and background
tasks. Registration checks the active session under the lifecycle lock. Failure and shutdown detach
that session before completing its calls, so concurrent starts cannot have their calls swept up by
old cleanup. Readers and delayed restarts from an older generation cannot change a newer one's state.
Cancelling the supplied scope terminates its child, including when a reader is blocked on a pipe.

## Threads

While a child runs, the client blocks two threads from `Dispatchers.IO`: one reads standard output
and one drains standard error. Body encoding runs on `Dispatchers.Default` with a write-count
reservation taken first and byte reservations acquired incrementally. A bounded streaming encoder rejects oversized bodies without
allocating the entire oversized message. A writer coroutine consumes each session's bounded queue
and blocks an additional IO thread while writing. Caller coroutines wait for a completion signal and can cancel
without waiting for the pipe writer. A separate daemon scheduler enforces write and shutdown
deadlines. Termination uses `ProcessHandle` to avoid waiting for Java's pipe locks inside
`Process.destroy()`. Descendants are captured before shutdown signals or stdin closure can orphan them.

## Notifications

The reader thread never waits on collectors. `notifications` is a hot `SharedFlow` with a bounded
buffer (`notificationBufferCapacity`, default 1,024) that drops the oldest message when a collector
falls behind. Notifications sent before anything subscribes are not replayed. Protocols that cannot
tolerate loss should number their notifications and offer a replay method. `standardError` behaves
the same way with a buffer of 256 lines.

Each notification receives a monotonically increasing client sequence number. `notificationEvents`
tracks the last observed sequence separately for each collector and reports gaps through
`droppedBefore`. A gap becomes observable when that collector resumes and receives another message;
notifications from before subscription are excluded. Sequences continue across child restarts.

The client does not serve requests. A child request (one with both `method` and `id`) is answered
with JSON-RPC error `-32601`. Batch requests receive one response array, omitting notifications.

## Cancellation and timeouts

Cancelling a caller cancels its `call()` normally. A call that outlives its timeout throws
`ProcessRpcException`. In both cases the child may still be working on the request; the
`onCallAbandoned` hook lets the protocol tell it to stop, for example with an LSP-style
`$/cancelRequest` notification. Hooks run sequentially per session, with a bounded queue; overflow
drops hooks, and stopping a session cancels its hooks.

The per-call timeout covers queueing, writing, and receiving the response after readiness. A separate
`writeTimeoutMs` deadline bounds body encoding and every write, including rejected child requests.
The session's writer, not the caller, owns each frame, so a caller that is cancelled or times out
never truncates one: a started write runs to completion and a queued one is skipped. If the writer
itself cannot finish within `writeTimeoutMs`, the client kills the process, because the stream may
then hold a partial frame. A stopped session never shares its writer with a new launch.

`stop()` runs its cleanup in a non-cancellable context. It closes stdin on an IO worker and bounds
the graceful wait, then signals termination and bounds that wait too. An independent deadline
forcibly kills captured processes even if closing stdin blocks. `close()` schedules escalation and
signals processes without closing pipes on the caller's thread.

`maxPendingCalls` bounds concurrent calls per launch. `maxQueuedWrites` bounds waiting messages and
abandonment hooks; full queues fail additional sends immediately. A full queue of child-request
rejections fails that session with `RpcFailureKind.OVERLOADED`. `maxQueuedWriteBytes` bounds the
combined encoded body bytes of in-progress encoders, queued messages, and the active writer. Queue
slots are reserved before encoding; bytes are reserved before each encoded chunk is retained. Failed
encodings and cancelled queued writes release their reservations. Completed writes release their
body and budget before signalling completion. Caller-owned JSON trees, encoder buffers, and transient
buffer copies are outside the encoded-byte budget. These limits avoid launching a coroutine per
child request.

## Wire format

Messages use JSON-RPC 2.0 bodies wrapped in LSP-style Content-Length framing:

```text
Content-Length: 49\r\n
\r\n
{"jsonrpc":"2.0","id":1,"method":"system.health"}
```

Header names are case-insensitive and headers other than `Content-Length` are ignored. Headers
are limited to 8 KiB and bodies to `maxMessageBytes` (64 MiB by default); exceeding either, or a
missing, duplicate, or invalid `Content-Length`, is a protocol error that stops the child. Outgoing
bodies share the message-size limit and are rejected before queueing. A well-framed body that is not
valid JSON is skipped. Parsed envelopes require the JSON-RPC version, valid IDs, and exactly one
response result or error. Errors require an integer code and string message. Explicit null results
are accepted. Nonempty batch arrays are handled element by element; request rejections are grouped
into one response array. Invalid envelopes stop the child. Outgoing params must be an object or array; raw
and typed calls reject scalar and null params locally with `RpcFailureKind.INVALID_PARAMS`.

Standard output is reserved for protocol frames. Child diagnostics should use standard error,
which the client drains independently to prevent pipe backpressure and publishes on
`standardError`. The reader retains at most `maxStandardErrorLineBytes` (8 KiB by default) from each
line, discards its remaining bytes until a newline, and marks the prefix `[truncated]`. The 256-line
flow buffer and 20-line diagnostic tail therefore cannot retain arbitrarily long lines.

## Boundaries

The library does not own application methods, model providers, UI state, device automation, or
business errors. Consumers translate `JsonElement` results and `ProcessRpcException` into their
own domain types, directly or with the `callAs` and `paramsAs` helpers. Exceptions and unavailable
states carry a `RpcFailureKind`, so callers can distinguish recovery actions without parsing text.
