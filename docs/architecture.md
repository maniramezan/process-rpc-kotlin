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

Each launch has a generation number. A reader or restart left over from an earlier launch can never
change the state of a newer one, so `close()` followed by `start()` is safe at any moment.

## Threads

While a child runs, the client blocks two threads from `Dispatchers.IO`: one reads standard output
and one drains standard error. Writes also run on `Dispatchers.IO`, serialized by a mutex, so a
caller on a UI dispatcher never blocks on a full pipe. None of this work runs on the caller's
`scope` dispatcher.

## Notifications

The reader thread never waits on collectors. `notifications` is a hot `SharedFlow` with a bounded
buffer (`notificationBufferCapacity`, default 1,024) that drops the oldest message when a collector
falls behind. Notifications sent before anything subscribes are not replayed. Protocols that cannot
tolerate loss should number their notifications and offer a replay method. `standardError` behaves
the same way with a buffer of 256 lines.

The client does not serve requests. A child request (one with both `method` and `id`) is answered
with JSON-RPC error `-32601`, so the child is never left waiting.

## Cancellation and timeouts

Cancelling a caller cancels its `call()` normally. A call that outlives its timeout throws
`ProcessRpcException`. In both cases the child may still be working on the request; the
`onCallAbandoned` hook lets the protocol tell it to stop, for example with an LSP-style
`$/cancelRequest` notification.

## Wire format

Messages use JSON-RPC 2.0 bodies wrapped in LSP-style Content-Length framing:

```text
Content-Length: 49\r\n
\r\n
{"jsonrpc":"2.0","id":1,"method":"system.health"}
```

Header names are case-insensitive and headers other than `Content-Length` are ignored. Headers
are limited to 8 KiB and bodies to `maxMessageBytes` (64 MiB by default); exceeding either, or a
missing or invalid `Content-Length`, is a protocol error that stops the child, because the stream can
no longer be trusted. A well-framed body that is not valid JSON is skipped. Batch arrays from the
child are handled element by element.

Standard output is reserved for protocol frames. Child diagnostics should use standard error,
which the client drains independently to prevent pipe backpressure and publishes on
`standardError`.

## Boundaries

The library does not own application methods, model providers, UI state, device automation, or
business errors. Consumers translate `JsonElement` results and `ProcessRpcException` into their
own domain types, directly or with the `callAs` and `paramsAs` helpers.
