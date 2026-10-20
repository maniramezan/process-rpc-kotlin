# Architecture

Process RPC is deliberately limited to the transport and process boundary. Applications define
their own JSON-RPC methods and serializable DTOs.

## Lifecycle

1. `BundledBinaryLocator` resolves an explicit environment override, packaged Compose resource,
   or executable on `PATH`.
2. `ProcessRpcClient.start()` launches the command without blocking the caller.
3. `ProcessRpcState` reports starting, ready, unavailable, and stopped states.
4. `call()` assigns a numeric request ID and correlates the framed response.
5. `notify()` sends a JSON-RPC notification, which has no ID and gets no response.
6. `notifications` publishes messages from the child that carry a `method` and no `id`.
7. `close()` terminates the child and fails outstanding calls. Existing `notifications` collectors
   stay subscribed across restarts.

## Notifications

The reader thread never waits on collectors. `notifications` is a hot `SharedFlow` with a bounded
buffer (`notificationBufferCapacity`, default 1,024) that drops the oldest message when a collector
falls behind. Notifications sent before anything subscribes are not replayed. Protocols that cannot
tolerate loss should number their notifications and offer a replay method.

The client does not serve requests. A child request (one with both `method` and `id`) is answered
with JSON-RPC error `-32601`, so the child is never left waiting.

## Wire format

Messages use JSON-RPC 2.0 bodies wrapped in LSP-style Content-Length framing:

```text
Content-Length: 58\r\n
\r\n
{"jsonrpc":"2.0","id":1,"method":"system.health"}
```

Standard output is reserved for protocol frames. Child diagnostics should use standard error,
which the client drains independently to prevent pipe backpressure.

## Boundaries

The library does not own application methods, model providers, UI state, device automation, or
business errors. Consumers translate `JsonElement` results and `ProcessRpcException` into their
own domain types.
