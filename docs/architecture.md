# Architecture

Process RPC is deliberately limited to the transport and process boundary. Applications define
their own JSON-RPC methods and serializable DTOs.

## Lifecycle

1. `BundledBinaryLocator` resolves an explicit environment override, packaged Compose resource,
   or executable on `PATH`.
2. `ProcessRpcClient.start()` launches the command without blocking the caller.
3. `ProcessRpcState` reports starting, ready, unavailable, and stopped states.
4. `call()` assigns a numeric request ID and correlates the framed response.
5. `close()` terminates the child and fails outstanding calls.

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
