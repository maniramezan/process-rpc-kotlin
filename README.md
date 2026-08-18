# Process RPC for Kotlin

Product-neutral Kotlin/JVM infrastructure for launching a bundled native executable and calling
it through JSON-RPC 2.0 over Content-Length-framed stdio.

This module was extracted from the MIT-licensed moqserver Studio process boundary. It deliberately
contains no Amoo, moqserver, Compose, or application-domain types, so it can become a standalone
open-source library without either product depending on the other.

## Usage

```kotlin
val client = ProcessRpcClient(
    command = { listOf("my-service", "serve") },
)
client.start()
val handshake = client.call("system.handshake")
```

The library does not prescribe application RPC methods or DTOs. Callers exchange
`kotlinx.serialization.json.JsonElement` values at the transport boundary.

## Modules

The initial repository publishes one JVM artifact, `process-rpc`. Process execution is inherently
host-specific, while the products consuming it may still use Compose Multiplatform for their UI.

## Origin

This library was extracted from the MIT-licensed moqserver Studio process boundary and generalized
while building Amoo Studio. Both projects are intended consumers.
