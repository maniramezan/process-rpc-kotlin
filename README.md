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

```kotlin
repositories {
    maven("https://jitpack.io")
}

dependencies {
    implementation("com.github.maniramezan:process-rpc-kotlin:0.1.0")
}
```

Then launch and call a service:

```kotlin
val client = ProcessRpcClient(
    command = { listOf("my-service", "serve") },
)
client.start()
val handshake = client.call("system.handshake")
```

Subscribe to notifications pushed by the child, such as progress or streamed events:

```kotlin
scope.launch {
    client.notifications.collect { notification ->
        println("${notification.method}: ${notification.params}")
    }
}
```

The library does not prescribe application RPC methods or DTOs. Callers exchange
`kotlinx.serialization.json.JsonElement` values at the transport boundary.

## Modules

The initial repository publishes one JVM artifact, `process-rpc`. Process execution is inherently
host-specific, while the products consuming it may still use Compose Multiplatform for their UI.

## Origin

This library was extracted from the MIT-licensed moqserver Studio process boundary and generalized
while building Amoo Studio. Both projects are intended consumers.

## Documentation

- [API reference](https://maniramezan.github.io/process-rpc-kotlin/)
- [Architecture](docs/architecture.md)
- [Contributing](CONTRIBUTING.md)
