# Contributing

Run the tests and the public API check before opening a pull request:

```bash
./gradlew check
```

`check` compares the public API with the reference dump in `api/`. When a change to the public API
is intended, regenerate the dump and commit it with the change, so reviewers see the API diff:

```bash
./gradlew updateKotlinAbi
```

The library uses Kotlin's explicit API mode, so every public declaration needs a visibility modifier
and should carry KDoc. It compiles for JVM 17 using a JDK 21 toolchain; CI runs on Linux, macOS,
and Windows because process behavior differs between them. Each platform tests on JVM 17 and 21.

Run the isolated consumer against a build-local Maven publication as well:

```bash
./gradlew check consumerSmokeTest -PtestJavaVersion=17
./gradlew check consumerSmokeTest -PtestJavaVersion=21
```

Both JDKs must be installed. The consumer depends on the published artifact and its transitive
dependencies, rather than on the root project's classes. It checks typed calls, notifications, stderr,
graceful shutdown, and starting again. The smoke repository stays under `build/`.

To verify an existing JitPack release, run the same consumer directly against its public coordinates:

```bash
./gradlew -p integration/consumer run -PrpcRepository=https://jitpack.io -PrpcCoordinates=com.github.maniramezan:process-rpc-kotlin:0.3.0 -PtestJavaVersion=17
```

Keep the library product-neutral. Application RPC methods, domain DTOs, Compose UI, and provider
logic belong in consuming applications.
