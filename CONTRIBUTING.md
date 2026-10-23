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
and Windows because process behavior differs between them.

Keep the library product-neutral. Application RPC methods, domain DTOs, Compose UI, and provider
logic belong in consuming applications.
