# Environment — quick commands

Authoritative CI/toolchain reference: [`.agents/context/techContext.md`](../context/techContext.md).

## Local YDB

```bash
docker compose up -d
export YDB_CONNECTION_STRING='grpc://localhost:2136?database=/local'
```

## Build, lint, test

```bash
cargo fmt
cargo clippy --workspace --all-targets --no-deps --exclude=ydb-grpc -- -D warnings
cargo test --workspace
cargo test --workspace -- --include-ignored   # needs YDB
```
