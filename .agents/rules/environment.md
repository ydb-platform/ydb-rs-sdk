# Environment — quick commands

Authoritative CI/toolchain reference: [`.agents/context/techContext.md`](../context/techContext.md).

## Local YDB

```bash
docker compose up -d
export YDB_CONNECTION_STRING='grpc://localhost:2136/local'
```

## Build, lint, test

**Lint is mandatory before handoff** — same commands as CI:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --no-deps --exclude=ydb-grpc -- -D warnings
cargo test --workspace
cargo test --workspace -- --include-ignored   # needs YDB
```

Run fmt + clippy after every change set; do not skip clippy when only tests were added.
