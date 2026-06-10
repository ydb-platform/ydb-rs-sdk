# Environment and CI

## Toolchain

| Item | Value |
|------|-------|
| Edition | 2021 |
| MSRV | 1.82 |
| Async | Tokio 1.x |
| gRPC | tonic 0.14, prost 0.14 |

Full details: [`.agents/context/techContext.md`](../context/techContext.md).

## Local YDB (recommended)

From repo root:

```bash
docker compose up -d
export YDB_CONNECTION_STRING='grpc://localhost:2136?database=/local'
```

`docker-compose.yaml` exposes ports 2135 (TLS) and 2136 (plain gRPC) with anonymous credentials.

## Build and lint

```bash
cargo build --workspace
cargo fmt
cargo clippy --workspace --all-targets --no-deps --exclude=ydb-grpc -- -D warnings
```

## CI workflows

| Workflow | Trigger | What it runs |
|----------|---------|--------------|
| `linter.yaml` | push/PR to `master` | `cargo fmt --check`, `cargo clippy` |
| `rust-tests.yml` | push/PR + nightly cron | tests with `--include-ignored` vs `local-ydb:nightly` |
| `publish-crate.yml` | manual dispatch | version bump + crates.io publish |
| `slo.yml` | push/PR to `master` + manual dispatch | SLO tests |
