# Testing

## Unit tests (default)

```bash
cargo test --workspace
```

No YDB instance required. Integration tests are excluded unless `--include-ignored` is passed.

## Integration tests

- Live in `*_test_integration.rs` files.
- Marked `#[ignore]` — gated on `YDB_CONNECTION_STRING` (see `test_integration_helper`).
- Run with a local YDB instance:

```bash
export YDB_CONNECTION_STRING='grpc://localhost:2136?database=/local'
cargo test --workspace -- --include-ignored
```

Prefer `docker compose up -d` from repo root (`docker-compose.yaml` uses `ydbplatform/local-ydb:latest`) over ad-hoc `docker run` with a different image tag.

## CI parity

Before requesting review:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --no-deps --exclude=ydb-grpc -- -D warnings
cargo test --workspace
```

CI also runs `cargo test --workspace -- --include-ignored` against `ydbplatform/local-ydb:nightly` on Rust 1.82 and 1.91.0.

## `ydb-grpc`

Generated protobuf crate — clippy is excluded. Do not hand-edit generated files.
