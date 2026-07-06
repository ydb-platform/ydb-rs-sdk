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
export YDB_CONNECTION_STRING='grpc://localhost:2136/local'
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

CI also runs `cargo test --workspace --doc` and `cargo test --workspace -- --include-ignored` against `ydbplatform/local-ydb:nightly` on Rust 1.88 and stable.

## What a test should assert

Assert the specific fact the test is meant to defend — the concrete error type / status code or the concrete successful result — not a proxy symptom.

Counting messages, sleeping for "a while", or matching on log output passes both when the code is correct and when an unrelated regression makes the proxy coincidentally match. The test is then green while the bug ships.

Also cover paths the code explicitly rejects, so a future relaxation of those checks does not slip through unnoticed.

## Integration test data

- Use stable, deterministic table/topic names. Start the test with `DROP TABLE IF EXISTS …` → `CREATE …`.
- Do **not** drop on teardown — a failed run is more debuggable if its data is still there.

## `ydb-grpc`

Generated protobuf crate — clippy is excluded. Do not hand-edit generated files.
