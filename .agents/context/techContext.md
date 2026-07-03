# Tech Context

## Toolchain

| Item | Value |
|------|-------|
| Edition | 2021 |
| MSRV | 1.85 (`rust-version` in `ydb/Cargo.toml`; Query `retry_tx` needs `AsyncFnMut`) |
| CI Rust versions | 1.82 (tests), 1.91.0 (tests + lint) |
| Async runtime | Tokio 1.x |
| gRPC | tonic 0.14, prost 0.14, pbjson 0.8 |
| TLS | rustls via tonic features (`tls-ring`, `tls-native-roots`) |

## Local development

```bash
cargo build --workspace
cargo test --workspace          # unit only; integration tests are #[ignore]
cargo fmt
cargo clippy --workspace --all-targets --no-deps --exclude=ydb-grpc -- -D warnings
```

### Integration tests with local YDB

**Recommended** — repo `docker-compose.yaml` (`ydbplatform/local-ydb:latest`):

```bash
docker compose up -d
export YDB_CONNECTION_STRING='grpc://localhost:2136/local'
cargo test --workspace -- --include-ignored
```

CI uses `ydbplatform/local-ydb:nightly` (see `rust-tests.yml`); image tag may differ from local compose.

## CI workflows

| Workflow | Trigger | What it runs |
|----------|---------|--------------|
| `linter.yaml` | push/PR to `master` | `cargo fmt --check`, `cargo clippy` |
| `rust-tests.yml` | push/PR + nightly cron | `cargo test --include-ignored` against `local-ydb:nightly` |
| `publish-crate.yml` | manual dispatch | version bump + crates.io publish |
| `slo.yml` | PR label `SLO` + manual dispatch | SLO tests via `ydb-slo-action` v2 (Docker workload, chaos, baseline comparison); see `tests/slo/README.md` |
| `slo-report.yml` | after `SLO` workflow | Publishes SLO report to PR comment |

## Workspace dependency policy

Shared versions for `prost`, `tonic`, `pbjson` are declared in the root `Cargo.toml` under `[workspace.dependencies]`. Member crates reference them with `workspace = true`.

Do not run `cargo update` or bump dependency versions unless the task requires it.

## Features

- `force-exhaustive-all` on `ydb` crate — removes `#[non_exhaustive]` for compile-time enum coverage in downstream crates.

## Publishing

- Manual workflow selects crate (`ydb` / `ydb-grpc` / `ydb-grpc-helpers`) and version part (`patch` / `minor`).
- Script: `.github/scripts/version-up.sh`.
