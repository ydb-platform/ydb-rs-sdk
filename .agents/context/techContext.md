# Tech Context

## Toolchain

| Item | Value |
|------|-------|
| Edition | 2021 |
| MSRV | 1.88 (`rust-version` in workspace `Cargo.toml`; Query `retry_tx` needs `AsyncFnMut`) |
| CI Rust versions | 1.88 (tests + proto generation), 1.96 (fmt + lint + tests + publish + SLO workload builds) |
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
| `linter.yaml` | push/PR to `master` | `cargo fmt --check`, `cargo clippy` on Rust 1.96 |
| `rust-tests.yml` | push/PR + nightly cron | `cargo test --workspace --doc`, `cargo test --include-ignored` against `local-ydb:nightly` on MSRV and Rust 1.96 |
| `publish-crate.yml` | manual dispatch | version bump + crates.io publish on Rust 1.96 |
| `slo.yml` | PR label `SLO` + manual dispatch | SLO tests via `ydb-slo-action` v2; workload Docker images build on Rust 1.96 |
| `slo-report.yml` | after `SLO` workflow | Publishes SLO report to PR comment |
| `dependencies.yml` | push/PR + nightly cron | `cargo deny` on the published graph (bans/licenses/sources) and advisories on the whole workspace, plus the `[workspace.dependencies]` inheritance check |

## Workspace dependency policy

Every third-party dependency of every workspace member is declared once, in the root `Cargo.toml` under `[workspace.dependencies]`; members inherit it with `{ workspace = true }` and may only add `features`. A version requirement in a member manifest is a CI failure (`.github/scripts/check_workspace_deps.py`), as is a `[workspace.dependencies]` entry that no member inherits. The single documented exception is the OpenTelemetry stack of `slo-framework`, which is a major version behind the one the `ydb` examples use; the exemption list lives in that script.

One requirement per crate keeps a crate at one version in the resolved tree. `deny.toml` enforces the result:

```bash
# the graph a user of the published crates compiles: a second version of any
# crate is an error
cargo deny --locked --all-features --exclude-unpublished --exclude-dev \
    check bans licenses sources -D unmatched-skip
# RustSec advisories over everything built in this repository
cargo deny --locked --all-features check advisories
```

Duplicates that the workspace cannot collapse are listed in `[bans].skip` with the crates that keep them alive. `-D unmatched-skip` turns an entry that no longer matches into a failure, so the list has to be pruned when a bump makes it obsolete. Same for `[advisories].ignore`: the only entry is `RUSTSEC-2024-0388` (`derivative` unmaintained, being replaced).

`--exclude-unpublished` drops the `publish = false` SLO workloads from the graph, `--exclude-dev` drops dev-dependencies; both are checked for advisories by the second invocation but are not part of the single-version rule.

MSRV-sensitive Clippy checks are enabled in root `[workspace.lints.clippy]` via `incompatible_msrv = "warn"`. Clippy derives the MSRV from each package's `rust-version`, inherited from root `[workspace.package]`; CI runs Clippy on Rust 1.96 and promotes warnings to errors with `-D warnings`.

The protobuf regeneration container (`ydb-grpc/generate-protobuf.Dockerfile`) intentionally uses Rust 1.88.0 to keep generated code buildable on the declared MSRV. SLO workload images intentionally use Rust 1.96.0.

Do not run `cargo update` or bump dependency versions unless the task requires it.

## Features

- `force-exhaustive-all` on `ydb` crate — removes `#[non_exhaustive]` for compile-time enum coverage in downstream crates.

## Publishing

- Manual workflow selects crate (`ydb` / `ydb-grpc` / `ydb-grpc-helpers`) and version part (`patch` / `minor`).
- Script: `.github/scripts/version-up.sh`.
