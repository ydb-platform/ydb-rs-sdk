# Progress

> **Volatile file** — append/update as work completes.

## What works (baseline)

- **Query API**: one-shot YQL (`exec`, `query_row`, streams), interactive `retry_tx`, execute-script + fetch results.
- **Table API**: DDL, `read_rows`, bulk upsert, per-call `.timeout()` on builders.
- **Operation API**: get/list/forget/cancel long-running operations with retries.
- **Scheme API**: directory listing, path operations.
- **Topics**: reader/writer with partitioning and offset management.
- **Coordination**: distributed semaphores (integration-tested).
- **Discovery**: endpoint discovery with load balancing strategies.
- **Auth**: static tokens, access tokens, JWT/metadata credentials.
- **TLS**: custom CA support, rustls via tonic.
- **Retries**: per-call deadline (`.timeout()`), driver-wide `RetryBudget` (`LimitedRetryBudget`, `PercentOfRpsRetryBudget`, `PercentRetryBudget`).
- **Session pool**: shared by table and query clients; default limit 50.

## CI status

- Lint: `cargo fmt --check` + `cargo clippy` on Rust 1.91.0.
- Tests: full workspace tests with `--include-ignored` against `ydbplatform/local-ydb:nightly` on Rust 1.82 and 1.91.0.

## Known issues / gaps

- Check GitHub Issues for active bugs and feature requests.
- Cross-SDK parity with Go/Java SDKs is tracked issue-by-issue.
- `ydb-grpc-helpers` is commented out of the workspace — status unclear for new contributors.
- Topic reader/writer reconnect retries are separate from driver `RetryBudget`.

## Milestones

| Date | Milestone |
|------|-----------|
| 2026-06 | Agent workspace under `.agents/` ([#428](https://github.com/ydb-platform/ydb-rs-sdk/issues/428)) |
| 2026-06 | Slim `AGENTS.md` router — selective `.agents/context/` reads, rules in `.agents/rules/` |
| 2026-06 | Per-call `.timeout()`, `retry_tx`, driver `RetryBudget` ([#516](https://github.com/ydb-platform/ydb-rs-sdk/pull/516)) — closes #511, #512; table `add_attribute`/`drop_attribute` (#410) |
| ongoing | Default gRPC message limits ([#417](https://github.com/ydb-platform/ydb-rs-sdk/pull/417)) merged |

## Changelog for agents

When making user-visible API or behavior changes, note them here briefly until a formal changelog process is adopted (unlike `ydb-go-sdk`, this repo does not yet require `CHANGELOG.md` entries).

**2026-06 — #516 merged**

- Removed per-client `clone_with_timeout` / `clone_with_retry` / per-call `retry_budget` on builders.
- Added per-call `.timeout()` on table/query/operation builders.
- Renamed `retry_transaction` → `retry_tx` on `QueryClient`.
- Added driver-wide `RetryBudget`: `clone_with_retry_budget`, `ClientBuilder::with_retry_budget`, `retry_metrics()`.
