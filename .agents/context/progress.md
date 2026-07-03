# Progress

> **Volatile file** — append/update as work completes.

## What works (baseline)

- **Table API** (feature-complete for known scope): DDL, describe, `read_rows`, `bulk_upsert` — **no SQL**; per-call `.timeout()` on builders.
- **Query API** (feature-complete for known scope): one-shot YQL (`exec`, `query_row`, streams), interactive `retry_tx`, execute-script + fetch results.
- **Operation API**: get/list/forget/cancel long-running server work (e.g. index build, backup).
- **Scheme API**: directory listing, path operations.
- **Topics**: reader/writer with partitioning and offset management; internal optimizations ongoing.
- **Coordination**: distributed semaphores (integration-tested).
- **Discovery**: endpoint discovery with load balancing strategies.
- **Auth**: static tokens, access tokens, JWT/metadata credentials.
- **TLS**: custom CA support, rustls via tonic.
- **Retries**: per-call deadline (`.timeout()`), driver-wide `RetryBudget` rate limiter (`LimitedRetryBudget`, `PercentOfRpsRetryBudget`, `PercentRetryBudget`).
- **Session pool**: shared by table and query clients; default limit 50.
- **Resilience**: SLO/chaos workloads (`tests/slo/`, CI label `SLO`) show good client survival under cluster failures.

## CI status

- Lint: `cargo fmt --check` + `cargo clippy` on Rust 1.91.0.
- Tests: full workspace tests with `--include-ignored` against `ydbplatform/local-ydb:nightly` on Rust 1.82 and 1.91.0.

## Known issues / gaps

- Check GitHub Issues for active bugs and feature requests — table/query gaps should be reported if something is still missing.
- Cross-SDK parity with Go/Java SDKs is tracked issue-by-issue.
- `ydb-grpc-helpers` is commented out of the workspace — status unclear for new contributors.
- **Topic client**: active internal work (features, optimizations); reader/writer reconnect retries use separate `Retry`, not driver `RetryBudget`.

## Milestones

| Date | Milestone |
|------|-----------|
| 2026-06 | Agent workspace under `.agents/` ([#428](https://github.com/ydb-platform/ydb-rs-sdk/issues/428)) |
| 2026-06 | Slim `AGENTS.md` router — selective `.agents/context/` reads, rules in `.agents/rules/` |
| 2026-06 | **ydb 0.16.0** — table/query refactor, operation client, driver `RetryBudget`, per-call builders ([#516](https://github.com/ydb-platform/ydb-rs-sdk/pull/516)) |
| 2026-06 | Per-call `.timeout()`, `retry_tx`, driver `RetryBudget` — closes #511, #512; table `add_attribute`/`drop_attribute` (#410) |
| ongoing | Default gRPC message limits ([#417](https://github.com/ydb-platform/ydb-rs-sdk/pull/417)) merged |

## Changelog for agents

When making user-visible API or behavior changes, note them here briefly until a formal changelog process is adopted (unlike `ydb-go-sdk`, this repo does not yet require `CHANGELOG.md` entries).

**2026-06 — ydb 0.16.0 / #516**

- Table vs Query split documented: table = DDL + read_rows + bulk_upsert only; SQL via Query Service.
- Removed per-client `clone_with_*` and mistaken per-call `.retry_budget()` (was timeout-like).
- Added per-call `.timeout()` / `.idempotent()` on builders; `retry_transaction` → `retry_tx`.
- Driver-wide `RetryBudget` (rate limiter): `clone_with_retry_budget`, `ClientBuilder::with_retry_budget`, `retry_metrics()`.
- Operation client for long-running async server operations.
