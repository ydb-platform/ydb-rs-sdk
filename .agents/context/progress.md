# Progress

> **Volatile file** — append/update as work completes.

## What works (baseline)

- **Table API**: queries, transactions with automatic retry, session pool, bulk upsert.
- **Scheme API**: directory listing, path operations (evolving — see open PRs).
- **Topics**: reader/writer with partitioning and offset management.
- **Coordination**: distributed semaphores (integration-tested).
- **Discovery**: endpoint discovery with load balancing strategies.
- **Auth**: static tokens, access tokens, JWT/metadata credentials.
- **TLS**: custom CA support, rustls via tonic.

## CI status

- Lint: `cargo fmt --check` + `cargo clippy` on Rust 1.91.0.
- Tests: full workspace tests with `--include-ignored` against `ydbplatform/local-ydb:nightly` on Rust 1.82 and 1.91.0.

## Known issues / gaps

- Check GitHub Issues for active bugs and feature requests.
- Cross-SDK parity with Go/Java SDKs is tracked issue-by-issue.
- `ydb-grpc-helpers` is commented out of the workspace — status unclear for new contributors.

## Milestones

| Date | Milestone |
|------|-----------|
| 2026-06-19 | OpenTelemetry tracing instrumentation — Phases 1-3, 5-7 complete (Phase 4 W3C propagation skipped) |
| 2026-06 | Agent workspace under `.agents/` ([#428](https://github.com/ydb-platform/ydb-rs-sdk/issues/428)) |
| 2026-06 | Slim `AGENTS.md` router — selective `.agents/context/` reads, rules in `.agents/rules/` |
| ongoing | Default gRPC message limits ([#417](https://github.com/ydb-platform/ydb-rs-sdk/pull/417)) merged |

## Changelog for agents

### 2026-06-19: OpenTelemetry Tracing Instrumentation

Implemented comprehensive OpenTelemetry tracing instrumentation following YDB SDK conventions:

**Instrumented layers:**
- **gRPC layer**: All raw service clients (Query, Table, Scheme, Coordination, Topic, Auth, Operation, Discovery) — 44 methods total
- **Public API**: QueryClient, TableClient, SchemeClient, CoordinationClient, TopicClient, OperationClient, Client (Driver) — 35+ methods
- **Connection/Session**: ConnectionPool, GrpcConnectionManager, QuerySessionPool

**Naming conventions:**
- Query Service: Special names (`ydb.RunWithRetry`, `ydb.Try`, `ydb.ExecuteQuery`, `ydb.BeginTransaction`, etc.)
- Other public API: `ydb.<TypeName>.<MethodName>` (CamelCase)
- gRPC internal: `ydb.grpc.<MethodName>`

**Attributes:**
- Standard OTel: `db.system.name`, `db.namespace`, `server.address`, `network.peer.address`, etc.
- YDB-specific: `ydb.session.id`, `ydb.tx.id`, `ydb.tx.mode`, `ydb.query.text` (truncated to 1000 chars), etc.

**Retry instrumentation:**
- `ydb.RunWithRetry` wraps entire retry cycle
- `ydb.Try` child spans for each attempt with `ydb.retry.attempt`, `ydb.retry.backoff_ms`

**Files:** See `.kilo/plans/tracing-instrumentation.md` for full implementation details.
