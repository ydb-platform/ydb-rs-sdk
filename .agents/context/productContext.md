# Product Context

## Users

- Rust application developers connecting to YDB for OLTP workloads, streaming (topics), and metadata (scheme).
- Maintainers of the YDB platform integrating Rust into internal services.
- Contributors extending SDK coverage to match other language SDKs (Go, Java, etc.).

## Problems solved

| Need | SDK surface |
|------|-------------|
| Run YQL via Query Service | `Client::query_client()` — `exec`, `query_row`, `retry_tx` |
| Table DDL / point reads / bulk upsert | `Client::table_client()` |
| Browse database directory / schema | `Client::scheme_client()` |
| Long-running operations | `Client::operation_client()` — get/list/forget/cancel |
| Produce/consume topic messages | `Client::topic_client()` — reader/writer APIs |
| Distributed locks / semaphores | `Client::coordination_client()` |
| Auth (static token, JWT, metadata) | `ClientBuilder::with_credentials`, credential types in `credentials.rs` |
| Multi-node clusters | Discovery + load balancers (`random`, `static`, `nearest_dc`) |
| Limit retry storm under load | `Client::clone_with_retry_budget`, `RetryBudget` trait |

## Developer experience goals

- **Connection string** as the primary entry point: `grpc://host:port/database`.
- **Automatic retries** on retriable errors with per-call `.timeout()` and optional driver-wide retry budget.
- **Shared session pool** between table and query clients (default limit 50; configurable via `with_session_pool`).
- **Type-safe row access** via `result` types, `FromYdbRow`, and `try_into` conversions.
- **Examples**: `ydb/examples/` — runnable `cargo example` snippets.

## API stability

- Published on crates.io as `ydb` (currently `0.16.x`).
- MSRV **1.85** (Query `retry_tx` uses `AsyncFnMut`).
- `#[non_exhaustive]` on many public enums; optional `force-exhaustive-all` feature for compile-time exhaustiveness checks.
- Breaking changes increment `0.X` per project policy (see root `README.md`).

## Related resources

- [docs.rs/ydb](https://docs.rs/ydb) — API reference
- [YDB documentation](https://ydb.tech/docs) — server-side concepts, YQL
- [ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk) — reference for cross-SDK feature parity (retry budget: `retry/budget`)
