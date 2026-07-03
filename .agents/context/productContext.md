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
| Long-running server operations | `Client::operation_client()` — poll index builds, backups, etc. (`get_operation`, `list_operations`, `forget_operation`, `cancel_operation`) |
| Produce/consume topic messages | `Client::topic_client()` — reader/writer APIs |
| Distributed locks / semaphores | `Client::coordination_client()` |
| Auth (static token, JWT, metadata) | `ClientBuilder::with_credentials`, credential types in `credentials.rs` |
| Multi-node clusters | Discovery + load balancers (`random`, `static`, `nearest_dc`) |
| Limit retry storm under load | `Client::clone_with_retry_budget`, `RetryBudget` trait (rate limiter — not a call timeout) |

## Table vs Query (intentional split)

| Client | Use for | Do **not** use for |
|--------|---------|-------------------|
| `TableClient` | DDL (`create_table`, `alter_table`, …), `describe_table`, `read_rows`, `bulk_upsert` | Arbitrary SQL/YQL |
| `QueryClient` | YQL (`exec`, `query_row`, streams), `retry_tx`, `execute_script` | Table DDL (use table client) |

Both share one **session pool** on the driver (`Client::with_session_pool`). Automatic retries apply on both; per-call options (`.timeout()`, `.idempotent()`) are set on builders, not via `clone_with_*`.

## Developer experience goals

- **Connection string** as the primary entry point: `grpc://host:port/database`.
- **Automatic retries** on retriable errors with per-call `.timeout()` and optional driver-wide retry budget.
- **Shared session pool** between table and query clients (default limit 50; configurable via `with_session_pool`).
- **Type-safe row access** via `result` types, `FromYdbRow`, and `try_into` conversions.
- **Examples**: `ydb/examples/` — runnable `cargo example` snippets.

## API stability

- Published on crates.io as `ydb` (**0.16.0** ships the table/query refactor and breaking API cleanup).
- **Pre-1.0 policy**: breaking changes to awkward or misleading API are acceptable before `1.0.0` — prefer fixing design early over carrying compatibility debt.
- MSRV **1.85** (Query `retry_tx` uses `AsyncFnMut`).
- `#[non_exhaustive]` on many public enums; optional `force-exhaustive-all` feature for compile-time exhaustiveness checks.
- Breaking changes increment `0.X` per project policy (see root `README.md`).

### RetryBudget vs `.timeout()` (do not confuse)

| Mechanism | What it limits | Where to set |
|-----------|----------------|--------------|
| `.timeout(d)` on a call builder | Wall-clock budget for that operation (attempts + backoff + waiting for budget quota) | Per call |
| `RetryBudget` | **Rate** of retry attempts across the driver (anti-DDOS under failures) | `ClientBuilder::with_retry_budget` or `clone_with_retry_budget` |

Older per-call `.retry_budget()` on builders was removed — it duplicated timeout semantics incorrectly. See [ydb-go-sdk `retry/budget`](https://github.com/ydb-platform/ydb-go-sdk/tree/master/retry/budget) for the theory.

## Related resources

- [docs.rs/ydb](https://docs.rs/ydb) — API reference
- [YDB documentation](https://ydb.tech/docs) — server-side concepts, YQL
- [ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk) — reference for cross-SDK feature parity (retry budget: `retry/budget`)
