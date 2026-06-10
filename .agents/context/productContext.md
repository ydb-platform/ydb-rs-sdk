# Product Context

## Users

- Rust application developers connecting to YDB for OLTP workloads, streaming (topics), and metadata (scheme).
- Maintainers of the YDB platform integrating Rust into internal services.
- Contributors extending SDK coverage to match other language SDKs (Go, Java, etc.).

## Problems solved

| Need | SDK surface |
|------|-------------|
| Run YQL queries and transactions | `Client::table_client()`, `retry_transaction`, `Query` |
| Browse database directory / schema | `Client::scheme_client()` |
| Produce/consume topic messages | `Client::topic_client()` — reader/writer APIs |
| Distributed locks / semaphores | `Client::coordination_client()` |
| Auth (static token, JWT, metadata) | `ClientBuilder::with_credentials`, credential types in `credentials.rs` |
| Multi-node clusters | Discovery + load balancers (`random`, `static`, `nearest_dc`) |

## Developer experience goals

- **Connection string** as the primary entry point: `grpc://host:port?database=/path`.
- **Automatic retries** on retriable errors for table operations (configurable).
- **Type-safe row access** via `result` types and `try_into` conversions.
- **Examples**: `ydb/examples/` for small snippets and `ydb/examples/urlshortener/` for a full app.

## API stability

- Published on crates.io as `ydb` (currently `0.12.x`).
- `#[non_exhaustive]` on many public enums; optional `force-exhaustive-all` feature for compile-time exhaustiveness checks.
- Breaking changes increment `0.X` per project policy (see root `README.md`).

## Related resources

- [docs.rs/ydb](https://docs.rs/ydb) — API reference
- [YDB documentation](https://ydb.tech/docs) — server-side concepts, YQL
- [ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk) — reference for cross-SDK feature parity
