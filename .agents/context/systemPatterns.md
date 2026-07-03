# System Patterns

## Workspace structure

```
ydb-rs-sdk/
├── ydb/                            # Public SDK (main crate)
│   └── examples/                   # cargo example targets (*.rs)
├── ydb-grpc/                       # Generated protobuf + tonic stubs
├── tests/slo/                      # SLO workloads (ydb-slo-action)
└── .agents/                        # Agent workspace (context/, rules/)
```

## Layered architecture (`ydb` crate)

```
ClientBuilder
    └── Client                    # driver: discovery, session pool, retry budget
            ├── table_client()    # Table API (DDL, read_rows, bulk upsert)
            ├── query_client()    # Query Service (YQL, retry_tx, scripts)
            ├── scheme_client()   # directory listing, describe path
            ├── topic_client()    # readers/writers
            ├── coordination_client()
            └── operation_client()  # long-running operations (get/list/forget/cancel)

Client
    ├── SessionPool               # shared CreateSession + AttachSession (table + query)
    ├── RetryControl              # driver-wide RetryBudget + RetryMetrics (RPS counters)
    └── GrpcConnectionManager     # per-endpoint channels + interceptors
            └── grpc_wrapper/     # raw tonic clients
                    └── ydb-grpc  # prost message types
```

Service clients (`TableClient`, `QueryClient`, …) are lightweight handles cloned from `Client`; they share the same pool, connection manager, and retry budget.

## Table vs Query clients

**By design**, `TableClient` does not run SQL. It covers Table Service RPCs only: schema DDL, describe, `read_rows`, `bulk_upsert` (with automatic retries). All YQL goes through `QueryClient`.

## Key modules

| Module | Responsibility |
|--------|----------------|
| `client_builder.rs` | Connection string, credentials, discovery, optional `with_retry_budget` |
| `client.rs` | `Client`, `clone_with_retry_budget`, `retry_metrics`, factory methods |
| `retry_budget.rs` | `RetryBudget` trait, `LimitedRetryBudget`, `PercentOfRpsRetryBudget`, `PercentRetryBudget` |
| `client_query/` | Query Service: one-shot exec, `retry_tx`, scripts, transaction types |
| `client_table/` | Table Service: builders with per-call `.timeout()`, session-backed RPCs |
| `client_operation/` | Operation Service: get/list/forget/cancel with retries |
| `session_pool/` | Shared session pool for table + query (`SessionPool`, `TableSessionPool`) |
| `grpc_connection_manager.rs` | Auth + discovery interceptors, channel lookup |
| `connection_pool.rs` | gRPC channel lifecycle per endpoint |
| `load_balancer/` | `RandomBalancer`, `StaticBalancer`, `NearestDcBalancer` |
| `grpc_wrapper/` | Thin wrappers around tonic services |
| `errors.rs` | `YdbError`, status mapping, `NeedRetry` classification |
| `types.rs` | YDB value types, conversions |

## Retry and timeout patterns

### Per-call builders (replaces `clone_with_*`)

Client-level `clone_with_timeout`, `clone_with_retry`, `clone_with_idempotent`, and per-call `.retry_budget()` were **removed** in 0.16.0. Override defaults on each operation builder:

```rust
client.query_client()
    .retry_tx(async |tx| { /* ... */ })
    .timeout(Duration::from_secs(30))
    .idempotent(true)
    .await?;
```

### Per-call `.timeout()` (wall-clock deadline)

- Table: `table_client.read_rows(...).timeout(d).await`
- Query one-shot: `query_client.exec("...").timeout(d).await`
- Operation: `operation_client.get_operation(id).timeout(d).await`

**Semantics:**

| Condition | Behavior |
|-----------|----------|
| No `.timeout()` | Retry until a non-retryable error |
| `.idempotent(true)` | Also retry `NeedRetry::IdempotentOnly` |
| `.timeout(d)` | Wall-clock budget for the whole call (attempts + backoff + budget wait) |

No `max_retries` — deadline-based only (see `AGENTS.md`).

### `retry_tx` (Query Service interactive transactions)

`QueryClient::retry_tx(callback)` returns a builder (requires Rust 1.85+, `AsyncFnMut`):

```rust
client.query_client()
    .retry_tx(async |tx| { /* tx.exec / tx.query_row */ })
    .isolation(TxMode::SerializableReadWrite)
    .with_begin()           // optional explicit BeginTransaction RPC
    .idempotent(true)       // outer retry loop
    .timeout(Duration::from_secs(30))
    .await?;
```

`.timeout(d)` sets an absolute deadline propagated to every RPC inside the callback; per-call `.timeout()` inside the callback uses `min(call, remaining)`.

Implementation: `client_query/retry_tx.rs`, `client_query/mod.rs` (`run_retry_tx`), `client_query/exec.rs` (`retry_until`).

### Driver-wide retry budget (rate limiter)

**Not a timeout** — limits how many retries per second the driver may attempt cluster-wide, to avoid retry storms when YDB is unhealthy.

One `RetryBudget` per `Client` (default: unlimited, internal `UnlimitedRetryBudget`).

- Set at build: `ClientBuilder::with_retry_budget(budget)`
- Or child driver: `client.clone_with_retry_budget(budget)` — shares pool/connections, new budget
- `RetryBudget::acquire(deadline)` is called on the **second and each subsequent** retry attempt (after backoff)
- When exhausted: wait for quota or until call deadline
- Applies to: table, query one-shot, `retry_tx`, operation client retry loops

Built-in: `LimitedRetryBudget` (N retries/sec), `PercentOfRpsRetryBudget` (% of driver RPS via `RetryMetrics`), `PercentRetryBudget` (probabilistic, go-sdk parity). Custom implementations via `RetryBudget` trait + `async_trait`.

Topic reader/writer reconnectors use separate `Retry` in their options — not the driver retry budget.

### Table retries

`client_table/call_options.rs` — `retry_table_operation` uses `Retry` trait (`IndefiniteRetrier` / `TimeoutRetrier`) plus driver budget acquire after backoff.

### Query retries

`client_query/exec.rs` — `run_with_retry` / `retry_until` for one-shot calls; shared `pause_before_retry` in `retry_budget.rs`.

## Recurring patterns

### `grpc_wrapper` naming

Raw service clients live under `grpc_wrapper/raw_*` (e.g. `raw_table_service`, `raw_query_service`). Public clients compose these with pool + interceptors.

### Integration tests

Files like `client_table_test_integration.rs`, `client_query/integration_test.rs` use `#[ignore]` and `YDB_CONNECTION_STRING`.

### Builder pattern

`ClientBuilder`, per-call operation builders (`.timeout()`), topic reader/writer options, `retry_tx` builder, and several config types use builders or `derive_builder`.

## Adding a new API

1. Confirm protobuf support exists in `ydb-grpc` (regenerate if needed).
2. Add `grpc_wrapper/raw_*` client methods.
3. Expose through a `client_*` module with retries, per-call options, and error mapping.
4. Wire retry loops through `pause_before_retry` / `acquire_retry_budget` if they retry transient errors.
5. Re-export stable types from `lib.rs`.
6. Add unit tests; add `#[ignore]` integration test if server interaction is required.

## Anti-patterns

- Leaking `ydb-grpc` types in the public `ydb` API without a stable wrapper.
- Bypassing the connection pool for production RPC paths.
- Per-client `clone_with_timeout` / `clone_with_retry` — use builders instead.
- Adding dependencies without workspace-level version alignment.
