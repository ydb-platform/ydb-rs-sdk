# Rust YDB SDK
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ydb-platform/ydb/blob/main/LICENSE)
[![Latest Version](https://img.shields.io/crates/v/ydb.svg)](https://crates.io/crates/ydb)
[![Released API docs](https://docs.rs/ydb/badge.svg)](https://docs.rs/ydb)
[![Linter](https://github.com/ydb-platform/ydb-rs-sdk/workflows/Linter/badge.svg)](https://github.com/ydb-platform/ydb-rs-sdk/actions/workflows/linter.yml)
[![YDB tests](https://github.com/ydb-platform/ydb-rs-sdk/actions/workflows/rust-tests.yml/badge.svg?branch=master&event=schedule)](https://github.com/ydb-platform/ydb-rs-sdk/actions/workflows/rust-tests.yml)
[![codecov](https://codecov.io/gh/ydb-platform/ydb-rs-sdk/badge.svg?precision=2)](https://app.codecov.io/gh/ydb-platform/ydb-rs-sdk)
[![View examples](https://img.shields.io/badge/learn-examples-brightgreen.svg)](https://github.com/ydb-platform/ydb-rs-sdk/tree/master/ydb/examples)
[![Telegram](https://img.shields.io/badge/chat-on%20Telegram-2ba2d9.svg)](https://t.me/ydb_en)
[![WebSite](https://img.shields.io/badge/website-ydb.tech-blue.svg)](https://ydb.tech)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/CONTRIBUTING.md)

Rust SDK for YDB.

### Prerequisites
Rust 1.88.0 or newer

CI checks compatibility on Rust 1.88 and Rust 1.96. Linting, publishing, and SLO workload builds use Rust 1.96.

### Installation
Add the YDB dependency to your project using `cargo add ydb` or add this your Cargo.toml:
```toml
[dependencies]
ydb = "0.18.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

### Example
Create a new Rust file (e.g., main.rs) and add the following code:

```rust
use ydb::{ClientBuilder, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?
        .build()
        .await?;

    let mut qc = client.query_client();

    // one-shot: retries internally, no closure for a single statement
    let mut row = qc.query_row("SELECT 1 + 1 AS sum").await?;
    let sum: i32 = row.remove_field_by_name("sum")?.try_into()?;

    println!("sum: {sum}");
    Ok(())
}
```

For more examples, see [ydb/examples](https://github.com/ydb-platform/ydb-rs-sdk/tree/master/ydb/examples).

### Strict serializable writes

Select `TxMode::StrictSerializableRW` on a one-shot Query call or on `retry_tx` to use Query
Service's strict serializable read-write mode. The server must have strict serializable isolation
enabled; otherwise it rejects the request. For a successful write, the server may return a
`VirtualTimestamp` with unsigned `plan_step` and `tx_id` components:

```rust,no_run
# use ydb::{ClientBuilder, TxMode, YdbResult};
# #[tokio::main]
# async fn main() -> YdbResult<()> {
# let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?.build().await?;
let mut query = client.query_client();
let timestamp = query
    .exec("UPSERT INTO test (id) VALUES (1)")
    .with_tx_mode(TxMode::StrictSerializableRW)
    .execute_with_commit_timestamp()
    .await?;
if let Some(timestamp) = timestamp {
    println!("{}:{}", timestamp.plan_step(), timestamp.tx_id());
}
# Ok(())
# }
```

For a streaming call, drain all result sets and use `QueryStream::close_with_commit_timestamp()`.
For an interactive transaction, use `Transaction::commit_with_timestamp()` for an explicit commit,
or `with_commit(true)` on its last query and close the resulting stream. Existing `exec().await`,
`QueryStream::close()`, and automatic `retry_tx` commits retain their previous return types.

The timestamp is optional. The server sends it only on `SUCCESS`, in this mode, and when the
transaction had write effects. `VirtualTimestamp::compare()` orders values by `plan_step` and
then `tx_id`; it rejects values produced by separately built drivers. The SDK has the database
path but cannot prove that two separate drivers address the same database.

### QueryClient one-shot methods

For a single YQL statement you usually do not need `retry_tx` — call a builder and `.await?`:

| Method | Returns | Use for |
|--------|---------|---------|
| [`exec`](https://docs.rs/ydb/latest/ydb/struct.QueryClient.html) | `()` | DDL, DML without rows (`CREATE TABLE`, `UPSERT`, `DELETE`) |
| [`query_row`](https://docs.rs/ydb/latest/ydb/struct.QueryClient.html) | one [`Row`](https://docs.rs/ydb/latest/ydb/struct.Row.html) | exactly one row (`SELECT COUNT(*) …`) |
| [`query_result_set`](https://docs.rs/ydb/latest/ydb/struct.QueryClient.html) | one [`ResultSet`](https://docs.rs/ydb/latest/ydb/struct.ResultSet.html) | all rows of one result set |
| [`query`](https://docs.rs/ydb/latest/ydb/struct.QueryClient.html) | [`QueryStream`](https://docs.rs/ydb/latest/ydb/struct.QueryStream.html) | multiple result sets, large reads |

Parameters chain at the call site:

```rust
use ydb::{ydb_params, ClientBuilder, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?
        .build()
        .await?;

    let mut qc = client.query_client();

    qc.exec("CREATE TABLE IF NOT EXISTS test (id Int64, val Utf8, PRIMARY KEY(id))")
        .await?;

    qc.exec(
        "UPSERT INTO test (id, val) VALUES ($id, $val)",
    )
    .param("$id", 1_i64)
    .param("$val", "hello")
    .await?;

    // or: .params(ydb_params!("$id" => 2_i64, "$val" => "world"))

    let mut row = qc.query_row("SELECT COUNT(*) AS cnt FROM test").await?;
    let cnt: i64 = row.remove_field_by_name("cnt")?.try_into()?;
    println!("cnt = {cnt}");

    Ok(())
}
```

Use `.optional()` when zero rows is OK, `.typed::<T>()` to map a row into your struct (see [`query-service-basic`](ydb/examples/query-service-basic.rs)).

For multi-statement atomic work, use [`QueryClient::retry_tx`](https://docs.rs/ydb/latest/ydb/struct.QueryClient.html) with `async |tx: &mut Transaction| { … }` (see [`query-service-transaction`](ydb/examples/query-service-transaction.rs)).

### Try QueryClient locally

**New project**

1. Start local YDB from the repository root: `docker compose up -d`
2. Add `ydb` and `tokio` to `Cargo.toml` (see [Installation](#installation)).
3. Copy the [Example](#example) or run the SDK example:
   ```bash
   cd ydb
   cargo run --example query-service-basic
   ```

**Migrating from `table_client`**

Replace `client.table_client()` with `client.query_client()` and simplify call sites:

| Table API | Query API |
|-----------|-----------|
| `execute_scheme_query(sql)` | `qc.exec(sql).await?` |
| `retry_tx` + one `t.query(...)` | one-shot: `qc.exec(...)` / `qc.query_row(...)` / `qc.query_result_set(...)` |
| `retry_tx` + several statements | `qc.retry_tx` + `tx.exec(...)` (see example below) |
| `Query::from(sql).with_params(...)` | `qc.exec(sql).params(ydb_params!(...)).await?` or `.param("$name", value)` |
| `res.into_only_row()?` | `qc.query_row(sql).await?` |
| `res.into_only_result()?.rows()` | `qc.query_result_set(sql).await?` |

Notes:
- One-shot calls use implicit sessions and server-side transaction mode by default (DDL — non-transactional, `SELECT` — snapshot read-only, DML — serializable read-write).
- `table_client` remains available for legacy code; new code should prefer `query_client`.
- Full before/after: compare [`basic-select-upsert.rs`](ydb/examples/basic-select-upsert.rs) (table) with [`query-service-basic.rs`](ydb/examples/query-service-basic.rs) (query).

## Tests

Integration tests, with dependency from real YDB database marked as ignored.
To run it:
1. Set YDB_CONNECTION_STRING env
2. run cargo test -- --include-ignored

# Version policy

Crates follow to semver 2.0 https://semver.org/spec/v2.0.0.html.
For version 0.X.Y: X increments for expected backwards incompatible changes, Y increments for any compatible changes (fixes, extend api without broke compatible).
For incompatible changes creates github release with describe incompatibles.
