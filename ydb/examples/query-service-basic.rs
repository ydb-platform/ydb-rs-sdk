//! Query Service API PROTOTYPE (issue #207): one-shot queries on
//! `QueryClient` — awaitable builders, params at the call site,
//! strict/optional/typed rows, reusable `Stmt` in a hot loop.
//!
//! Compiles to validate the API shape; running it fails at the first query —
//! execution is not implemented in the prototype.

use std::time::Duration;

use ydb::{ydb_params, ClientBuilder, FromYdbRow, QueryExecutor, Row, YdbResult};

#[derive(Debug)]
struct CounterRow {
    cnt: i64,
}

// Real SDK: #[derive(FromYdbRow)]
impl FromYdbRow for CounterRow {
    fn from_row(mut row: Row) -> YdbResult<Self> {
        Ok(Self {
            cnt: row.remove_field_by_name("cnt")?.try_into()?,
        })
    }
}

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .client()?;
    client.wait().await?;

    let mut qc = client.query_client();

    // 1. DDL / DML without result rows. One-shot client calls retry
    //    internally — no closure needed for a single statement.
    qc.exec("CREATE TABLE test (id Int64, val Utf8, PRIMARY KEY(id))")
        .await?;

    // 2. Parameters chain at the call site; no ExecuteOptions argument.
    //    `.params(ydb_params!(...))` works too, for migration from table API.
    qc.exec(
        "DECLARE $id AS Int64; DECLARE $val AS Utf8; \
         UPSERT INTO test (id, val) VALUES ($id, $val)",
    )
    .param("$id", 1_i64)
    .param("$val", "hello")
    .await?;

    qc.exec(
        "DECLARE $id AS Int64; DECLARE $val AS Utf8; \
         UPSERT INTO test (id, val) VALUES ($id, $val)",
    )
    .params(ydb_params!("$id" => 2_i64, "$val" => "world"))
    .await?;

    // 3. Exactly one row: 0 rows -> Err(NoRows), >1 -> error.
    let mut row = qc.query_row("SELECT COUNT(*) AS cnt FROM test").await?;
    let cnt: i64 = row.remove_field_by_name("cnt")?.try_into()?;
    println!("cnt = {cnt}");

    // 4. Optional row — the sqlx `fetch_optional` analogue.
    let found: Option<Row> = qc
        .query_row("DECLARE $id AS Int64; SELECT val FROM test WHERE id = $id")
        .param("$id", 42_i64)
        .optional()
        .await?;
    println!("found: {}", found.is_some());

    // 5. Typed row — the sqlx `query_as` analogue.
    let counter = qc
        .query_row("SELECT COUNT(*) AS cnt FROM test")
        .typed::<CounterRow>()
        .await?;
    println!("typed cnt = {}", counter.cnt);

    // 6. Per-call overrides, without touching client-level settings.
    qc.query_row("SELECT 1 AS one")
        .timeout(Duration::from_secs(5))
        .idempotent(true)
        .await?;

    // 7. Reuse in a hot loop: text AND big parameter values are borrowed —
    //    nothing is deep-copied per iteration until the gRPC request itself
    //    is encoded. Works the same for dynamic SQL built once outside.
    let table = "test";
    let dynamic_sql = format!(
        "DECLARE $id AS Int64; DECLARE $payload AS Utf8; \
         UPSERT INTO {table} (id, val) VALUES ($id, $payload)"
    );
    let big_payload = "x".repeat(1024 * 1024); // e.g. a large blob / bulk data
    for id in 0..100_i64 {
        qc.exec(&dynamic_sql)
            .param("$id", id) // small Copy value: owned, nothing to win
            .param("$payload", &big_payload) // borrowed: no 1 MB clone per call
            .await?;
    }

    Ok(())
}
