//! Query Service examples — implicit sessions (default), awaitable builders.

use std::time::Duration;

use ydb::{ydb_params, ClientBuilder, FromYdbRow, Row, YdbResult};

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
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?
        .client()?;
    client.wait().await?;

    let mut qc = client.query_client();

    // 1. QueryClient one-shots default to ImplicitTx (no explicit tx_control): the server
    //    picks isolation from the SQL (DDL — non-transactional, SELECT — snapshot RO, DML — serializable RW).
    //    One-shot calls retry internally — no closure needed for a single statement.
    qc.exec("CREATE TABLE test (id Int64, val Utf8, PRIMARY KEY(id))")
        .await?;

    // 2. Parameters chain at the call site; no ExecuteOptions argument.
    //    `.params(ydb_params!(...))` works too, for migration from table API.
    qc.exec("UPSERT INTO test (id, val) VALUES ($id, $val)")
        .param("$id", 1_i64)
        .param("$val", "hello")
        .await?;

    qc.exec("UPSERT INTO test (id, val) VALUES ($id, $val)")
        .params(ydb_params!("$id" => 2_i64, "$val" => "world"))
        .await?;

    // 3. Exactly one row: 0 rows -> Err(NoRows), >1 -> error.
    let mut row = qc.query_row("SELECT COUNT(*) AS cnt FROM test").await?;
    let cnt: i64 = row.remove_field_by_name("cnt")?.try_into()?;
    println!("cnt = {cnt}");

    // 4. Optional row — the sqlx `fetch_optional` analogue.
    let found: Option<Row> = qc
        .query_row("SELECT val FROM test WHERE id = $id")
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

    // 7. Hot loop with dynamic SQL. The builder is consumed by `.await`, so
    //    an owned value passed by value is moved in, not copied. Pass
    //    `value.clone()` only if you need to keep the value after the call.
    //
    //    Table/identifier names cannot be YQL parameters — validate any user-
    //    controlled identifier before interpolating into SQL (literal is safe).
    let table = "test";
    let dynamic_sql = format!("UPSERT INTO {table} (id, val) VALUES ($id, $payload)");
    for id in 0..100_i64 {
        let payload = format!("payload for row {id}");
        qc.exec(dynamic_sql.clone())
            .param("$id", id)
            .param("$payload", payload) // moved in, no extra copy
            .await?;
    }

    Ok(())
}
