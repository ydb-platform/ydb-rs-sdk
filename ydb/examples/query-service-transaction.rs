//! `retry_transaction` with `AsyncFnMut(&mut Transaction)` on implicit sessions.

use std::time::Duration;

use ydb::{
    ClientBuilder, ExecBuilder, QueryExecutor, QueryRowBuilder, Transaction, YdbOrCustomerError,
    YdbResult, YdbResultWithCustomerErr,
};

const EXAMPLE_RETRY: Duration = Duration::from_secs(30);

fn idem_exec<'a>(b: ExecBuilder<'a>) -> ExecBuilder<'a> {
    b.idempotent(true).retry_budget(EXAMPLE_RETRY)
}

fn idem_row<'a>(b: QueryRowBuilder<'a>) -> QueryRowBuilder<'a> {
    b.idempotent(true).retry_budget(EXAMPLE_RETRY)
}

enum Withdraw {
    Done { remaining: i64 },
    Insufficient,
}

/// Generic over the executor via the `QueryExecutor` trait: works with both a
/// `QueryClient` and a `Transaction`. This is how an external library /
/// ORM adapter stays decoupled from the concrete type.
async fn fetch_total(e: &mut impl QueryExecutor) -> YdbResult<i64> {
    let mut row = e.query_row("SELECT SUM(id) AS s FROM test").await?;
    row.remove_field_by_name("s")?.try_into()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client =
        ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?.client()?;
    client.wait().await?;

    let mut qc = client.query_client();

    // --- 1. Borrowing the environment across attempts ----------------------
    // The query text lives outside the callback and is reused on every
    // attempt; the attempt counter is a plain `u32` captured mutably (the
    // table API needed AtomicUsize here).
    let upsert = "UPSERT INTO test (id, val) VALUES ($id, $val)";
    let mut attempts = 0_u32;

    let total: i64 = qc
        // Annotate `tx: &mut Transaction` so the IDE completes methods
        // on `tx`: rust-analyzer does not yet reliably infer `async ||`
        // closure parameter types from the `AsyncFnMut` bound (the compiler
        // infers it fine without this).
        .retry_transaction(async |tx: &mut Transaction| {
            attempts += 1; // mutable capture: AsyncFnMut allows it
            for id in 0..10_i64 {
                tx.exec(upsert)
                    .param("$id", id)
                    .param("$val", format!("val {id}"))
                    .await?;
            }
            let sum = fetch_total(tx).await?; // generic over QueryExecutor
            Ok(sum) // Ok => commit; Err => rollback + retry
        })
        .retry_budget(EXAMPLE_RETRY)
        .await?;
    println!("total = {total} after {attempts} attempt(s)");

    // --- 2. Rollback without an error ---------------------------------------
    // Requires `accounts` table; create minimal schema for the example.
    idem_exec(
        qc.exec("CREATE TABLE IF NOT EXISTS accounts (id Int64, balance Int64, PRIMARY KEY(id))"),
    )
    .await?;
    idem_exec(qc.exec("UPSERT INTO accounts (id, balance) VALUES (1, 500)")).await?;

    // A business outcome, not a failure: finish the transaction explicitly
    // and return a value. No commit, no retry, no Err.
    let outcome = qc
        .retry_transaction(async |tx: &mut Transaction| {
            let mut row = tx
                .query_row("SELECT balance FROM accounts WHERE id = $id")
                .param("$id", 1_i64)
                .await?;
            let balance: i64 = row.remove_field_by_name("balance")?.try_into()?;

            if balance < 100 {
                tx.rollback().await?;
                return Ok(Withdraw::Insufficient);
            }

            tx.exec("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
                .await?;
            Ok(Withdraw::Done {
                remaining: balance - 100,
            })
        })
        .retry_budget(EXAMPLE_RETRY)
        .await?;
    match outcome {
        Withdraw::Done { remaining } => println!("done, remaining = {remaining}"),
        Withdraw::Insufficient => println!("insufficient funds"),
    }

    // --- 3. Customer errors are never retried -------------------------------
    let res: YdbResultWithCustomerErr<()> = qc
        .retry_transaction(async |tx: &mut Transaction| {
            tx.exec("DELETE FROM test").await?;
            Err(YdbOrCustomerError::from_err(std::io::Error::other(
                "business rule violated",
            )))
        })
        .retry_budget(EXAMPLE_RETRY)
        .await;
    println!("customer error passed through: {}", res.is_err());

    // --- 5. Lazy tx vs explicit begin ---------------------------------------
    // Default (lazy): the first ExecuteQuery opens the transaction (BeginTx in tx_control).
    qc.retry_transaction(async |tx: &mut Transaction| {
        tx.exec("SELECT 1").await?;
        Ok(())
    })
    .retry_budget(EXAMPLE_RETRY)
    .await?;

    // Explicit BeginTransaction RPC before any YQL:
    qc.retry_transaction(async |tx: &mut Transaction| {
        tx.begin().await?;
        tx.exec("SELECT 1").await?;
        Ok(())
    })
    .retry_budget(EXAMPLE_RETRY)
    .await?;

    // Or configure explicit begin per retry_transaction call:
    qc.retry_transaction(async |tx: &mut Transaction| {
        tx.exec("SELECT 1").await?; // BeginTransaction RPC runs before this query
        Ok(())
    })
    .with_begin()
    .retry_budget(EXAMPLE_RETRY)
    .await?;

    // --- 6. Commit with the last query (with_commit) ------------------------
    let table = "query_example_with_commit";
    idem_exec(
        qc.exec(format!(
            "CREATE TABLE IF NOT EXISTS {table} (id Int64, val Int64, PRIMARY KEY(id))"
        )),
    )
    .await?;

    qc.retry_transaction(async |tx: &mut Transaction| {
        tx.exec(format!("UPSERT INTO {table} (id, val) VALUES ($id, $val)"))
            .param("$id", 1_i64)
            .param("$val", 100_i64)
            .with_commit(true) // server commits when the stream is fully read
            .await?;
        // Transaction is already committed; further queries in this callback would fail.
        Ok(())
    })
    .retry_budget(EXAMPLE_RETRY)
    .await?; // retry_transaction commit is a no-op

    let mut row = idem_row(qc.query_row(format!("SELECT val FROM {table} WHERE id = 1"))).await?;
    let val: Option<i64> = row.remove_field_by_name("val")?.try_into()?;
    println!("with_commit persisted val = {:?}", val);

    // --- 7. What does NOT compile (by design) -------------------------------
    //
    // a) Smuggling a stream (or the tx itself) out of the attempt — the
    //    stream borrows the transaction and cannot outlive the callback:
    //
    //     let stream = qc
    //         .retry_transaction(async |tx| Ok(tx.query("SELECT 1").await?))
    //         .await?;
    //     // error: lifetime may not live long enough
    //
    // b) Moving a captured value into the attempt (would break attempt #2):
    //
    //     let sql = String::from("SELECT 1");
    //     qc.retry_transaction(async |tx| {
    //         tx.exec(sql).await?; // error[E0507]: cannot move out of `sql`,
    //         Ok(())               // which is behind a mutable reference
    //     })
    //     .await?;
    //
    // c) CAUTION — compiles, but is a logic hazard (same as in the Go SDK):
    //    pushing into a captured collection survives failed attempts and
    //    duplicates data on retry. Accumulate inside the callback and return
    //    the collection via Ok(...) instead.

    Ok(())
}
