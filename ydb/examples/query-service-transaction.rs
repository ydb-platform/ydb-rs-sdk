//! `retry_transaction` with `AsyncFnMut(&mut QueryTransaction)` on implicit sessions.

use ydb::{
    ClientBuilder, QueryExecutor, QueryTransaction, QueryTransactionOptions, YdbOrCustomerError,
    YdbResult, YdbResultWithCustomerErr,
};

enum Withdraw {
    Done { remaining: i64 },
    Insufficient,
}

/// Generic over the executor via the `QueryExecutor` trait: works with both a
/// `QueryClient` and a `QueryTransaction`. This is how an external library /
/// ORM adapter stays decoupled from the concrete type.
async fn fetch_total(e: &mut impl QueryExecutor) -> YdbResult<i64> {
    let mut row = e.query_row("SELECT SUM(id) AS s FROM test").await?;
    row.remove_field_by_name("s")?.try_into()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .client()?;
    client.wait().await?;

    let mut qc = client.query_client().clone_with_idempotent_operations(true);

    // --- 1. Borrowing the environment across attempts ----------------------
    // The query text lives outside the callback and is reused on every
    // attempt; the attempt counter is a plain `u32` captured mutably (the
    // table API needed AtomicUsize here).
    let upsert = "DECLARE $id AS Int64; DECLARE $val AS Utf8; \
                  UPSERT INTO test (id, val) VALUES ($id, $val)";
    let mut attempts = 0_u32;

    let total: i64 = qc
        // Annotate `tx: &mut QueryTransaction` so the IDE completes methods
        // on `tx`: rust-analyzer does not yet reliably infer `async ||`
        // closure parameter types from the `AsyncFnMut` bound (the compiler
        // infers it fine without this).
        .retry_transaction(async |tx: &mut QueryTransaction| {
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
        .await?;
    println!("total = {total} after {attempts} attempt(s)");

    // --- 2. Rollback without an error ---------------------------------------
    // Requires `accounts` table; create minimal schema for the example.
    qc.exec("CREATE TABLE IF NOT EXISTS accounts (id Int64, balance Int64, PRIMARY KEY(id))")
        .await?;
    qc.exec("UPSERT INTO accounts (id, balance) VALUES (1, 500)")
        .await?;

    // A business outcome, not a failure: finish the transaction explicitly
    // and return a value. No commit, no retry, no Err.
    let outcome = qc
        .retry_transaction(async |tx: &mut QueryTransaction| {
            let mut row = tx
                .query_row("DECLARE $id AS Int64; SELECT balance FROM accounts WHERE id = $id")
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
        .await?;
    match outcome {
        Withdraw::Done { remaining } => println!("done, remaining = {remaining}"),
        Withdraw::Insufficient => println!("insufficient funds"),
    }

    // --- 3. Customer errors are never retried -------------------------------
    let res: YdbResultWithCustomerErr<()> = qc
        .retry_transaction(async |tx: &mut QueryTransaction| {
            tx.exec("DELETE FROM test").await?;
            Err(YdbOrCustomerError::from_err(std::io::Error::other(
                "business rule violated",
            )))
        })
        .await;
    println!("customer error passed through: {}", res.is_err());

    // --- 5. Lazy tx vs explicit begin ---------------------------------------
    // Default: tx_id appears only after the first ExecuteQuery (BeginTx in tx_control).
    qc.retry_transaction(async |tx: &mut QueryTransaction| {
        assert!(tx.tx_id_for_test().is_none());
        tx.exec("SELECT 1").await?;
        assert!(tx.tx_id_for_test().is_some());
        Ok(())
    })
    .await?;

    // Explicit BeginTransaction RPC before any YQL:
    qc.retry_transaction(async |tx: &mut QueryTransaction| {
        tx.begin().await?;
        assert!(tx.tx_id_for_test().is_some());
        tx.exec("SELECT 1").await?;
        Ok(())
    })
    .await?;

    // Or configure eager begin on the client for every retry_transaction:
    let eager_qc = qc.clone_with_transaction_options(QueryTransactionOptions::new().with_eager_begin());
    eager_qc
        .retry_transaction(async |tx: &mut QueryTransaction| {
            tx.exec("SELECT 1").await?; // BeginTransaction RPC runs first
            assert!(tx.tx_id_for_test().is_some());
            Ok(())
        })
        .await?;

    // --- 6. Commit with the last query (with_commit) ------------------------
    let table = "query_example_with_commit";
    qc.exec(format!(
        "CREATE TABLE IF NOT EXISTS {table} (id Int64, val Int64, PRIMARY KEY(id))"
    ))
    .await?;

    qc.retry_transaction(async |tx: &mut QueryTransaction| {
        tx.exec(format!(
            "DECLARE $id AS Int64; DECLARE $val AS Int64; \
             UPSERT INTO {table} (id, val) VALUES ($id, $val)"
        ))
        .param("$id", 1_i64)
        .param("$val", 100_i64)
        .with_commit() // server commits when the stream is fully read
        .await?;
        // Transaction is already committed; further queries in this callback would fail.
        Ok(())
    })
    .await?; // retry_transaction commit is a no-op

    let mut row = qc
        .query_row(format!("SELECT val FROM {table} WHERE id = 1"))
        .await?;
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
