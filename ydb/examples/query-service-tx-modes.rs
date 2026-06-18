//! Query Service transaction isolation modes (ImplicitTx and explicit BeginTx modes).

use ydb::{
    ClientBuilder, QueryTransactionOptions, QueryTxMode, YdbError, YdbOrCustomerError, YdbResult,
};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client =
        ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?.client()?;
    client.wait().await?;

    let mut qc = client.query_client().clone_with_idempotent_operations(true);

    // --- ImplicitTx (default): server infers isolation from SQL ----------------
    // SELECT → snapshot read-only; DML → serializable read-write; DDL → non-transactional.
    let mut row = qc.query_row("SELECT 1 AS one").await?;
    let one: i64 = row.remove_field_by_name("one")?.try_into()?;
    println!("implicit SELECT → {one}");

    qc.exec("CREATE TABLE IF NOT EXISTS tx_modes_demo (id Int64, val Int64, PRIMARY KEY(id))")
        .await?;

    // --- One-shot explicit modes -----------------------------------------------
    for (label, mode) in [
        ("SerializableRW", QueryTxMode::SerializableReadWrite),
        ("SnapshotRO", QueryTxMode::SnapshotReadOnly),
        ("SnapshotRW", QueryTxMode::SnapshotReadWrite),
        ("StaleRO", QueryTxMode::StaleReadOnly),
        ("OnlineRO", QueryTxMode::OnlineReadOnly),
    ] {
        match qc.query_row("SELECT 42 AS v").with_tx_mode(mode).await {
            Ok(mut row) => {
                let v: i64 = row.remove_field_by_name("v")?.try_into()?;
                println!("one-shot {label} SELECT → {v}");
            }
            Err(err)
                if mode == QueryTxMode::SnapshotReadWrite
                    && err.to_string().contains("Snapshot Isolation") =>
            {
                println!("one-shot {label} not supported on this cluster, skipped");
            }
            Err(err) => return Err(err),
        }
    }

    // --- Interactive modes (SerializableRW, SnapshotRO, SnapshotRW) ------------
    for (label, mode) in [
        ("SerializableRW", QueryTxMode::SerializableReadWrite),
        ("SnapshotRO", QueryTxMode::SnapshotReadOnly),
        ("SnapshotRW", QueryTxMode::SnapshotReadWrite),
    ] {
        let with_mode =
            qc.clone_with_transaction_options(QueryTransactionOptions::new().with_mode(mode));
        let v: i64 = match with_mode
            .retry_transaction(async |tx| {
                let mut row = tx.query_row("SELECT 42 AS v").await?;
                Ok(row.remove_field_by_name("v")?.try_into()?)
            })
            .await
        {
            Ok(v) => v,
            Err(err)
                if mode == QueryTxMode::SnapshotReadWrite
                    && err.to_string().contains("Snapshot Isolation") =>
            {
                println!("interactive {label} not supported on this cluster, skipped");
                continue;
            }
            Err(err) => {
                return Err(match err {
                    YdbOrCustomerError::YDB(e) => e,
                    YdbOrCustomerError::Customer(e) => YdbError::Custom(e.to_string()),
                });
            }
        };
        println!("interactive {label} SELECT → {v}");
    }

    // StaleRO / OnlineRO / ImplicitTx are one-shot only — not valid on QueryTransaction.
    Ok(())
}
