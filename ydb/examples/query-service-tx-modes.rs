#![recursion_limit = "256"]
//! Query Service transaction isolation modes (ImplicitTx and explicit BeginTx modes).

use std::time::Duration;

use ydb::{
    ClientBuilder, ExecBuilder, QueryRowBuilder, TxMode, YdbError, YdbOrCustomerError, YdbResult,
};

const EXAMPLE_TIMEOUT: Duration = Duration::from_secs(30);

fn idem_exec<'a>(b: ExecBuilder<'a>) -> ExecBuilder<'a> {
    b.idempotent(true).timeout(EXAMPLE_TIMEOUT)
}

fn idem_row<'a>(b: QueryRowBuilder<'a>) -> QueryRowBuilder<'a> {
    b.idempotent(true).timeout(EXAMPLE_TIMEOUT)
}

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client =
        ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?.client()?;
    client.wait().await?;

    let mut qc = client.query_client();

    // --- ImplicitTx (default): server infers isolation from SQL ----------------
    // SELECT → snapshot read-only; DML → serializable read-write; DDL → non-transactional.
    let mut row = idem_row(qc.query_row("SELECT 1 AS one")).await?;
    let one: i64 = row.remove_field_by_name("one")?.try_into()?;
    println!("implicit SELECT → {one}");

    idem_exec(
        qc.exec("CREATE TABLE IF NOT EXISTS tx_modes_demo (id Int64, val Int64, PRIMARY KEY(id))"),
    )
    .await?;

    // --- One-shot explicit modes -----------------------------------------------
    for (label, mode) in [
        ("SerializableRW", TxMode::SerializableReadWrite),
        ("SnapshotRO", TxMode::SnapshotReadOnly),
        ("SnapshotRW", TxMode::SnapshotReadWrite),
        ("StaleRO", TxMode::StaleReadOnly),
        ("OnlineRO", TxMode::OnlineReadOnly),
        ("OnlineInconsistentRO", TxMode::OnlineReadOnlyInconsistent),
    ] {
        match idem_row(qc.query_row("SELECT 42 AS v").with_tx_mode(mode)).await {
            Ok(mut row) => {
                let v: i64 = row.remove_field_by_name("v")?.try_into()?;
                println!("one-shot {label} SELECT → {v}");
            }
            Err(err)
                if mode == TxMode::SnapshotReadWrite
                    && err.to_string().contains("Snapshot Isolation") =>
            {
                println!("one-shot {label} not supported on this cluster, skipped");
            }
            Err(err) => return Err(err),
        }
    }

    // --- Interactive modes (SerializableRW, SnapshotRO, SnapshotRW) ------------
    for (label, mode) in [
        ("SerializableRW", TxMode::SerializableReadWrite),
        ("SnapshotRO", TxMode::SnapshotReadOnly),
        ("SnapshotRW", TxMode::SnapshotReadWrite),
    ] {
        let v: i64 = match qc
            .retry_tx(async |tx| {
                let mut row = tx.query_row("SELECT 42 AS v").await?;
                Ok(row.remove_field_by_name("v")?.try_into()?)
            })
            .isolation(mode)
            .timeout(EXAMPLE_TIMEOUT)
            .await
        {
            Ok(v) => v,
            Err(err)
                if mode == TxMode::SnapshotReadWrite
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

    // StaleRO / OnlineRO / ImplicitTx are one-shot only — not valid on Transaction.
    Ok(())
}
