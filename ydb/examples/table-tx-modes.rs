//! Table Service transaction isolation modes (autocommit one-shot via [`TransactionOptions`]).

use ydb::{
    ClientBuilder, Mode, Query, TransactionOptions, YdbError, YdbOrCustomerError, YdbResult,
};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client =
        ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?.client()?;
    client.wait().await?;

    let table_client = client.table_client();

    for (label, mode) in [
        ("SerializableRW", Mode::SerializableReadWrite),
        ("SnapshotRO", Mode::SnapshotReadOnly),
        ("SnapshotRW", Mode::SnapshotReadWrite),
        ("StaleRO", Mode::StaleReadOnly),
        ("OnlineRO", Mode::OnlineReadonly),
        ("OnlineInconsistentRO", Mode::OnlineReadonlyInconsistent),
    ] {
        let client = table_client.clone_with_transaction_options(
            TransactionOptions::new().with_mode(mode).with_autocommit(true),
        );
        match client
            .retry_transaction(|mut t| async move {
                let result = t.query(Query::new("SELECT 42 AS v")).await?;
                Ok(result)
            })
            .await
        {
            Ok(result) => {
                let mut row = result.into_only_result()?.rows().next().unwrap();
                let v: i64 = row.remove_field_by_name("v")?.try_into()?;
                println!("autocommit {label} SELECT → {v}");
            }
            Err(err)
                if matches!(mode, Mode::SnapshotReadWrite)
                    && err.to_string().contains("Snapshot Isolation") =>
            {
                println!("autocommit {label} not supported on this cluster, skipped");
            }
            Err(err) => {
                return Err(match err {
                    YdbOrCustomerError::YDB(e) => e,
                    YdbOrCustomerError::Customer(e) => YdbError::Custom(e.to_string()),
                });
            }
        }
    }

    Ok(())
}
