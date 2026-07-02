use super::TransactionOptions;
use crate::errors::{YdbError, YdbOrCustomerError, YdbResult};
use crate::test_integration_helper::create_client;
use crate::TxMode;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing_test::traced_test;

fn unique_table_name(prefix: &str) -> String {
    format!(
        "{prefix}_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before UNIX epoch")
            .as_nanos()
    )
}

fn customer_err(err: YdbOrCustomerError) -> YdbError {
    match err {
        YdbOrCustomerError::YDB(e) => e,
        YdbOrCustomerError::Customer(e) => YdbError::Custom(e.to_string()),
    }
}

fn is_snapshot_rw_unsupported(err: &impl std::fmt::Display) -> bool {
    err.to_string().contains("Snapshot Isolation")
}

macro_rules! client_mode_select {
    ($name:ident, $mode:expr) => {
        #[tokio::test]
        #[traced_test]
        #[ignore] // need YDB access
        async fn $name() -> YdbResult<()> {
            let client = create_client().await?;
            let mut qc = client.query_client().clone_with_idempotent_operations(true);

            let mut row = qc.query_row("SELECT 42 AS v").with_tx_mode($mode).await?;
            let v: i64 = row.remove_field_by_name("v")?.try_into()?;
            assert_eq!(v, 42);
            Ok(())
        }
    };
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_client_implicit_tx_select() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client.query_client().clone_with_idempotent_operations(true);

    let mut row = qc.query_row("SELECT 42 AS v").await?;
    let v: i64 = row.remove_field_by_name("v")?.try_into()?;
    assert_eq!(v, 42);
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_client_implicit_tx_ddl_and_dml() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client.query_client().clone_with_idempotent_operations(true);
    let table_name = unique_table_name("implicit_tx");

    let _ = qc.exec(format!("DROP TABLE IF EXISTS {table_name}")).await;
    qc.exec(format!(
        "CREATE TABLE {table_name} (id Int64, val Int64, PRIMARY KEY(id))"
    ))
    .await?;
    qc.exec(format!(
        "UPSERT INTO {table_name} (id, val) VALUES ($id, $val)"
    ))
    .param("$id", 1_i64)
    .param("$val", 7_i64)
    .await?;

    let mut row = qc
        .query_row(format!("SELECT val FROM {table_name} WHERE id = 1"))
        .await?;
    let val: Option<i64> = row.remove_field_by_name("val")?.try_into()?;
    assert_eq!(val, Some(7));

    qc.exec(format!("DROP TABLE {table_name}")).await?;
    Ok(())
}

client_mode_select!(
    query_client_serializable_rw_one_shot,
    TxMode::SerializableReadWrite
);
client_mode_select!(query_client_snapshot_ro_one_shot, TxMode::SnapshotReadOnly);
client_mode_select!(query_client_stale_ro_one_shot, TxMode::StaleReadOnly);
client_mode_select!(query_client_online_ro_one_shot, TxMode::OnlineReadOnly);

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_client_snapshot_rw_one_shot() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client.query_client().clone_with_idempotent_operations(true);

    match qc
        .query_row("SELECT 42 AS v")
        .with_tx_mode(TxMode::SnapshotReadWrite)
        .await
    {
        Ok(mut row) => {
            let v: i64 = row.remove_field_by_name("v")?.try_into()?;
            assert_eq!(v, 42);
        }
        Err(err) if is_snapshot_rw_unsupported(&err) => {
            eprintln!("SnapshotReadWrite not supported on this YDB cluster, skipping");
        }
        Err(err) => return Err(err),
    }
    Ok(())
}

macro_rules! interactive_mode_select {
    ($name:ident, $mode:expr) => {
        #[tokio::test]
        #[traced_test]
        #[ignore] // need YDB access
        async fn $name() -> YdbResult<()> {
            let client = create_client().await?;
            let qc = client
                .query_client()
                .clone_with_idempotent_operations(true)
                .clone_with_transaction_options(TransactionOptions::new().with_mode($mode));

            let v: i64 = qc
                .retry_transaction(async |tx| {
                    let mut row = tx.query_row("SELECT 42 AS v").await?;
                    Ok(row.remove_field_by_name("v")?.try_into()?)
                })
                .await?;
            assert_eq!(v, 42);
            Ok(())
        }
    };
}

interactive_mode_select!(query_tx_serializable_rw, TxMode::SerializableReadWrite);
interactive_mode_select!(query_tx_snapshot_ro, TxMode::SnapshotReadOnly);

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_tx_snapshot_rw() -> YdbResult<()> {
    let client = create_client().await?;
    let qc = client
        .query_client()
        .clone_with_idempotent_operations(true)
        .clone_with_transaction_options(
            TransactionOptions::new().with_mode(TxMode::SnapshotReadWrite),
        );

    match qc
        .retry_transaction(async |tx| {
            let mut row = tx.query_row("SELECT 42 AS v").await?;
            let v: i64 = row.remove_field_by_name("v")?.try_into()?;
            Ok(v)
        })
        .await
    {
        Ok(v) => assert_eq!(v, 42_i64),
        Err(err) if is_snapshot_rw_unsupported(&err) => {
            eprintln!("SnapshotReadWrite not supported on this YDB cluster, skipping");
        }
        Err(err) => return Err(customer_err(err)),
    }
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_tx_snapshot_rw_upsert() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client.query_client().clone_with_idempotent_operations(true);
    let table_name = unique_table_name("snapshot_rw_tx");

    let _ = qc.exec(format!("DROP TABLE IF EXISTS {table_name}")).await;
    qc.exec(format!(
        "CREATE TABLE {table_name} (id Int64, val Int64, PRIMARY KEY(id))"
    ))
    .await?;

    let mut qc = qc.clone_with_transaction_options(
        TransactionOptions::new().with_mode(TxMode::SnapshotReadWrite),
    );
    if let Err(err) = qc
        .retry_transaction(async |tx| {
            tx.exec(format!(
                "UPSERT INTO {table_name} (id, val) VALUES ($id, $val)"
            ))
            .param("$id", 1_i64)
            .param("$val", 55_i64)
            .await?;
            Ok(())
        })
        .await
    {
        if is_snapshot_rw_unsupported(&err) {
            eprintln!("SnapshotReadWrite not supported on this YDB cluster, skipping");
            qc.exec(format!("DROP TABLE {table_name}")).await?;
            return Ok(());
        }
        return Err(customer_err(err));
    }

    let mut row = qc
        .query_row(format!("SELECT val FROM {table_name} WHERE id = 1"))
        .await?;
    let val: Option<i64> = row.remove_field_by_name("val")?.try_into()?;
    assert_eq!(val, Some(55));

    qc.exec(format!("DROP TABLE {table_name}")).await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_tx_stale_ro_rejected_in_interactive() {
    let client = create_client().await.unwrap();
    let qc = client
        .query_client()
        .clone_with_idempotent_operations(true)
        .clone_with_transaction_options(TransactionOptions::new().with_mode(TxMode::StaleReadOnly));

    let err = qc
        .retry_transaction(async |tx| {
            tx.query_row("SELECT 1 AS v").await?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("not supported in interactive transactions"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_tx_implicit_rejected_in_interactive() {
    let client = create_client().await.unwrap();
    let qc = client
        .query_client()
        .clone_with_idempotent_operations(true)
        .clone_with_transaction_options(TransactionOptions::new().with_mode(TxMode::Implicit));

    let err = qc
        .retry_transaction(async |tx| {
            tx.query_row("SELECT 1 AS v").await?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("Implicit is not available inside Transaction"),
        "unexpected error: {err}"
    );
}
