use crate::closure;
use crate::errors::{YdbError, YdbOrCustomerError, YdbResult};
use crate::test_integration_helper::create_client;
use crate::{Transaction, TxMode};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing_test::traced_test;
use ydb_grpc::ydb_proto::status_ids::StatusCode;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

macro_rules! idem {
    ($builder:expr_2021) => {
        $builder.idempotent(true).timeout(TEST_TIMEOUT)
    };
}

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

fn is_strict_serializable_disabled(err: &YdbError) -> bool {
    // The server reports this feature flag as BAD_REQUEST with issue code 0, so the issue text
    // is the only way for this integration test to distinguish it from a real query failure.
    matches!(err, YdbError::YdbStatusError(status)
        if status.operation_status == StatusCode::BadRequest as i32
            && status.issues.iter().any(|issue| issue.message.contains("Strict Serializable mode is disabled")))
}

macro_rules! client_mode_select {
    ($name:ident, $mode:expr_2021) => {
        #[tokio::test]
        #[traced_test]
        #[ignore] // need YDB access
        async fn $name() -> YdbResult<()> {
            let client = create_client().await?;
            let mut qc = client.query_client();

            let mut row = idem!(qc.query_row("SELECT 42 AS v").with_tx_mode($mode)).await?;
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
    let mut qc = client.query_client();

    let mut row = idem!(qc.query_row("SELECT 42 AS v")).await?;
    let v: i64 = row.remove_field_by_name("v")?.try_into()?;
    assert_eq!(v, 42);
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_client_implicit_tx_ddl_and_dml() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client.query_client();
    let table_name = unique_table_name("implicit_tx");

    let _ = idem!(qc.exec(format!("DROP TABLE IF EXISTS {table_name}"))).await;
    idem!(qc.exec(format!(
        "CREATE TABLE {table_name} (id Int64, val Int64, PRIMARY KEY(id))"
    )))
    .await?;
    idem!(
        qc.exec(format!(
            "UPSERT INTO {table_name} (id, val) VALUES ($id, $val)"
        ))
        .param("$id", 1_i64)
        .param("$val", 7_i64)
    )
    .await?;

    let mut row = idem!(qc.query_row(format!("SELECT val FROM {table_name} WHERE id = 1"))).await?;
    let val: Option<i64> = row.remove_field_by_name("val")?.try_into()?;
    assert_eq!(val, Some(7));

    idem!(qc.exec(format!("DROP TABLE {table_name}"))).await?;
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
async fn strict_serializable_commit_timestamps() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client.query_client();
    let table_name = unique_table_name("strict_serializable");
    idem!(qc.exec(format!("DROP TABLE IF EXISTS {table_name}"))).await?;
    idem!(qc.exec(format!(
        "CREATE TABLE {table_name} (id Int64, PRIMARY KEY(id))"
    )))
    .await?;

    let read_only = match idem!(
        qc.exec("SELECT 1")
            .with_tx_mode(TxMode::StrictSerializableRW)
    )
    .execute_with_commit_timestamp()
    .await
    {
        Ok(timestamp) => timestamp,
        Err(error) if is_strict_serializable_disabled(&error) => return Ok(()),
        Err(error) => return Err(error),
    };
    assert!(read_only.is_none());

    let timestamp = idem!(
        qc.exec(format!("UPSERT INTO {table_name} (id) VALUES (1)"))
            .with_tx_mode(TxMode::StrictSerializableRW)
    )
    .execute_with_commit_timestamp()
    .await?;
    assert!(timestamp.is_some());

    let mut stream = idem!(
        qc.query(format!("UPSERT INTO {table_name} (id) VALUES (2)"))
            .with_tx_mode(TxMode::StrictSerializableRW)
    )
    .await?;
    while stream.next_result_set().await?.is_some() {}
    let streamed = stream.close_with_commit_timestamp().await?;
    assert!(streamed.is_some());

    let explicit = qc
        .retry_tx(closure!([&table_name], async |tx: &mut Transaction| {
            tx.exec(format!("UPSERT INTO {table_name} (id) VALUES (3)"))
                .await?;
            Ok(tx.commit_with_timestamp().await?)
        }))
        .isolation(TxMode::StrictSerializableRW)
        .timeout(TEST_TIMEOUT)
        .await
        .map_err(customer_err)?;
    assert!(explicit.is_some());
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_client_snapshot_rw_one_shot() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client.query_client();

    match idem!(
        qc.query_row("SELECT 42 AS v")
            .with_tx_mode(TxMode::SnapshotReadWrite)
    )
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
    ($name:ident, $mode:expr_2021) => {
        #[tokio::test]
        #[traced_test]
        #[ignore] // need YDB access
        async fn $name() -> YdbResult<()> {
            let client = create_client().await?;
            let qc = client.query_client();

            let v: i64 = qc
                .retry_tx(closure!(async |tx: &mut Transaction| {
                    let mut row = tx.query_row("SELECT 42 AS v").await?;
                    Ok(row.remove_field_by_name("v")?.try_into()?)
                }))
                .isolation($mode)
                .timeout(TEST_TIMEOUT)
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
    let qc = client.query_client();

    match qc
        .retry_tx(closure!(async |tx: &mut Transaction| {
            let mut row = tx.query_row("SELECT 42 AS v").await?;
            let v: i64 = row.remove_field_by_name("v")?.try_into()?;
            Ok(v)
        }))
        .isolation(TxMode::SnapshotReadWrite)
        .timeout(TEST_TIMEOUT)
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
    let mut qc = client.query_client();
    let table_name = unique_table_name("snapshot_rw_tx");

    let _ = idem!(qc.exec(format!("DROP TABLE IF EXISTS {table_name}"))).await;
    idem!(qc.exec(format!(
        "CREATE TABLE {table_name} (id Int64, val Int64, PRIMARY KEY(id))"
    )))
    .await?;

    if let Err(err) = qc
        .retry_tx(closure!([&table_name], async |tx: &mut Transaction| {
            tx.exec(format!(
                "UPSERT INTO {table_name} (id, val) VALUES ($id, $val)"
            ))
            .param("$id", 1_i64)
            .param("$val", 55_i64)
            .await?;
            Ok(())
        }))
        .isolation(TxMode::SnapshotReadWrite)
        .timeout(TEST_TIMEOUT)
        .await
    {
        if is_snapshot_rw_unsupported(&err) {
            eprintln!("SnapshotReadWrite not supported on this YDB cluster, skipping");
            idem!(qc.exec(format!("DROP TABLE {table_name}"))).await?;
            return Ok(());
        }
        return Err(customer_err(err));
    }

    let mut row = idem!(qc.query_row(format!("SELECT val FROM {table_name} WHERE id = 1"))).await?;
    let val: Option<i64> = row.remove_field_by_name("val")?.try_into()?;
    assert_eq!(val, Some(55));

    idem!(qc.exec(format!("DROP TABLE {table_name}"))).await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_tx_stale_ro_rejected_in_interactive() {
    let client = create_client().await.unwrap();
    let qc = client.query_client();

    let err = qc
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.query_row("SELECT 1 AS v").await?;
            Ok(())
        }))
        .isolation(TxMode::StaleReadOnly)
        .timeout(TEST_TIMEOUT)
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
    let qc = client.query_client();

    let err = qc
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.query_row("SELECT 1 AS v").await?;
            Ok(())
        }))
        .isolation(TxMode::Implicit)
        .timeout(TEST_TIMEOUT)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("Implicit is not available inside Transaction"),
        "unexpected error: {err}"
    );
}
