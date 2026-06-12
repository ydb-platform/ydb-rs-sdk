use crate::client_query::{QuerySessionMode, QueryTransactionOptions, QueryTxMode};
use crate::errors::YdbResult;
use crate::test_integration_helper::create_client;
use tracing_test::traced_test;

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_client_select_one() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client.query_client().clone_with_idempotent_operations(true);

    let mut row = qc.query_row("SELECT 1 + 1 AS sum").await?;
    let sum: i64 = row.remove_field_by_name("sum")?.try_into()?;
    assert_eq!(sum, 2);
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_client_exec_ddl() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client.query_client().clone_with_idempotent_operations(true);

    let _ = qc.exec("DROP TABLE IF EXISTS query_client_test").await;
    qc.exec("CREATE TABLE query_client_test (id Int64, val Utf8, PRIMARY KEY(id))")
        .await?;
    qc.exec("DROP TABLE query_client_test").await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_client_multi_result_set() -> YdbResult<()> {
    let client = create_client().await?;
    let qc = client.query_client().clone_with_idempotent_operations(true);

    let set_count = qc
        .retry_transaction(async |tx| {
            let mut stream = tx.query("SELECT 42 AS a; SELECT 1 AS b, 2 AS c;").await?;
            let mut count = 0usize;
            while stream.next_result_set().await?.is_some() {
                count += 1;
            }
            stream.close().await?;
            Ok(count)
        })
        .await?;

    assert_eq!(set_count, 2);
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_client_retry_transaction_upsert() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client.query_client().clone_with_idempotent_operations(true);

    let _ = qc.exec("DROP TABLE IF EXISTS query_client_test").await;
    qc.exec("CREATE TABLE query_client_test (id Int64, val Utf8, PRIMARY KEY(id))")
        .await?;

    let upsert = "DECLARE $id AS Int64; DECLARE $val AS Utf8; \
                  UPSERT INTO query_client_test (id, val) VALUES ($id, $val)";

    qc.retry_transaction(async |tx| {
        for id in 0..3_i64 {
            tx.exec(upsert)
                .param("$id", id)
                .param("$val", format!("v{id}"))
                .await?;
        }
        Ok(())
    })
    .await?;

    let mut row = qc
        .query_row("SELECT COUNT(*) AS cnt FROM query_client_test")
        .await?;
    let cnt: i64 = row.remove_field_by_name("cnt")?.try_into()?;
    assert_eq!(cnt, 3);

    qc.exec("DROP TABLE query_client_test").await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_client_pooled_session_not_implemented() {
    let client = create_client().await.unwrap();
    let mut qc = client
        .query_client()
        .clone_with_session_mode(QuerySessionMode::Pool);

    let err = qc.query_row("SELECT 1").await.unwrap_err();
    assert!(err.to_string().contains("session pool is not implemented"));
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_client_snapshot_read_only_tx() -> YdbResult<()> {
    let client = create_client().await?;
    let qc = client
        .query_client()
        .clone_with_idempotent_operations(true)
        .clone_with_transaction_options(
            QueryTransactionOptions::new().with_mode(QueryTxMode::SnapshotReadOnly),
        );

    let value: i64 = qc
        .retry_transaction(async |tx| {
            let mut row = tx.query_row("SELECT 42 AS v").await?;
            Ok(row.remove_field_by_name("v")?.try_into()?)
        })
        .await?;

    assert_eq!(value, 42);
    Ok(())
}
