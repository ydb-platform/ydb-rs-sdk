use super::{QuerySessionPoolSettings, QueryTransactionOptions, QueryTxMode};
use crate::errors::YdbResult;
use crate::test_integration_helper::{create_client, create_client_with_session_pool};
use crate::types::Value;
use crate::ydb_struct;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
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
    let table_name = unique_table_name("query_client_test_exec_ddl");

    let _ = qc.exec(format!("DROP TABLE IF EXISTS {table_name}")).await;
    qc.exec(format!(
        "CREATE TABLE {table_name} (id Int64, val Utf8, PRIMARY KEY(id))"
    ))
    .await?;
    qc.exec(format!("DROP TABLE {table_name}")).await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_client_autocommit_by_default() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client.query_client().clone_with_idempotent_operations(true);
    let table_name = unique_table_name("query_client_with_commit");

    let _ = qc.exec(format!("DROP TABLE IF EXISTS {table_name}")).await;
    qc.exec(format!(
        "CREATE TABLE {table_name} (id Int64, val Int64, PRIMARY KEY(id))"
    ))
    .await?;

    qc.exec(format!(
        "UPSERT INTO {table_name} (id, val) VALUES ($id, $val)"
    ))
    .param("$id", 1_i64)
    .param("$val", 77_i64)
    .await?;

    let mut row = qc
        .query_row(format!("SELECT val FROM {table_name} WHERE id = 1"))
        .await?;
    let val: Option<i64> = row.remove_field_by_name("val")?.try_into()?;
    assert_eq!(val, Some(77));

    qc.exec(format!("DROP TABLE {table_name}")).await?;
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
    let table_name = unique_table_name("query_client_test_upsert");

    let _ = qc.exec(format!("DROP TABLE IF EXISTS {table_name}")).await;
    qc.exec(format!(
        "CREATE TABLE {table_name} (id Int64, val Utf8, PRIMARY KEY(id))"
    ))
    .await?;

    let upsert = format!("UPSERT INTO {table_name} (id, val) VALUES ($id, $val)");

    qc.retry_transaction(async |tx| {
        for id in 0..3_i64 {
            tx.exec(&upsert)
                .param("$id", id)
                .param("$val", format!("v{id}"))
                .await?;
        }
        Ok(())
    })
    .await?;

    let mut row = qc
        .query_row(format!("SELECT COUNT(*) AS cnt FROM {table_name}"))
        .await?;
    let cnt: u64 = row.remove_field_by_name("cnt")?.try_into()?;
    assert_eq!(cnt, 3);

    qc.exec(format!("DROP TABLE {table_name}")).await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_client_pooled_session_select() -> YdbResult<()> {
    let client = create_client_with_session_pool(
        QuerySessionPoolSettings::new()
            .with_limit(4)
            .with_warm_up(1),
    )
    .await?;
    let mut qc = client.query_client().clone_with_idempotent_operations(true);

    let mut row = qc.query_row("SELECT 1 + 1 AS sum").await?;
    let sum: i64 = row.remove_field_by_name("sum")?.try_into()?;
    assert_eq!(sum, 2);
    Ok(())
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

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_lazy_tx_materializes_on_first_query() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client.query_client().clone_with_idempotent_operations(true);
    let table_name = unique_table_name("query_lazy_tx");

    let _ = qc.exec(format!("DROP TABLE IF EXISTS {table_name}")).await;
    qc.exec(format!(
        "CREATE TABLE {table_name} (id Int64, val Int64, PRIMARY KEY(id))"
    ))
    .await?;

    qc.retry_transaction(async |tx| {
        assert!(
            tx.tx_id_for_test().is_none(),
            "lazy transaction must not have tx_id before the first query"
        );

        tx.exec(format!(
            "UPSERT INTO {table_name} (id, val) VALUES ($id, $val)"
        ))
        .param("$id", 1_i64)
        .param("$val", 42_i64)
        .await?;

        let _tx_id = tx
            .tx_id_for_test()
            .filter(|id| !id.is_empty())
            .expect("lazy transaction must receive tx_id from the first ExecuteQuery");

        let mut row = tx
            .query_row(format!("SELECT val FROM {table_name} WHERE id = 1"))
            .await?;
        let val: Option<i64> = row.remove_field_by_name("val")?.try_into()?;
        assert_eq!(val, Some(42));

        Ok(())
    })
    .await?;

    let mut row = qc
        .query_row(format!("SELECT val FROM {table_name} WHERE id = 1"))
        .await?;
    let val: Option<i64> = row.remove_field_by_name("val")?.try_into()?;
    assert_eq!(val, Some(42));

    qc.exec(format!("DROP TABLE {table_name}")).await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_lazy_tx_commit_without_queries() -> YdbResult<()> {
    let client = create_client().await?;
    let qc = client.query_client().clone_with_idempotent_operations(true);

    let value = qc
        .retry_transaction(async |tx| {
            assert!(tx.tx_id_for_test().is_none());
            Ok(7_i32)
        })
        .await?;

    assert_eq!(value, 7);
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_explicit_begin_via_begin() -> YdbResult<()> {
    let client = create_client().await?;
    let qc = client.query_client().clone_with_idempotent_operations(true);

    qc.retry_transaction(async |tx| {
        assert!(
            tx.tx_id_for_test().is_none(),
            "lazy transaction must not have tx_id before begin()"
        );
        tx.begin().await?;
        assert!(
            tx.tx_id_for_test().is_some_and(|id| !id.is_empty()),
            "explicit begin() must set tx_id before the first query"
        );

        let mut row = tx.query_row("SELECT 1 AS v").await?;
        let v: i64 = row.remove_field_by_name("v")?.try_into()?;
        assert_eq!(v, 1);
        Ok(())
    })
    .await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_explicit_begin_via_client_option() -> YdbResult<()> {
    let client = create_client().await?;
    let qc = client
        .query_client()
        .clone_with_idempotent_operations(true)
        .clone_with_transaction_options(QueryTransactionOptions::new().with_begin());

    qc.retry_transaction(async |tx| {
        tx.exec("SELECT 1 AS v").await?;
        assert!(
            tx.tx_id_for_test().is_some_and(|id| !id.is_empty()),
            "with_begin must obtain tx_id on the first operation via BeginTransaction RPC"
        );
        Ok(())
    })
    .await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_with_commit_on_last_query() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client.query_client().clone_with_idempotent_operations(true);
    let table_name = unique_table_name("query_with_commit");

    let _ = qc.exec(format!("DROP TABLE IF EXISTS {table_name}")).await;
    qc.exec(format!(
        "CREATE TABLE {table_name} (id Int64, val Int64, PRIMARY KEY(id))"
    ))
    .await?;

    qc.retry_transaction(async |tx| {
        tx.exec(format!(
            "UPSERT INTO {table_name} (id, val) VALUES ($id, $val)"
        ))
        .param("$id", 1_i64)
        .param("$val", 99_i64)
        .with_commit(true)
        .await?;

        let err = tx
            .query_row(format!("SELECT val FROM {table_name} WHERE id = 1"))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("already finished"),
            "query after with_commit must fail: {err}"
        );

        Ok(())
    })
    .await?;

    let mut row = qc
        .query_row(format!("SELECT val FROM {table_name} WHERE id = 1"))
        .await?;
    let val: Option<i64> = row.remove_field_by_name("val")?.try_into()?;
    assert_eq!(val, Some(99));

    qc.exec(format!("DROP TABLE {table_name}")).await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_execute_script() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client.query_client().clone_with_idempotent_operations(true);
    let op_client = client.operation_client();
    let table_name = unique_table_name("query_execute_script");

    const UPSERT_ROWS_COUNT: i32 = 100_000;
    const BATCH_SIZE: i32 = 10_000;
    const EXPECTED_CHECKSUM: u64 = 4_999_950_000;

    assert_eq!(UPSERT_ROWS_COUNT % BATCH_SIZE, 0);

    let _ = qc.exec(format!("DROP TABLE IF EXISTS {table_name}")).await;
    qc.exec(format!(
        "CREATE TABLE {table_name} (val Int64, PRIMARY KEY (val))"
    ))
    .await?;

    let upsert_query = format!("UPSERT INTO {table_name} SELECT val FROM AS_TABLE($values);");

    let mut upserted = 0_u32;
    for batch in 0..(UPSERT_ROWS_COUNT / BATCH_SIZE) {
        let from = batch * BATCH_SIZE;
        let to = from + BATCH_SIZE;
        let example = ydb_struct!("val" => 0_i32);
        let values: Vec<Value> = (from..to).map(|j| ydb_struct!("val" => j)).collect();
        let list = Value::list_from(example, values)?;
        qc.exec(&upsert_query).param("$values", list).await?;
        upserted += (to - from) as u32;
    }
    assert_eq!(upserted, UPSERT_ROWS_COUNT as u32);

    let mut row = qc
        .query_row(format!("SELECT COUNT(*) AS cnt FROM {table_name}"))
        .await?;
    let rows_from_db: Option<u64> = row.remove_field_by_name("cnt")?.try_into()?;
    assert_eq!(rows_from_db.unwrap_or(0), UPSERT_ROWS_COUNT as u64);

    let mut row = qc
        .query_row(format!("SELECT SUM(val) AS s FROM {table_name}"))
        .await?;
    let checksum_from_db: Option<i64> = row.remove_field_by_name("s")?.try_into()?;
    assert_eq!(checksum_from_db.unwrap_or(0) as u64, EXPECTED_CHECKSUM);

    let op = qc
        .execute_script(format!("SELECT val FROM {table_name};"))
        .results_ttl(Duration::from_secs(3600))
        .await?;

    let poll_deadline = Instant::now() + Duration::from_secs(120);
    loop {
        assert!(
            Instant::now() < poll_deadline,
            "script operation did not become ready within 120s"
        );
        let status = op_client.get_operation(&op.id).await?;
        if status.ready {
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }

    let mut next_token = String::new();
    let mut rows_count = 0_usize;
    let mut checksum = 0_u64;

    loop {
        let page = qc
            .fetch_script_results(&op.id)
            .result_set_index(0)
            .rows_limit(1000)
            .fetch_token(&next_token)
            .await?;
        next_token = page.next_fetch_token;
        assert_eq!(page.result_set_index, 0);

        for mut row in page.result_set {
            rows_count += 1;
            let val: Option<i64> = row.remove_field_by_name("val")?.try_into()?;
            checksum += val.unwrap_or(0) as u64;
        }

        if next_token.is_empty() {
            break;
        }
    }

    assert_eq!(rows_count, UPSERT_ROWS_COUNT as usize);
    assert_eq!(checksum, EXPECTED_CHECKSUM);

    op_client.forget_operation(&op.id).await?;
    qc.exec(format!("DROP TABLE {table_name}")).await?;
    Ok(())
}
