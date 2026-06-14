use super::QuerySessionPoolSettings;
use crate::errors::YdbResult;
use crate::test_integration_helper::create_client;

#[tokio::test]
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
#[ignore] // need YDB access
async fn query_client_implicit_pool_stream() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client
        .query_client()
        .clone_with_idempotent_operations(true)
        .with_implicit_session_pool(
            QuerySessionPoolSettings::new()
                .with_limit(4)
                .with_warm_up(1),
        );

    let mut stream = qc.query("SELECT 1 + 1 AS sum").await?;
    let result_set = stream.next_result_set().await?.expect("one result set");
    let mut row = result_set.into_iter().next().expect("one row");
    let sum: i64 = row.remove_field_by_name("sum")?.try_into()?;
    assert_eq!(sum, 2);
    stream.close().await?;
    Ok(())
}
