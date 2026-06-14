use super::FromYdbRow;
use crate::errors::YdbResult;
use crate::result::Row;
use crate::test_integration_helper::create_client;

#[derive(Debug)]
struct SumRow {
    sum: i64,
}

impl FromYdbRow for SumRow {
    fn from_row(mut row: Row) -> YdbResult<Self> {
        Ok(Self {
            sum: row.remove_field_by_name("sum")?.try_into()?,
        })
    }
}

#[tokio::test]
#[ignore] // need YDB access
async fn query_client_multi_result_set() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client.query_client().clone_with_idempotent_operations(true);

    let mut stream = qc.query("SELECT 42 AS a; SELECT 1 AS b, 2 AS c;").await?;
    let mut count = 0usize;
    while stream.next_result_set().await?.is_some() {
        count += 1;
    }
    stream.close().await?;
    assert_eq!(count, 2);
    Ok(())
}

#[tokio::test]
#[ignore] // need YDB access
async fn query_client_from_ydb_row() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client.query_client().clone_with_idempotent_operations(true);

    let mut stream = qc.query("SELECT 1 + 1 AS sum").await?;
    let result_set = stream.next_result_set().await?.expect("one result set");
    let row = result_set.into_iter().next().expect("one row");
    let typed = SumRow::from_row(row)?;
    assert_eq!(typed.sum, 2);
    stream.close().await?;
    Ok(())
}
