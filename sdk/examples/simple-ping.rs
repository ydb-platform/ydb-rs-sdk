use ydb::YdbResult;

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client: ydb::Client = ydb::ClientBuilder::new().build().unwrap();
    client.wait().await.unwrap();
    let table_client = client.table_client();
    let res = table_client
        .retry_transaction(
            ydb::TransactionOptions::new(),
            ydb::RetryOptions::new(),
            |t| async {
                let mut t = t; // force borrow for lifetime of t inside closure
                let res = t.query(ydb::Query::from("SELECT 1 + 1 AS sum")).await?;
                let sum: i32 = res
                    .first()
                    .unwrap()
                    .rows()
                    .next()
                    .unwrap()
                    .remove_field_by_name("sum")
                    .unwrap()
                    .try_into()
                    .unwrap();
                return Ok(sum);
            },
        )
        .await
        .unwrap();
    trace!("sum: {}", res);
    return Ok(());
}
