use ydb::{ClientBuilder, Query, StaticToken, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::from_str("grpc://localhost:2136?database=local")?
        .with_credentials(StaticToken::from("asd"))
        .client()?;
    client.wait().await?;
    let sum: i32 = client
        .table_client()
        .retry_transaction(|mut t| async move {
            let res = t.query(Query::from("SELECT 1 + 1 AS sum")).await?;
            return Ok(res.into_only_row()?.remove_field_by_name("sum")?);
        })
        .await?
        .try_into()
        .unwrap();
    println!("sum: {}", sum);
    return Ok(());
}
