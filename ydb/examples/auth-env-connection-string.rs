use ydb::{ClientBuilder, Query, YandexMetadata, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::from_str(std::env::var("YDB_CONNECTION_STRING")?)?
        .with_credentials(YandexMetadata::new())
        .client()?;
    client.wait().await?;
    let sum: i32 = client
        .table_client()
        .retry_transaction(|mut t| async move {
            let res = t.query(Query::from("SELECT 1 + 1 AS sum")).await?;
            Ok(res.into_only_row()?.remove_field_by_name("sum")?)
        })
        .await?
        .try_into()
        .unwrap();
    println!("sum: {}", sum);
    Ok(())
}
