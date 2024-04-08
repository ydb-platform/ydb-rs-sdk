use ydb::{ClientBuilder, MetadataUrlCredentials, Query, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .with_credentials(MetadataUrlCredentials::new())
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
