use std::time::Duration;
use tokio::time::timeout;
use ydb::{ClientBuilder, MetadataUrlCredentials, Query, YdbError, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client =
        ClientBuilder::new_from_connection_string(std::env::var("YDB_CONNECTION_STRING")?)?
            .with_credentials(MetadataUrlCredentials::new())
            .client()?;

    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    };

    let sum: i32 = client
        .table_client()
        .retry_transaction(|mut t| async move {
            let res = t.query(Query::from("SELECT 1 + 1 AS sum")).await?;
            Ok(res.into_only_row()?.remove_field_by_name("sum")?)
        })
        .await?
        .try_into()
        .unwrap();
    println!("sum: {sum}");
    Ok(())
}
