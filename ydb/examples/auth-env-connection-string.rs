use std::time::Duration;
use tokio::time::timeout;
use ydb::{ClientBuilder, MetadataUrlCredentials, YdbError, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client =
        ClientBuilder::new_from_connection_string(std::env::var("YDB_CONNECTION_STRING")?)?
            .with_credentials(MetadataUrlCredentials::new())
            .client()?;

    match timeout(Duration::from_secs(3), client.wait()).await {
        Ok(res) => res?,
        _ => {
            return Err(YdbError::from("Connection timeout"));
        }
    };

    let mut row = client
        .query_client()
        .query_row("SELECT 1 + 1 AS sum")
        .await?;
    let sum: i32 = row.remove_field_by_name("sum")?.try_into()?;
    println!("sum: {sum}");
    Ok(())
}
