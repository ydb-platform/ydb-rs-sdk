use std::time::Duration;
use tokio::time::timeout;
use ydb::{AccessTokenCredentials, ClientBuilder, YdbError, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?
        .with_credentials(AccessTokenCredentials::from("asd"))
        .client()?;

    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    };

    let mut row = client
        .query_client()
        .query_row("SELECT 1 + 1 AS sum")
        .await?;
    let sum: i32 = row.remove_field_by_name("sum")?.try_into()?;
    println!("sum: {sum}");
    Ok(())
}
