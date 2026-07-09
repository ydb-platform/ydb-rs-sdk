use std::time::Duration;
use tokio::time::timeout;
use ydb::{ClientBuilder, StaticCredentials, YdbError, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    println!("create client...");
    let client =
        ClientBuilder::new_from_connection_string("grpc://localhost:2136/local".to_string())
            .unwrap()
            .with_credentials(StaticCredentials::new(
                "root".to_string(),
                "1234".to_string(),
                http::uri::Uri::from_static("grpc://localhost:2136/local"),
                "local".to_string(),
            ))
            .client()?;

    match timeout(Duration::from_secs(3), client.wait()).await {
        Ok(res) => res?,
        _ => {
            return Err(YdbError::from("Connection timeout"));
        }
    };

    println!("created\nmake a query...");
    let mut row = client
        .query_client()
        .query_row("SELECT 14 * 3 AS product")
        .await?;
    let product: i32 = row.remove_field_by_name("product")?.try_into()?;

    println!("product: {product}");
    Ok(())
}
