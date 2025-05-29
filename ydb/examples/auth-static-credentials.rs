use std::time::Duration;
use tokio::time::timeout;
use ydb::{ClientBuilder, Query, StaticCredentials, YdbError, YdbResult};

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

    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    };

    println!("created\nmake a query...");
    let product: i32 = client
        .table_client()
        .retry_transaction(|mut t| async move {
            let res = t.query(Query::from("SELECT 14 * 3 AS product")).await?;
            Ok(res.into_only_row()?.remove_field_by_name("product")?)
        })
        .await?
        .try_into()
        .unwrap();

    println!("product: {}", product);
    Ok(())
}
