use std::time::Duration;
use tokio::time::timeout;
use ydb::{ClientBuilder, YdbError, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    // Get connection string from environment variable or use default
    let connection_string = std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or_else(|_| "grpc://localhost:2136?database=local".to_string());

    // Create a client from connection string
    let client = ClientBuilder::new_from_connection_string(&connection_string)?.client()?;

    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    };

    let table_client = client.table_client();

    // Check if full diagnostics should be collected
    let collect_full_diagnostics = std::env::var("YDB_COLLECT_FULL_DIAGNOSTICS").is_ok();

    // Execute explain data query with retry policy using a system query
    let result = table_client
        .retry_explain_data_query(
            "SELECT MIN(NodeId) FROM `.sys/nodes`",
            collect_full_diagnostics,
        )
        .await?;

    println!("Query AST: {}", result.query_ast);
    println!("Query Plan: {}", result.query_plan);

    // Print full diagnostics only if enabled
    if collect_full_diagnostics {
        println!();
        println!("Full Diagnostics: {}", result.query_full_diagnostics);
    }

    Ok(())
}
