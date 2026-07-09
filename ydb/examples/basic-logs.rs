use std::time::Duration;
use tokio::time::timeout;
use ydb::{ClientBuilder, YdbError, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    // very verbose logs
    tracing_subscriber::fmt()
        // enable everything
        .with_max_level(tracing::Level::TRACE)
        // sets this to be the default, global collector for this application.
        .init();

    let client =
        ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?.client()?;

    match timeout(Duration::from_secs(3), client.wait()).await {
        Ok(res) => res?,
        _ => {
            return Err(YdbError::from("Connection timeout"));
        }
    };

    println!("done");
    Ok(())
}
