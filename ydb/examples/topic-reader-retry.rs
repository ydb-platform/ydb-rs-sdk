use std::time::Duration;
use tokio::time::timeout;
use tracing::info;
use ydb::{ClientBuilder, YdbError, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    tracing_subscriber::fmt().init();

    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .client()?;

    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    };

    info!("Connected to YDB");

    let mut topic_client = client.topic_client();

    let mut reader = topic_client
        .create_reader("consumer".to_string(), "test-topic".to_string())
        .await?;

    let batch0 = reader.read_batch().await?;
    let batch1 = reader.read_batch().await?;

    info!(?batch0, "Batch0 processed");
    reader.commit(batch0.get_commit_marker())?;

    info!(?batch1, "Batch1 processed");
    reader.commit_with_ack(batch1.get_commit_marker()).await?;
    info!("Batch1 is guaranteed to be committed");

    Ok(())
}
