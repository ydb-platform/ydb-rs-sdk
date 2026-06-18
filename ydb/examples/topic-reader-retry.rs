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

    let mut reader = topic_client.create_reader("consumer", "test-topic").await?;

    let batch0 = reader.read_batch().await?;
    let batch1 = reader.read_batch().await?;
    let batch2 = reader.read_batch().await?;

    info!(?batch0, "Batch0 processed");
    reader.commit(batch0.get_commit_marker())?;

    let handler1 = reader.commit_with_ack(batch1.get_commit_marker());
    let handler2 = reader.commit_with_ack(batch2.get_commit_marker());

    tokio::try_join!(handler1, handler2)?;
    info!("Both batch1 and batch2 were committed and confirmed to be acked by server");

    Ok(())
}
