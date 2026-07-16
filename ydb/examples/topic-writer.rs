#![recursion_limit = "256"]
use std::time::Duration;
use tokio::time::timeout;
use ydb::{
    ClientBuilder, TopicWriter, TopicWriterMessage, TopicWriterOptions, YdbError, YdbResult,
};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client =
        ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?.client()?;

    match timeout(Duration::from_secs(3), client.wait()).await {
        Ok(res) => res?,
        _ => {
            return Err(YdbError::from("Connection timeout"));
        }
    };

    let mut topic_client = client.topic_client();
    let writer: TopicWriter = topic_client
        .create_writer_with_params(
            TopicWriterOptions::builder()
                .topic_path("/local/my-topic".to_string())
                .producer_id("some_id".to_string())
                .build(),
        )
        .await?;

    writer
        .write(
            TopicWriterMessage::builder()
                .data("Sent from Rust SDK".as_bytes().to_vec())
                .build(),
        )
        .await?;

    writer.stop().await?;
    Ok(())
}
