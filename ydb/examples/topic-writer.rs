use std::time::Duration;
use tokio::time::timeout;
use ydb::{
    ClientBuilder, TopicWriter, TopicWriterMessageBuilder, TopicWriterOptionsBuilder, YdbError,
    YdbResult,
};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .client()?;

    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    };

    let mut topic_client = client.topic_client();
    let writer: TopicWriter = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .topic_path("/local/my-topic".to_string())
                .producer_id("some_id".to_string())
                .build()?,
        )
        .await?;

    writer
        .write(
            TopicWriterMessageBuilder::default()
                .data("Sent from Rust SDK".as_bytes().to_vec())
                .build()?,
        )
        .await?;

    writer.stop().await?;
    Ok(())
}
