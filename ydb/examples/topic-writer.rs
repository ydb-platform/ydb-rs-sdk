use tonic::codegen::Bytes;
use ydb::{
    ClientBuilder, TopicWriter, TopicWriterMessage, TopicWriterMessageBuilder,
    TopicWriterOptionsBuilder, YdbResult,
};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .client()?;
    client.wait().await?;
    let mut topic_client = client.topic_client();

    // Default topic writer initialization
    let writer: TopicWriter = topic_client.create_writer("/database/topic/path1".to_string());

    // Parametrized topic write initialization
    let _writer_with_params = topic_client.create_writer_with_params(
        "/database/topic/path1".to_string(),
        TopicWriterOptionsBuilder::default()
            .producer_id("some_id".to_string())
            .build()?,
    );

    // Simple write
    writer.write_message(TopicWriterMessage::new("123")).await?;

    // Simple write bytes
    writer
        .write_message(TopicWriterMessage::new(vec![50, 51, 52]))
        .await?;

    // Write with meta info
    writer
        .write_message(
            TopicWriterMessageBuilder::default()
                .seq_no(123)
                .created_at(std::time::Instant::now())
                .data(Bytes::from("123".to_string()))
                .build()?,
        )
        .await?;

    // Write messages bulk
    writer
        .write_messages_bulk(vec![
            TopicWriterMessage::new("123"),
            TopicWriterMessage::new("456"),
            TopicWriterMessage::new("789"),
        ])
        .await?;

    Ok(())
}
