use ydb::{
    ClientBuilder, TopicWriter, TopicWriterMessageBuilder,
    TopicWriterOptionsBuilder, YdbResult,
};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .client()?;
    client.wait().await?;
    let mut topic_client = client.topic_client();

    // Default topic writer initialization
    let writer: TopicWriter = topic_client.create_writer_with_params("/local/my-topic".to_string(), TopicWriterOptionsBuilder::default()
        .producer_id("some_id".to_string())
        .build()?).await;

    // Parametrized topic write initialization
    let _writer_with_params = topic_client.create_writer_with_params(
        "/database/topic/path1".to_string(),
        TopicWriterOptionsBuilder::default()
            .producer_id("some_id".to_string())
            .build()?,
    );

    // Simple write string, waits on message being written into internal buffer
    writer
        .write(
            TopicWriterMessageBuilder::default()
                .data("Sent from Rust SDK".as_bytes().to_vec())
                .build()?,
        )
        .await?;



    /*
    // Simple write raw bytes, waits on message being written into internal buffer
    writer
        .write(
            TopicWriterMessageBuilder::default()
                .data(vec![50, 51, 52])
                .build()?,
        )
        .await?;

    // Write with meta info
    writer
        .write(
            TopicWriterMessageBuilder::default()
                .seq_no(123)
                .created_at(std::time::SystemTime::now())
                .data(vec![50, 51, 52])
                .build()?,
        )
        .await?;

    // Write and wait on message being sent to server and returned confirmation or error
    let _ack_info = writer.write_with_ack(TopicWriterMessageBuilder::default()
        .data(vec![50, 51, 52])
        .build()?).await?;

    // Write and get write future, you can wait on that future for server acknowledgement or just ignore it
    let ack_future = writer.write_with_ack_future(TopicWriterMessageBuilder::default()
        .data(vec![50, 51, 52])
        .build()?).await?;

    ack_future.await;
    
    // Waits on current buffer messages to be sent and received confirmation
    for n_message in 1..10 {
        writer
            .write(
                TopicWriterMessageBuilder::default()
                    .data(vec![n_message])
                    .build()?,
            )
            .await?;
    }
    writer.flush().await?;
    */
    Ok(())
}
