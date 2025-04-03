use ydb::{
    ClientBuilder, TopicWriter, TopicWriterMessageBuilder, TopicWriterOptionsBuilder, YdbResult,
};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .client()?;
    client.wait().await?;

    let mut topic_client = client.topic_client();
    let mut writer: TopicWriter = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default() // TODO: is it really should be mutable?
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
