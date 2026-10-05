use std::time::{Duration, SystemTime};

use ydb::{
    AlterTopicOptionsBuilder, ClientBuilder, Codec, ConsumerBuilder, CreateTopicOptionsBuilder,
    DescribeTopicOptionsBuilder, PartitioningStrategy, TopicReaderOptions, TopicSelector,
    TopicSelectors, TopicWriterMessage, TopicWriterOptions, Transaction, YdbError, closure,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    tokio::time::timeout(Duration::from_secs(90), run()).await??;
    tracing::info!("All topic scenarios completed");
    Ok(())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let connection_string = match std::env::var("YDB_CONNECTION_STRING") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "grpc://localhost:2136/local".to_owned(),
        Err(error) => return Err(error.into()),
    };
    // [BEGIN topic_init]
    let client = ClientBuilder::new_from_connection_string(connection_string)?
        .build()
        .await?;
    let mut topic_client = client.topic_client();
    // [END topic_init]

    let topic_path = format!("ydb_tech_{}", uuid::Uuid::new_v4().simple());
    let second_topic = format!("{topic_path}_another");
    let consumers = vec![
        ConsumerBuilder::default().name("consumer".into()).build()?,
        ConsumerBuilder::default()
            .name("selectors".into())
            .build()?,
        ConsumerBuilder::default()
            .name("transaction".into())
            .build()?,
    ];
    // [BEGIN topic_create]
    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .min_active_partitions(3)
                .partition_count_limit(5)
                .supported_codecs(vec![Codec::RAW, Codec::ZSTD])
                .consumers(consumers)
                .build()?,
        )
        .await?;
    // [END topic_create]
    let result = async {
        topic_client
            .create_topic(
                second_topic.clone(),
                CreateTopicOptionsBuilder::default()
                    .min_active_partitions(3)
                    .partition_count_limit(3)
                    .consumers(vec![
                        ConsumerBuilder::default()
                            .name("selectors".into())
                            .build()?,
                    ])
                    .build()?,
            )
            .await?;

        // [BEGIN topic_alter]
        topic_client
            .alter_topic(
                topic_path.clone(),
                AlterTopicOptionsBuilder::default()
                    .set_min_active_partitions(5)
                    .build()?,
            )
            .await?;
        // [END topic_alter]

        // [BEGIN topic_describe]
        let description = topic_client
            .describe_topic(
                topic_path.clone(),
                DescribeTopicOptionsBuilder::default()
                    .include_stats(true)
                    .build()?,
            )
            .await?;
        // [END topic_describe]
        if description.consumers.len() != 3 {
            return Err(YdbError::Custom("Unexpected consumer count".into()));
        }

        // [BEGIN topic_start_writer]
        let writer = topic_client
            .create_writer_with_params(
                TopicWriterOptions::builder()
                    .topic_path(topic_path.clone())
                    .producer_id("ydb-tech-producer".to_owned())
                    .partitioning(PartitioningStrategy::PartitionId(0))
                    .build(),
            )
            .await?;
        // [END topic_start_writer]

        // [BEGIN topic_write]
        writer
            .write(
                TopicWriterMessage::builder()
                    .data(b"buffered".to_vec())
                    .build(),
            )
            .await?;
        // [END topic_write]

        // [BEGIN topic_write_ack]
        writer
            .write_with_ack(
                TopicWriterMessage::builder()
                    .data(b"acknowledged".to_vec())
                    .build(),
            )
            .await?;
        // [END topic_write_ack]
        writer.stop().await?;

        // [BEGIN topic_start_reader]
        let mut reader = topic_client.create_reader("consumer", &topic_path).await?;
        // [END topic_start_reader]
        let mut payloads = Vec::new();
        while payloads.len() < 2 {
            // [BEGIN topic_read_commit]
            let mut batch = reader.read_batch().await?;
            for message in &mut batch.messages {
                if let Some(payload) = message.read_and_take().await? {
                    payloads.push(payload);
                }
            }
            reader.commit_with_ack(batch.get_commit_marker()).await?;
            // [END topic_read_commit]
        }
        if payloads != vec![b"buffered".to_vec(), b"acknowledged".to_vec()] {
            return Err(YdbError::Custom("Unexpected topic payloads".into()));
        }
        drop(reader);

        // [BEGIN topic_reader_selectors]
        let mut reader = topic_client
            .create_reader_with_params(
                TopicReaderOptions::builder()
                    .consumer("selectors")
                    .topic(TopicSelectors(vec![
                        TopicSelector::builder()
                            .path(topic_path.clone())
                            .partition_ids(vec![0])
                            .build(),
                        TopicSelector::builder()
                            .path(second_topic.clone())
                            .partition_ids(vec![1])
                            .read_from(SystemTime::UNIX_EPOCH)
                            .build(),
                    ]))
                    .build(),
            )
            .await?;
        // [END topic_reader_selectors]
        let batch = reader.read_batch().await?;
        if batch.messages.is_empty() {
            return Err(YdbError::Custom(
                "The selector reader returned an empty batch".into(),
            ));
        }
        drop(reader);

        let mut reader = topic_client
            .create_reader("transaction", &topic_path)
            .await?;
        let mut received = 0;
        client
            .query_client()
            .retry_tx(closure!(
                [&mut reader, &mut received],
                async |tx: &mut Transaction| {
                    while *received < 2 {
                        // [BEGIN topic_read_tx]
                        let batch = reader.pop_batch_in_tx(tx).await?;
                        for message in &batch.messages {
                            tracing::info!(
                                offset = message.offset,
                                "Received a message in transaction"
                            );
                        }
                        // [END topic_read_tx]
                        *received += batch.messages.len();
                    }
                    Ok(())
                }
            ))
            .await?;
        drop(reader);
        Ok::<(), YdbError>(())
    }
    .await;

    // [BEGIN topic_drop]
    topic_client.drop_topic(topic_path).await?;
    // [END topic_drop]
    topic_client.drop_topic(second_topic).await?;
    result?;
    Ok(())
}
