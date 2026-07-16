use std::time::{Duration, Instant};

use tokio::time::timeout;
use tracing::{info, trace};
use tracing_test::traced_test;

use crate::client_topic::client::{
    CreateTopicOptionsBuilder, DescribeConsumerOptionsBuilder, TopicClient,
};
use crate::client_topic::list_types::ConsumerBuilder;
use crate::test_integration_helper::create_client;
use crate::{
    Client, TopicReaderBatch, TopicReaderOptions, TopicWriterMessage, TopicWriterOptions,
    Transaction, YdbError, YdbResult, YdbResultWithCustomerErr, closure,
};

async fn wait_topic_absent(
    client: &Client,
    database_path: &str,
    topic_name: &str,
) -> YdbResult<()> {
    loop {
        let mut scheme = client.scheme_client();
        let entries = scheme.list_directory(database_path.to_string()).await?;
        if !entries.iter().any(|entry| entry.name == topic_name) {
            return Ok(());
        }
        info!("waiting previous topic dropped...");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn create_topic(
    client: &Client,
    topic_name: &str,
    consumer_names: &[&str],
) -> YdbResult<String> {
    let database_path = client.database();
    let topic_path = format!("{database_path}/{topic_name}");

    let mut topic_client = client.topic_client();
    let _ = topic_client.drop_topic(topic_path.clone()).await;
    wait_topic_absent(client, &database_path, topic_name).await?;

    let consumers = consumer_names
        .iter()
        .map(|name| ConsumerBuilder::default().name((*name).to_string()).build())
        .collect::<YdbResult<Vec<_>>>()?;

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .consumers(consumers)
                .build()?,
        )
        .await?;
    trace!("topic reader tx test topic created: {topic_path}");

    Ok(topic_path)
}

async fn write_messages(
    topic_client: &mut TopicClient,
    topic_path: &str,
    producer_id: &str,
    payloads: &[&str],
) -> YdbResult<()> {
    let writer = topic_client
        .create_writer_with_params(
            TopicWriterOptions::builder()
                .auto_seq_no(false)
                .topic_path(topic_path)
                .producer_id(producer_id.to_string())
                .build(),
        )
        .await?;

    for (index, payload) in payloads.iter().enumerate() {
        writer
            .write_with_ack(
                TopicWriterMessage::builder()
                    .data(payload.as_bytes().to_vec())
                    .seq_no(index as i64 + 1)
                    .build(),
            )
            .await?;
    }

    writer.stop().await
}

async fn committed_offset(
    topic_client: &mut TopicClient,
    topic_path: &str,
    consumer_name: &str,
) -> YdbResult<i64> {
    let description = topic_client
        .describe_consumer(
            topic_path.to_string(),
            consumer_name.to_string(),
            DescribeConsumerOptionsBuilder::default()
                .include_stats(true)
                .build()?,
        )
        .await?;

    Ok(description.partitions[0].consumer_stats.committed_offset)
}

async fn wait_committed_offset(
    topic_client: &mut TopicClient,
    topic_path: &str,
    consumer_name: &str,
    expected_offset: i64,
) -> YdbResult<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let offset = committed_offset(topic_client, topic_path, consumer_name).await?;
        if offset == expected_offset {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(YdbError::Custom(format!(
                "timeout waiting for committed_offset={expected_offset}, last offset={offset}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn read_payloads_from_batch(batch: TopicReaderBatch) -> YdbResult<Vec<String>> {
    let mut payloads = Vec::new();
    for mut message in batch.messages {
        let data = message
            .read_and_take()
            .await?
            .expect("topic reader tx test message should contain data");
        payloads.push(String::from_utf8(data).expect("topic reader tx test payload must be UTF-8"));
    }
    Ok(payloads)
}

async fn read_payloads_from_reader(
    reader: &mut crate::TopicReader,
    expected_count: usize,
    timeout_message: &str,
) -> YdbResult<Vec<String>> {
    let mut payloads = Vec::new();
    while payloads.len() < expected_count {
        let batch = timeout(Duration::from_secs(30), reader.read_batch())
            .await
            .expect(timeout_message)?;
        payloads.extend(read_payloads_from_batch(batch).await?);
    }
    Ok(payloads)
}

fn expected_payloads(payloads: &[&str]) -> Vec<String> {
    payloads
        .iter()
        .map(|payload| (*payload).to_string())
        .collect()
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_topic_reader_tx_commit_advances_offset() -> YdbResult<()> {
    let client = create_client().await?;
    let topic_name = "query_topic_reader_tx_commit_advances_offset";
    let consumer_name = "query-topic-reader-tx-commit-consumer";
    let producer_id = "query-topic-reader-tx-commit-producer";
    let payloads = ["tx-commit-1", "tx-commit-2"];

    let topic_path = create_topic(&client, topic_name, &[consumer_name]).await?;
    let mut topic_client = client.topic_client();
    write_messages(&mut topic_client, &topic_path, producer_id, &payloads).await?;
    assert_eq!(
        committed_offset(&mut topic_client, &topic_path, consumer_name).await?,
        0
    );

    let mut reader = topic_client
        .create_reader(consumer_name.to_string(), topic_path.clone())
        .await?;
    let query_client = client.query_client();

    query_client
        .retry_tx(closure!(
            [&mut reader, &payloads],
            async |tx: &mut Transaction| {
                tx.begin().await?;
                let mut reader_tx = reader.tx_reader(tx).await?;
                let mut observed = Vec::new();
                while observed.len() < payloads.len() {
                    let batch = timeout(Duration::from_secs(10), reader_tx.read_batch())
                        .await
                        .expect("timeout waiting for tx reader batch")?;
                    observed.extend(read_payloads_from_batch(batch).await?);
                }
                assert_eq!(observed, expected_payloads(payloads));
                Ok(())
            }
        ))
        .await?;

    wait_committed_offset(
        &mut topic_client,
        &topic_path,
        consumer_name,
        payloads.len() as i64,
    )
    .await?;

    assert!(
        timeout(Duration::from_secs(2), reader.read_batch())
            .await
            .is_err(),
        "committed topic reader tx messages must not be redelivered"
    );

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_topic_reader_tx_callback_error_redelivers_messages() -> YdbResult<()> {
    let client = create_client().await?;
    let topic_name = "query_topic_reader_tx_callback_error_redelivers_messages";
    let consumer_name = "query-topic-reader-tx-error-consumer";
    let producer_id = "query-topic-reader-tx-error-producer";
    let payloads = ["tx-error-1", "tx-error-2"];

    let topic_path = create_topic(&client, topic_name, &[consumer_name]).await?;
    let mut topic_client = client.topic_client();
    write_messages(&mut topic_client, &topic_path, producer_id, &payloads).await?;

    let mut reader = topic_client
        .create_reader(consumer_name.to_string(), topic_path.clone())
        .await?;
    let query_client = client.query_client();

    let result: YdbResultWithCustomerErr<()> = query_client
        .retry_tx(closure!(
            [&mut reader, payloads],
            async |tx: &mut Transaction| {
                tx.begin().await?;
                let mut reader_tx = reader.tx_reader(tx).await?;
                let mut observed = Vec::new();
                while observed.len() < payloads.len() {
                    let batch = timeout(Duration::from_secs(10), reader_tx.read_batch())
                        .await
                        .expect("timeout waiting for tx reader batch")?;
                    observed.extend(read_payloads_from_batch(batch).await?);
                }
                assert_eq!(observed, expected_payloads(payloads));
                Err(YdbError::Custom("planned topic reader tx abort".into()).into())
            }
        ))
        .await;
    assert!(result.is_err());

    assert_eq!(
        committed_offset(&mut topic_client, &topic_path, consumer_name).await?,
        0
    );

    assert_eq!(
        read_payloads_from_reader(
            &mut reader,
            payloads.len(),
            "timeout waiting for redelivered batch"
        )
        .await?,
        expected_payloads(&payloads)
    );

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_topic_reader_tx_explicit_rollback_redelivers_messages() -> YdbResult<()> {
    let client = create_client().await?;
    let topic_name = "query_topic_reader_tx_explicit_rollback_redelivers_messages";
    let consumer_name = "query-topic-reader-tx-rollback-consumer";
    let producer_id = "query-topic-reader-tx-rollback-producer";
    let payloads = ["tx-rollback-1", "tx-rollback-2"];

    let topic_path = create_topic(&client, topic_name, &[consumer_name]).await?;
    let mut topic_client = client.topic_client();
    write_messages(&mut topic_client, &topic_path, producer_id, &payloads).await?;

    let mut reader = topic_client
        .create_reader(consumer_name.to_string(), topic_path.clone())
        .await?;
    let query_client = client.query_client();

    query_client
        .retry_tx(closure!(
            [&mut reader, &payloads],
            async |tx: &mut Transaction| {
                tx.begin().await?;
                let mut reader_tx = reader.tx_reader(tx).await?;
                let mut observed = Vec::new();
                while observed.len() < payloads.len() {
                    let batch = timeout(Duration::from_secs(10), reader_tx.read_batch())
                        .await
                        .expect("timeout waiting for tx reader batch")?;
                    observed.extend(read_payloads_from_batch(batch).await?);
                }
                assert_eq!(observed, expected_payloads(payloads));
                tx.rollback().await?;
                Ok(())
            }
        ))
        .await?;

    assert_eq!(
        committed_offset(&mut topic_client, &topic_path, consumer_name).await?,
        0
    );

    assert_eq!(
        read_payloads_from_reader(
            &mut reader,
            payloads.len(),
            "timeout waiting for redelivered batch"
        )
        .await?,
        expected_payloads(&payloads)
    );

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_topic_reader_tx_rewrap_same_reader_same_tx_commits_all_offsets() -> YdbResult<()> {
    let client = create_client().await?;
    let topic_name = "query_topic_reader_tx_rewrap_same_reader_same_tx_commits_all_offsets";
    let consumer_name = "query-topic-reader-tx-rewrap-consumer";
    let producer_id = "query-topic-reader-tx-rewrap-producer";
    let payloads = ["tx-rewrap-1", "tx-rewrap-2"];

    let topic_path = create_topic(&client, topic_name, &[consumer_name]).await?;
    let mut topic_client = client.topic_client();
    write_messages(&mut topic_client, &topic_path, producer_id, &payloads).await?;

    let reader_options = TopicReaderOptions::builder()
        .consumer(consumer_name)
        .topic(topic_path.clone())
        .batch_size(1)
        .build();
    let mut reader = topic_client
        .create_reader_with_params(reader_options)
        .await?;
    let query_client = client.query_client();

    query_client
        .retry_tx(closure!(
            [&mut reader, &payloads],
            async |tx: &mut Transaction| {
                tx.begin().await?;

                let mut observed = Vec::new();
                {
                    let mut reader_tx = reader.tx_reader(tx).await?;
                    let batch = timeout(Duration::from_secs(10), reader_tx.read_batch())
                        .await
                        .expect("timeout waiting for first tx reader batch")?;
                    observed.extend(read_payloads_from_batch(batch).await?);
                }

                {
                    let mut reader_tx = reader.tx_reader(tx).await?;
                    let batch = timeout(Duration::from_secs(10), reader_tx.read_batch())
                        .await
                        .expect("timeout waiting for second tx reader batch")?;
                    observed.extend(read_payloads_from_batch(batch).await?);
                }

                assert_eq!(observed, expected_payloads(payloads));
                Ok(())
            }
        ))
        .await?;

    wait_committed_offset(
        &mut topic_client,
        &topic_path,
        consumer_name,
        payloads.len() as i64,
    )
    .await?;

    Ok(())
}
