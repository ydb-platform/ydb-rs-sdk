use std::time::Duration;
use tokio::time::timeout;
use tracing_test::traced_test;

use crate::client_topic::client::CreateTopicOptionsBuilder;
use crate::client_topic::list_types::ConsumerBuilder;
use crate::test_integration_helper::create_client;
use crate::{TopicWriterMessage, Transaction, YdbResult, closure};

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn topic_writer_tx_write_and_commit() -> YdbResult<()> {
    let client = create_client().await?;
    let database = client.database();
    let topic_path = format!("{database}/writer_tx_commit_test");
    let consumer_name = "writer-tx-consumer";

    let mut topic_client = client.topic_client();
    let _ = topic_client.drop_topic(topic_path.clone()).await;
    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .consumers(vec![
                    ConsumerBuilder::default()
                        .name(consumer_name.to_string())
                        .build()?,
                ])
                .build()?,
        )
        .await?;

    let message_data = b"hello from tx writer".to_vec();

    client
        .query_client()
        .retry_tx(closure!(
            [&mut topic_client, &topic_path, &message_data],
            async |tx: &mut Transaction| {
                let mut writer = topic_client.create_writer_tx(topic_path, tx).await?;
                writer
                    .write(
                        TopicWriterMessage::builder()
                            .data(message_data.clone())
                            .build(),
                    )
                    .await?;
                Ok(true)
            }
        ))
        .await?;

    let mut reader = topic_client
        .create_reader(consumer_name, topic_path.clone())
        .await?;

    let batch = timeout(Duration::from_secs(10), reader.read_batch())
        .await
        .map_err(|_| crate::YdbError::custom("timed out waiting for committed tx message"))??;

    let mut msg = batch.messages.into_iter().next().unwrap();
    let body = msg.read_and_take().await?.unwrap();
    assert_eq!(body, message_data);

    let _ = topic_client.drop_topic(topic_path).await;
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn topic_writer_tx_rollback_discards_message() -> YdbResult<()> {
    let client = create_client().await?;
    let database = client.database();
    let topic_path = format!("{database}/writer_tx_rollback_test");
    let consumer_name = "writer-tx-rb-consumer";

    let mut topic_client = client.topic_client();
    let _ = topic_client.drop_topic(topic_path.clone()).await;
    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .consumers(vec![
                    ConsumerBuilder::default()
                        .name(consumer_name.to_string())
                        .build()?,
                ])
                .build()?,
        )
        .await?;

    client
        .query_client()
        .retry_tx(closure!(
            [&mut topic_client, &topic_path],
            async |tx: &mut Transaction| {
                let mut writer = topic_client.create_writer_tx(topic_path, tx).await?;
                writer
                    .write(
                        TopicWriterMessage::builder()
                            .data(b"should be discarded".to_vec())
                            .build(),
                    )
                    .await?;
                tx.rollback().await?;
                Ok(())
            }
        ))
        .await?;

    let mut reader = topic_client
        .create_reader(consumer_name, topic_path.clone())
        .await?;

    match timeout(Duration::from_millis(500), reader.read_batch()).await {
        Err(_) => {}
        Ok(Ok(_)) => panic!("rolled-back tx message must not be visible to reader"),
        Ok(Err(e)) => return Err(e),
    }

    let _ = topic_client.drop_topic(topic_path).await;
    Ok(())
}
