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

    let messages = [b"first tx writer".to_vec(), b"second tx writer".to_vec()];

    for message_data in &messages {
        client
            .query_client()
            .retry_tx(closure!(
                [&mut topic_client, &topic_path, message_data],
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
    }

    let mut reader = topic_client
        .create_reader(consumer_name, topic_path.clone())
        .await?;

    let received = timeout(Duration::from_secs(10), async {
        let mut received = Vec::with_capacity(messages.len());
        while received.len() < messages.len() {
            let batch = reader.read_batch().await?;
            for mut message in batch.messages {
                if let Some(body) = message.read_and_take().await? {
                    received.push(body);
                }
            }
        }
        YdbResult::Ok(received)
    })
    .await
    .map_err(|_| crate::YdbError::custom("timed out waiting for committed tx messages"))??;

    assert_eq!(received, messages);

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
