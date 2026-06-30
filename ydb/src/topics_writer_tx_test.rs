use std::time::Duration;
use tokio::time::timeout;
use tracing_test::traced_test;

use crate::client_topic::client::CreateTopicOptionsBuilder;
use crate::client_topic::list_types::ConsumerBuilder;
use crate::test_integration_helper::create_client;
use crate::transaction::Transaction;
use crate::{TopicWriterMessageBuilder, YdbResult};

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
                .consumers(vec![ConsumerBuilder::default()
                    .name(consumer_name.to_string())
                    .build()?])
                .build()?,
        )
        .await?;

    let message_data = b"hello from tx writer".to_vec();

    let table_client = client.table_client();
    let topic_path_clone = topic_path.clone();
    let message_clone = message_data.clone();
    let topic_client_clone = topic_client.clone();

    table_client
        .retry_transaction(|mut t| {
            let topic_path_inner = topic_path_clone.clone();
            let message_inner = message_clone.clone();
            let mut tc = topic_client_clone.clone();
            async move {
                let mut writer = tc.create_writer_tx(topic_path_inner, &mut *t).await?;
                writer
                    .write(
                        TopicWriterMessageBuilder::default()
                            .data(message_inner)
                            .build()?,
                    )
                    .await?;
                writer.stop().await?;
                t.commit().await?;
                Ok(true)
            }
        })
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
                .consumers(vec![ConsumerBuilder::default()
                    .name(consumer_name.to_string())
                    .build()?])
                .build()?,
        )
        .await?;

    {
        let table_client = client.table_client();
        let mut tx = table_client.create_interactive_transaction();
        let mut writer = topic_client
            .create_writer_tx(topic_path.clone(), &mut tx)
            .await?;
        writer
            .write(
                TopicWriterMessageBuilder::default()
                    .data(b"should be discarded".to_vec())
                    .build()?,
            )
            .await?;
        writer.stop().await?;
        tx.rollback().await?;
    }

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
