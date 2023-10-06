use tracing_test::traced_test;

use crate::client_topic::list_types::ConsumerBuilder;
use crate::test_integration_helper::create_client;
use crate::{
    client_topic::client::TopicOptionsBuilder, TopicWriterMessageBuilder,
    TopicWriterOptionsBuilder, YdbResult,
};
use tracing::trace;
use ydb_grpc::ydb_proto::topic::stream_read_message::init_request::TopicReadSettings;
use ydb_grpc::ydb_proto::topic::{stream_read_message, stream_write_message};

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn create_delete_topic_test() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "test_topic".to_string();
    let topic_path = format!("{}/{}", database_path, topic_name);

    let mut topic_client = client.topic_client();
    let mut scheme_client = client.scheme_client();

    let _ = topic_client.drop_topic(topic_path.clone()).await; // ignoring error

    topic_client
        .create_topic(topic_path.clone(), TopicOptionsBuilder::default().build()?)
        .await?;
    let directories_after_topic_creation =
        scheme_client.list_directory(database_path.clone()).await?;
    assert!(directories_after_topic_creation
        .iter()
        .any(|d| d.name == topic_name));

    topic_client.drop_topic(topic_path).await?;
    let directories_after_topic_droppage = scheme_client.list_directory(database_path).await?;
    assert!(!directories_after_topic_droppage
        .iter()
        .any(|d| d.name == topic_name));

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn send_message_test() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "test_topic".to_string();
    let topic_path = format!("{}/{}", database_path, topic_name);
    let producer_id = "test-producer-id".to_string();
    let consumer_name = "test-consumer".to_string();

    let mut topic_client = client.topic_client();
    let _ = topic_client.drop_topic(topic_path.clone()).await; // ignoring error

    topic_client
        .create_topic(
            topic_path.clone(),
            TopicOptionsBuilder::default()
                .consumers(vec![ConsumerBuilder::default()
                    .name(consumer_name.clone())
                    .build()?])
                .build()?,
        )
        .await?;

    println!("sent-0");

    // manual seq
    let mut writer_manual = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .auto_seq_no(false)
                .topic_path(topic_path.clone())
                .producer_id(producer_id.clone())
                .build()?,
        )
        .await?;
    println!("sent-1");

    writer_manual
        .write(
            TopicWriterMessageBuilder::default()
                .seq_no(Some(200))
                .data("test-1".as_bytes().into())
                .build()?,
        )
        .await?;
    println!("sent-2");

    writer_manual
        .write_with_ack(
            TopicWriterMessageBuilder::default()
                .seq_no(Some(300))
                .data("test-2".as_bytes().into())
                .build()?,
        )
        .await?;
    println!("sent-3");

    drop(writer_manual);

    // quto-seq
    let mut writer = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .auto_seq_no(true)
                .topic_path(topic_path.clone())
                .producer_id(producer_id)
                .build()?,
        )
        .await?;

    writer
        .write_with_ack(
            TopicWriterMessageBuilder::default()
                .data("test-3".as_bytes().into())
                .build()?,
        )
        .await?;
    println!("sent-4");

    // TODO: read messages with raw grpc queries to check it;
    let mut _grpc_client = topic_client
        .raw_client_connection()
        .await?
        .get_grpc_service();

    Ok(())
}
