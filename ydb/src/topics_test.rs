use futures_util::StreamExt;
use std::time::{Duration, SystemTime};
use tracing_test::traced_test;

use crate::client_topic::list_types::ConsumerBuilder;
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use crate::test_integration_helper::create_client;
use crate::{
    client_topic::client::{AlterTopicOptionsBuilder, CreateTopicOptionsBuilder},
    TopicWriterMessageBuilder, TopicWriterOptionsBuilder, YdbError, YdbResult,
};
use crate::{Codec, DescribeTopicOptionsBuilder};
use tracing::{info, trace, warn};
use ydb_grpc::ydb_proto::topic::stream_read_message;
use ydb_grpc::ydb_proto::topic::stream_read_message::init_request::TopicReadSettings;
use ydb_grpc::ydb_proto::topic::v1::topic_service_client::TopicServiceClient;

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn create_delete_topic_test() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "del_test_topic".to_string();
    let topic_path = format!("{}/{}", database_path, topic_name);

    let mut topic_client = client.topic_client();
    let mut scheme_client = client.scheme_client();

    let _ = topic_client.drop_topic(topic_path.clone()).await; // ignoring error

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default().build()?,
        )
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
async fn describe_topic_test() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "describe_test_topic".to_string();
    let topic_path = format!("{}/{}", database_path, topic_name);

    let mut topic_client = client.topic_client();
    let mut scheme_client = client.scheme_client();

    let _ = topic_client.drop_topic(topic_path.clone()).await; // ignoring error

    let time = std::time::SystemTime::UNIX_EPOCH
        .checked_add(std::time::Duration::from_secs(100))
        .unwrap();

    let min_active_partitions = 5;
    let retention_period = std::time::Duration::from_secs(600);
    let retention_storage_mb = 100;
    let supported_codecs = vec![Codec::RAW, Codec::GZIP];
    let write_speed = 100;
    let write_burst = 50;
    let mut consumers = vec![
        ConsumerBuilder::default()
            .name("c1".to_string())
            .supported_codecs(vec![Codec::RAW, Codec::GZIP])
            .read_from(Some(time))
            .build()?,
        ConsumerBuilder::default().name("c2".to_string()).build()?,
    ];

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .retention_period(retention_period)
                .min_active_partitions(min_active_partitions)
                .retention_storage_mb(retention_storage_mb)
                .supported_codecs(supported_codecs.clone())
                .partition_write_speed_bytes_per_second(write_speed)
                .partition_write_burst_bytes(write_burst)
                .consumers(consumers.clone())
                .build()?,
        )
        .await?;
    let directories_after_topic_creation =
        scheme_client.list_directory(database_path.clone()).await?;
    assert!(directories_after_topic_creation
        .iter()
        .any(|d| d.name == topic_name));

    let topic_description = topic_client
        .describe_topic(
            topic_path.clone(),
            DescribeTopicOptionsBuilder::default()
                .include_stats(true)
                .include_location(true)
                .build()?,
        )
        .await?;
    assert_eq!(topic_description.path, topic_name);
    assert_eq!(topic_description.retention_period, retention_period);
    assert_eq!(
        topic_description
            .partitioning_settings
            .min_active_partitions,
        min_active_partitions
    );
    assert_eq!(
        topic_description.retention_storage_mb,
        Some(retention_storage_mb)
    );
    assert_eq!(topic_description.supported_codecs, supported_codecs);
    assert_eq!(
        topic_description.partition_write_speed_bytes_per_second,
        write_speed
    );
    assert_eq!(topic_description.partition_write_burst_bytes, write_burst);
    assert_eq!(topic_description.consumers.len(), consumers.len());

    // when `read_from` was not set, server returns zero timestamp
    consumers[1].read_from = Some(SystemTime::UNIX_EPOCH);

    for (expected, got) in consumers.iter().zip(topic_description.consumers.iter()) {
        assert_eq!(expected.name, got.name);
        assert_eq!(expected.important, got.important);
        assert_eq!(expected.read_from, got.read_from);
        assert_eq!(expected.supported_codecs, got.supported_codecs);
        for (k, v) in expected.attributes.iter() {
            assert_eq!(Some(v), got.attributes.get(k));
        }
    }

    for (expected_id, partition) in topic_description.partitions.iter().enumerate() {
        assert_eq!(partition.partition_id, expected_id as i64);
        assert!(partition.active);
    }

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn alter_topic_test() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "alter_test_topic".to_string();
    let topic_path = format!("{}/{}", database_path, topic_name);

    let mut topic_client = client.topic_client();
    let mut scheme_client = client.scheme_client();

    let _ = topic_client.drop_topic(topic_path.clone()).await; // ignoring error

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .retention_period(std::time::Duration::from_secs(600))
                .min_active_partitions(5)
                .build()?,
        )
        .await?;
    let directories_after_topic_creation =
        scheme_client.list_directory(database_path.clone()).await?;
    assert!(directories_after_topic_creation
        .iter()
        .any(|d| d.name == topic_name));

    let topic_description = topic_client
        .describe_topic(
            topic_path.clone(),
            DescribeTopicOptionsBuilder::default().build()?,
        )
        .await?;

    assert_eq!(topic_description.path, topic_name);
    assert_eq!(
        topic_description.retention_period,
        std::time::Duration::from_secs(600)
    );
    assert_eq!(
        topic_description
            .partitioning_settings
            .min_active_partitions,
        5
    );

    topic_client
        .alter_topic(
            topic_path.clone(),
            AlterTopicOptionsBuilder::default()
                .set_retention_period(std::time::Duration::from_secs(3600))
                .set_min_active_partitions(10)
                .build()?,
        )
        .await?;

    let topic_description = topic_client
        .describe_topic(
            topic_path.clone(),
            DescribeTopicOptionsBuilder::default().build()?,
        )
        .await?;

    assert_eq!(topic_description.path, topic_name);
    assert_eq!(
        topic_description.retention_period,
        std::time::Duration::from_secs(3600)
    );
    assert_eq!(
        topic_description
            .partitioning_settings
            .min_active_partitions,
        10
    );

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn send_message_test() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "send_test_topic".to_string();
    let topic_path = format!("{}/{}", database_path, topic_name);
    let producer_id = "test-producer-id".to_string();
    let consumer_name = "test-consumer".to_string();

    let mut topic_client = client.topic_client();
    let _ = topic_client.drop_topic(topic_path.clone()).await; // ignoring error
    trace!("previous topic removed");

    'wait_topic_dropped: loop {
        let mut scheme = client.scheme_client();
        let res = scheme.list_directory(database_path.clone()).await?;
        let mut topic_exists = false;
        for item in res.into_iter() {
            if item.name == topic_name {
                topic_exists = true;
                break;
            }
        }
        if !topic_exists {
            break 'wait_topic_dropped;
        }
        info!("waiting previous topic dropped...");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .consumers(vec![ConsumerBuilder::default()
                    .name(consumer_name.clone())
                    .build()?])
                .build()?,
        )
        .await?;

    trace!("topic created");

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
    trace!("first writer created");

    writer_manual
        .write(
            TopicWriterMessageBuilder::default()
                .seq_no(Some(200))
                .data("test-1".as_bytes().into())
                .build()?,
        )
        .await?;
    trace!("sent message test-1");

    writer_manual
        .write_with_ack(
            TopicWriterMessageBuilder::default()
                .seq_no(Some(300))
                .data("test-2".as_bytes().into())
                .build()?,
        )
        .await?;
    trace!("sent message test-2");
    writer_manual.stop().await?;

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
    trace!("sent message test-3");
    writer.stop().await?;

    let grpc_client = topic_client
        .raw_client_connection()
        .await?
        .get_grpc_service();

    let mut topic_messages = start_read_topic(grpc_client, consumer_name, topic_path).await?;

    let r_mess1 = topic_messages.recv().await.unwrap();
    assert_eq!(r_mess1.offset, 0);
    assert_eq!(r_mess1.seq_no, 200);
    assert_eq!(r_mess1.data, "test-1".as_bytes());

    let r_mess2 = topic_messages.recv().await.unwrap();
    assert_eq!(r_mess2.offset, 1);
    assert_eq!(r_mess2.seq_no, 300);
    assert_eq!(r_mess2.data, "test-2".as_bytes());

    let r_mess3 = topic_messages.recv().await.unwrap();
    assert_eq!(r_mess3.offset, 2);
    assert_eq!(r_mess3.seq_no, 301);
    assert_eq!(r_mess3.data, "test-3".as_bytes());

    Ok(())
}

async fn start_read_topic(
    mut grpc_topic_service: TopicServiceClient<InterceptedChannel>,
    consumer: String,
    topic_path: String,
) -> YdbResult<tokio::sync::mpsc::UnboundedReceiver<stream_read_message::read_response::MessageData>>
{
    let (reader_stream_tx, reader_stream_rx): (
        tokio::sync::mpsc::UnboundedSender<stream_read_message::FromClient>,
        tokio::sync::mpsc::UnboundedReceiver<stream_read_message::FromClient>,
    ) = tokio::sync::mpsc::unbounded_channel();

    let init_request = stream_read_message::from_client::ClientMessage::InitRequest(
        #[allow(clippy::needless_update)]
        stream_read_message::InitRequest {
            topics_read_settings: vec![TopicReadSettings {
                path: topic_path,
                ..TopicReadSettings::default()
            }],
            consumer,
            ..stream_read_message::InitRequest::default()
        },
    );

    let mess = stream_read_message::FromClient {
        client_message: Some(init_request),
    };

    reader_stream_tx
        .send(mess)
        .expect("failed to send init message from test topic reader");

    let request_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(reader_stream_rx);
    let mut reader_stream = grpc_topic_service
        .stream_read(request_stream)
        .await?
        .into_inner();

    let _init_response = reader_stream.next().await.ok_or(YdbError::custom(
        "failed receive init response in test reader",
    ))??;

    let data_request = stream_read_message::from_client::ClientMessage::ReadRequest(
        #[allow(clippy::needless_update)]
        stream_read_message::ReadRequest {
            bytes_size: 1024 * 1024,
            ..stream_read_message::ReadRequest::default()
        },
    );

    let mess = stream_read_message::FromClient {
        client_message: Some(data_request),
    };
    reader_stream_tx
        .send(mess)
        .expect("failed to send data request in test topic reader");

    let (topic_messages_tx, topic_messages_rx): (
        tokio::sync::mpsc::UnboundedSender<stream_read_message::read_response::MessageData>,
        tokio::sync::mpsc::UnboundedReceiver<stream_read_message::read_response::MessageData>,
    ) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(async move {
        loop {
            let mess = reader_stream.next().await;
            trace!("test topic reader receive server message: {:?}", mess);
            let mess = match mess {
                Some(Ok(mess)) => mess,
                mess => {
                    trace!("stop to receive reader stream mess in test: {:?}", mess);
                    return;
                }
            };

            let mess = if let stream_read_message::FromServer {
                server_message: Some(mess),
                ..
            } = mess
            {
                mess
            } else {
                warn!(
                    "failed decode server message in test topic reader: {:?}",
                    mess
                );
                return;
            };

            match mess {
                stream_read_message::from_server::ServerMessage::StartPartitionSessionRequest(
                    stream_read_message::StartPartitionSessionRequest{
                        partition_session: Some(partition_session),
                        ..
                    }
                ) => {
                    reader_stream_tx.send(stream_read_message::FromClient{ client_message: Some(
                        stream_read_message::from_client::ClientMessage::StartPartitionSessionResponse(
                            stream_read_message::StartPartitionSessionResponse{
                                partition_session_id: partition_session.partition_session_id,
                                ..stream_read_message::StartPartitionSessionResponse::default()
                            }
                        )) }).expect("send start partition response in test topic reader")
                },
                stream_read_message::from_server::ServerMessage::ReadResponse(
                    stream_read_message::ReadResponse{
                       partition_data,
                        ..
                    }
                ) => {
                    for pd in partition_data.into_iter(){
                        for batch in pd.batches.into_iter(){
                            for message in batch.message_data {
                                topic_messages_tx.send(message).expect("failed to send message from test topic reader")
                            }
                        }
                    }
                }
                mess => {
                    trace!("skip message in test reader: {:?}", mess);
                }
            };
        }
    });

    Ok(topic_messages_rx)
}
