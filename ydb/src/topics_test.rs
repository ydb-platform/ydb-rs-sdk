use futures_util::StreamExt;
use std::time::{Duration, Instant, SystemTime};
use tokio::time::timeout;
use tracing_test::traced_test;

use crate::client_topic::client::DescribeConsumerOptionsBuilder;
use crate::client_topic::list_types::ConsumerBuilder;
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use crate::test_helpers::CONNECTION_STRING;
use crate::test_integration_helper::{TcpForwardProxy, create_client};
use crate::{
    ClientBuilder, Codec, DescribeTopicOptionsBuilder, PartitioningStrategy, StaticDiscovery,
    TopicWriterMessage, TopicWriterOptions, YdbError, YdbResult,
    client_topic::client::{AlterTopicOptionsBuilder, CreateTopicOptionsBuilder},
};
use crate::{Transaction, closure};
use tracing::{debug, info, trace, warn};
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
    let topic_path = format!("{database_path}/{topic_name}");

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
    assert!(
        directories_after_topic_creation
            .iter()
            .any(|d| d.name == topic_name)
    );

    topic_client.drop_topic(topic_path).await?;
    let directories_after_topic_droppage = scheme_client.list_directory(database_path).await?;
    assert!(
        !directories_after_topic_droppage
            .iter()
            .any(|d| d.name == topic_name)
    );

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn describe_topic_test() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "describe_test_topic".to_string();
    let topic_path = format!("{database_path}/{topic_name}");

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
    assert!(
        directories_after_topic_creation
            .iter()
            .any(|d| d.name == topic_name)
    );

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
    let topic_path = format!("{database_path}/{topic_name}");

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
    assert!(
        directories_after_topic_creation
            .iter()
            .any(|d| d.name == topic_name)
    );

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
    let topic_path = format!("{database_path}/{topic_name}");
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
                .consumers(vec![
                    ConsumerBuilder::default()
                        .name(consumer_name.clone())
                        .build()?,
                ])
                .build()?,
        )
        .await?;

    trace!("topic created");

    // manual seq
    let writer_manual = topic_client
        .create_writer_with_params(
            TopicWriterOptions::builder()
                .auto_seq_no(false)
                .topic_path(topic_path.clone())
                .producer_id(producer_id.clone())
                .build(),
        )
        .await?;
    trace!("first writer created");

    writer_manual
        .write(
            TopicWriterMessage::builder()
                .seq_no(200)
                .data("test-1".as_bytes().into())
                .build(),
        )
        .await?;
    trace!("sent message test-1");

    writer_manual
        .write_with_ack(
            TopicWriterMessage::builder()
                .seq_no(300)
                .data("test-2".as_bytes().into())
                .build(),
        )
        .await?;
    trace!("sent message test-2");
    writer_manual.stop().await?;

    // auto-seq
    let writer = topic_client
        .create_writer_with_params(
            TopicWriterOptions::builder()
                .auto_seq_no(true)
                .topic_path(topic_path.clone())
                .producer_id(producer_id)
                .build(),
        )
        .await?;

    writer
        .write_with_ack(
            TopicWriterMessage::builder()
                .data("test-3".as_bytes().to_vec())
                .build(),
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
                    stream_read_message::StartPartitionSessionRequest {
                        partition_session: Some(partition_session),
                        ..
                    }
                ) => {
                    reader_stream_tx.send(stream_read_message::FromClient {
                        client_message: Some(
                            stream_read_message::from_client::ClientMessage::StartPartitionSessionResponse(
                                stream_read_message::StartPartitionSessionResponse {
                                    partition_session_id: partition_session.partition_session_id,
                                    ..stream_read_message::StartPartitionSessionResponse::default()
                                }
                            ))
                    }).expect("send start partition response in test topic reader")
                }
                stream_read_message::from_server::ServerMessage::ReadResponse(
                    stream_read_message::ReadResponse {
                        partition_data,
                        ..
                    }
                ) => {
                    for pd in partition_data.into_iter() {
                        for batch in pd.batches.into_iter() {
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

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn read_topic_message() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "read_topic_message".to_string();
    let topic_path = format!("{database_path}/{topic_name}");
    let producer_id = "test-producer-id".to_string();
    let consumer_name = "test-consumer".to_string();

    let mut topic_client = client.topic_client();
    let _ = topic_client.drop_topic(topic_path.clone()).await; // ignoring error
    debug!("previous topic removed");

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
                .consumers(vec![
                    ConsumerBuilder::default()
                        .name(consumer_name.clone())
                        .build()?,
                ])
                .build()?,
        )
        .await?;

    debug!("topic created");

    // manual seq
    let writer_manual = topic_client
        .create_writer_with_params(
            TopicWriterOptions::builder()
                .auto_seq_no(false)
                .topic_path(topic_path.clone())
                .producer_id(producer_id.clone())
                .build(),
        )
        .await?;
    debug!("first writer created");

    writer_manual
        .write(
            TopicWriterMessage::builder()
                .seq_no(200)
                .data("test-1".as_bytes().into())
                .build(),
        )
        .await?;
    debug!("sent message");

    let consumer_description_before_commit = topic_client
        .describe_consumer(
            topic_path.clone(),
            consumer_name.clone(),
            DescribeConsumerOptionsBuilder::default()
                .include_stats(true)
                .build()?,
        )
        .await?;

    assert_eq!(
        consumer_description_before_commit.partitions[0]
            .consumer_stats
            .committed_offset,
        0
    );

    info!("creating topic reader");
    let mut reader = topic_client
        .create_reader(consumer_name.clone(), topic_path.clone())
        .await?;
    let batch = reader.read_batch().await?;

    debug!("read a messages batch");
    assert_eq!(batch.messages.len(), 1);

    let commit_marker = batch.get_commit_marker();
    let mut msg = batch.messages.into_iter().next().unwrap();
    assert_eq!(msg.get_producer_id(), producer_id);
    assert_eq!(msg.seq_no, 200);
    assert_eq!(msg.read_and_take().await?.unwrap(), "test-1".as_bytes());
    // assert_eq!(msg.get_topic_path(), topic_path);

    reader.commit(commit_marker)?;

    let start = std::time::Instant::now();
    let mut consumer_description_after_commit;
    loop {
        consumer_description_after_commit = topic_client
            .describe_consumer(
                topic_path.clone(),
                consumer_name.clone(),
                DescribeConsumerOptionsBuilder::default()
                    .include_stats(true)
                    .build()?,
            )
            .await?;
        if consumer_description_after_commit.partitions[0]
            .consumer_stats
            .committed_offset
            == 1
        {
            break;
        }
        if start.elapsed() > Duration::from_secs(10) {
            panic!("Timeout waiting for committed_offset == 1");
        }
    }

    debug!(
        "consumer description: {:?}",
        consumer_description_after_commit
    );

    assert_eq!(
        consumer_description_after_commit.partitions[0]
            .consumer_stats
            .committed_offset,
        1
    );
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn read_topic_message_in_transaction() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "tx_test_topic".to_string();
    let topic_path = format!("{database_path}/{topic_name}");
    let producer_id = "test-producer-id-tx".to_string();
    let consumer_name = "test-consumer-tx".to_string();

    let mut topic_client = client.topic_client();
    let _ = topic_client.drop_topic(topic_path.clone()).await;
    debug!("previous topic removed");

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
                .consumers(vec![
                    ConsumerBuilder::default()
                        .name(consumer_name.clone())
                        .build()?,
                ])
                .build()?,
        )
        .await?;

    debug!("topic created");

    let writer_manual = topic_client
        .create_writer_with_params(
            TopicWriterOptions::builder()
                .auto_seq_no(false)
                .topic_path(topic_path.clone())
                .producer_id(producer_id.clone())
                .build(),
        )
        .await?;
    debug!("writer created");

    let expected_messages = vec![
        (1, "test-tx-message-1"),
        (2, "test-tx-message-2"),
        (3, "test-tx-message-3"),
    ];

    for (seq_no, content) in &expected_messages {
        writer_manual
            .write_with_ack(
                TopicWriterMessage::builder()
                    .seq_no(*seq_no)
                    .data(content.as_bytes().into())
                    .build(),
            )
            .await?;
        debug!("sent message with seq_no: {}, content: {}", seq_no, content);
    }

    let consumer_description_before_commit = topic_client
        .describe_consumer(
            topic_path.clone(),
            consumer_name.clone(),
            DescribeConsumerOptionsBuilder::default()
                .include_stats(true)
                .build()?,
        )
        .await?;

    assert_eq!(
        consumer_description_before_commit.partitions[0]
            .consumer_stats
            .committed_offset,
        0,
        "Consumer should have committed_offset=0 before reading messages"
    );

    info!("creating topic reader");
    let mut reader = topic_client
        .create_reader(consumer_name.clone(), topic_path.clone())
        .await?;

    let mut received_messages = Vec::new();

    client
        .query_client()
        .retry_tx(closure!([&mut reader, &mut received_messages], async |tx: &mut Transaction| {
            let mut local_received_messages = Vec::new();
            let mut message_counter = 0;
            const EXPECTED_MESSAGE_COUNT: usize = 3;

            while message_counter < EXPECTED_MESSAGE_COUNT {
                debug!(
                    "Reading batch in transaction, current counter: {}/{}",
                    message_counter, EXPECTED_MESSAGE_COUNT
                );

                let batch = timeout(
                    Duration::from_secs(10),
                    reader.pop_batch_in_tx(tx),
                )
                .await
                .map_err(|_| {
                    YdbError::Custom(format!(
                        "Timeout waiting for topic message batch. Expected {EXPECTED_MESSAGE_COUNT} messages, received {message_counter} so far"
                    ))
                })??;

                debug!(
                    "read a messages batch in transaction with {} messages",
                    batch.messages.len()
                );

                for msg in batch.messages {
                    local_received_messages.push(msg);
                    message_counter += 1;
                    if message_counter >= EXPECTED_MESSAGE_COUNT {
                        break;
                    }
                }
            }

            *received_messages = local_received_messages;

            Ok(())
        }))
        .await?;

    assert_eq!(
        received_messages.len(),
        expected_messages.len(),
        "Should receive exactly {} messages, but got {}",
        expected_messages.len(),
        received_messages.len()
    );

    for (i, mut received_msg) in received_messages.into_iter().enumerate() {
        let (expected_seq_no, expected_content) = &expected_messages[i];

        assert_eq!(received_msg.seq_no, *expected_seq_no);
        let received_data = received_msg
            .read_and_take()
            .await?
            .expect("Message should contain data");
        let received_content = String::from_utf8(received_data).expect("valid UTF-8");
        assert_eq!(received_content, *expected_content);
        assert_eq!(received_msg.get_producer_id(), producer_id);
        assert_eq!(received_msg.get_topic(), topic_name);
    }

    let consumer_description_after_commit = topic_client
        .describe_consumer(
            topic_path.clone(),
            consumer_name.clone(),
            DescribeConsumerOptionsBuilder::default()
                .include_stats(true)
                .build()?,
        )
        .await?;

    assert_eq!(
        consumer_description_after_commit.partitions[0]
            .consumer_stats
            .committed_offset,
        3,
        "Consumer should have committed_offset=3 after reading and committing 3 messages"
    );

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn write_to_specific_partition() -> YdbResult<()> {
    use std::collections::HashMap;

    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "write_to_specific_partition".to_string();
    let topic_path = format!("{database_path}/{topic_name}");
    let consumer_name = "test-consumer".to_string();

    let mut topic_client = client.topic_client();
    let _ = topic_client.drop_topic(topic_path.clone()).await;

    'wait_topic_dropped: loop {
        let mut scheme = client.scheme_client();
        let res = scheme.list_directory(database_path.clone()).await?;
        if !res.iter().any(|item| item.name == topic_name) {
            break 'wait_topic_dropped;
        }
        info!("waiting previous topic dropped...");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .min_active_partitions(2)
                .consumers(vec![
                    ConsumerBuilder::default()
                        .name(consumer_name.clone())
                        .build()?,
                ])
                .build()?,
        )
        .await?;

    let description = topic_client
        .describe_topic(
            topic_path.clone(),
            DescribeTopicOptionsBuilder::default().build()?,
        )
        .await?;
    assert_eq!(
        description.partitions.len(),
        2,
        "topic must have 2 partitions"
    );

    // Write one tagged message to each target partition via explicit PartitionId strategy.
    for target_partition in [0i64, 1i64] {
        let payload = format!("msg-for-partition-{target_partition}");
        let writer = topic_client
            .create_writer_with_params(
                TopicWriterOptions::builder()
                    .topic_path(topic_path.clone())
                    .producer_id(format!("producer-p{target_partition}"))
                    .partitioning(PartitioningStrategy::PartitionId(target_partition))
                    .build(),
            )
            .await?;

        writer
            .write_with_ack(
                TopicWriterMessage::builder()
                    .data(payload.clone().into_bytes())
                    .build(),
            )
            .await?;
        writer.stop().await?;
        debug!("wrote {} and stopped writer", payload);
    }

    // Read both messages back. A single reader session may need multiple batches
    // to receive messages from both partitions.
    let mut reader = topic_client
        .create_reader(consumer_name.clone(), topic_path.clone())
        .await?;

    let mut observed: HashMap<String, i64> = HashMap::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while observed.len() < 2 {
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for messages from both partitions, observed: {observed:?}");
        }
        let batch = reader.read_batch().await?;
        for mut msg in batch.messages {
            let partition_id = msg.get_partition_id();
            let data = msg.read_and_take().await?.unwrap();
            let text = String::from_utf8(data).unwrap();
            debug!("received {} from partition {}", text, partition_id);
            observed.insert(text, partition_id);
        }
    }

    assert_eq!(
        observed.get("msg-for-partition-0").copied(),
        Some(0),
        "message targeted to partition 0 must be read from partition 0, observed: {observed:?}",
    );
    assert_eq!(
        observed.get("msg-for-partition-1").copied(),
        Some(1),
        "message targeted to partition 1 must be read from partition 1, observed: {observed:?}",
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[traced_test]
#[ignore] // need YDB access
async fn read_batch_merges_and_respects_hard_limit() -> YdbResult<()> {
    use crate::TopicReaderOptions;

    timeout(Duration::from_secs(10), async {
        let client = create_client().await?;
        let database_path = client.database();
        let topic_name = "read_batch_merges_and_respects_hard_limit".to_string();
        let topic_path = format!("{database_path}/{topic_name}");
        let producer_id = "test-producer".to_string();
        let consumer_name = "test-consumer".to_string();

        let mut topic_client = client.topic_client();
        let _ = topic_client.drop_topic(topic_path.clone()).await;

        'wait_topic_dropped: loop {
            let mut scheme = client.scheme_client();
            let res = scheme.list_directory(database_path.clone()).await?;
            if !res.iter().any(|item| item.name == topic_name) {
                break 'wait_topic_dropped;
            }
            info!("waiting previous topic dropped...");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        topic_client
            .create_topic(
                topic_path.clone(),
                CreateTopicOptionsBuilder::default()
                    .min_active_partitions(1)
                    .consumers(vec![
                        ConsumerBuilder::default()
                            .name(consumer_name.clone())
                            .build()?,
                    ])
                    .build()?,
            )
            .await?;

        const TOTAL: usize = 10;
        const BATCH_SIZE: usize = 3;

        let writer = topic_client
            .create_writer_with_params(
                TopicWriterOptions::builder()
                    .topic_path(topic_path.clone())
                    .producer_id(producer_id.clone())
                    .build(),
            )
            .await?;
        for i in 0..TOTAL {
            writer
                .write(
                    TopicWriterMessage::builder()
                        .data(format!("msg-{i}").into_bytes())
                        .build(),
                )
                .await?;
        }
        writer.stop().await?;

        let options = TopicReaderOptions::builder()
            .consumer(consumer_name.clone())
            .topic(topic_path.clone())
            .batch_size(BATCH_SIZE)
            .build();

        let mut reader = topic_client.create_reader_with_params(options).await?;

        // Let the background receive_loop fill the buffer before reading.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut total_read = 0usize;
        let mut any_merge = false;
        let mut last_offset: Option<i64> = None;

        while total_read < TOTAL {
            let batch = reader.read_batch().await?;

            assert!(
                batch.messages.len() <= BATCH_SIZE,
                "hard limit violated: got {} > {}",
                batch.messages.len(),
                BATCH_SIZE
            );
            if batch.messages.len() > 1 {
                any_merge = true;
            }
            for msg in &batch.messages {
                if let Some(prev) = last_offset {
                    assert!(
                        msg.offset > prev,
                        "offsets must be monotonically increasing"
                    );
                }
                last_offset = Some(msg.offset);
            }
            total_read += batch.messages.len();
        }

        assert_eq!(total_read, TOTAL);
        assert!(any_merge, "expected at least one batch with >1 messages");

        Ok(())
    })
    .await
    .map_err(|_| YdbError::Custom("test timed out after 10s".to_string()))?
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn topic_writer_reconnects() -> YdbResult<()> {
    const MSGS_BEFORE_OUTAGE: usize = 5;
    const MSGS_AFTER_OUTAGE: usize = 5;
    const MSG_COUNT: usize = MSGS_BEFORE_OUTAGE + MSGS_AFTER_OUTAGE;

    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "topic_writer_reconnects".to_string();
    let topic_path = format!("{database_path}/{topic_name}");
    let producer_id = "test-producer".to_string();
    let consumer_name = "test-consumer".to_string();
    let mut topic_client = client.topic_client();

    let payloads: Vec<Vec<u8>> = (0..MSG_COUNT)
        .map(|i| format!("topic_writer_reconnect:{i}").into_bytes())
        .collect();

    let _ = topic_client.drop_topic(topic_path.clone()).await;
    'wait_topic_dropped: loop {
        let mut scheme = client.scheme_client();
        let res = scheme.list_directory(database_path.clone()).await?;
        if !res.iter().any(|item| item.name == topic_name) {
            break 'wait_topic_dropped;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .consumers(vec![
                    ConsumerBuilder::default()
                        .name(consumer_name.clone())
                        .build()?,
                ])
                .build()?,
        )
        .await?;

    let proxy = TcpForwardProxy::start(CONNECTION_STRING.as_str()).await?;
    let proxy_listen_port = proxy.listen_addr().port();

    let connection_url = url::Url::parse(CONNECTION_STRING.as_str()).map_err(|err| {
        YdbError::custom(format!(
            "topic_writer_reconnects: failed to parse CONNECTION_STRING: {err}"
        ))
    })?;
    let scheme = connection_url.scheme();

    let discovery = StaticDiscovery::new_from_str(
        format!("{scheme}://127.0.0.1:{proxy_listen_port}").as_str(),
    )?;
    let proxied_client = ClientBuilder::new_from_connection_string(format!(
        "{scheme}://127.0.0.1:{proxy_listen_port}{}",
        client.database()
    ))?
    .with_discovery(discovery)
    .client()?;
    proxied_client.wait().await?;

    let mut proxied_topic_client = proxied_client.topic_client();
    let writer = proxied_topic_client
        .create_writer_with_params(
            TopicWriterOptions::builder()
                .topic_path(topic_path.clone())
                .producer_id(producer_id.clone())
                .build(),
        )
        .await?;

    for payload in payloads.iter().take(MSGS_BEFORE_OUTAGE) {
        writer
            .write_with_ack(TopicWriterMessage::builder().data(payload.clone()).build())
            .await?;
    }

    proxy.set_allow_forward(false);
    tokio::time::sleep(Duration::from_millis(200)).await;
    proxy.set_allow_forward(true);
    tokio::time::sleep(Duration::from_millis(100)).await;

    timeout(Duration::from_secs(60), async {
        for payload in payloads
            .iter()
            .skip(MSGS_BEFORE_OUTAGE)
            .take(MSGS_AFTER_OUTAGE)
        {
            writer
                .write_with_ack(TopicWriterMessage::builder().data(payload.clone()).build())
                .await?;
        }
        YdbResult::Ok(())
    })
    .await
    .map_err(|_| {
        YdbError::custom("topic_writer_reconnects: timed out waiting for writes after reconnect")
    })??;

    writer.stop().await?;

    let mut reader = topic_client
        .create_reader(consumer_name.clone(), topic_path.clone())
        .await?;

    let read_deadline = Instant::now() + Duration::from_secs(30);
    let mut expected_index = 0usize;
    while expected_index < MSG_COUNT && Instant::now() < read_deadline {
        let batch = reader.read_batch().await?;
        for mut msg in batch.messages {
            let body = msg.read_and_take().await?.unwrap();

            assert!(
                expected_index < MSG_COUNT,
                "unexpected extra message after {MSG_COUNT} payloads: {body:?}"
            );
            assert_eq!(
                body, payloads[expected_index],
                "messages must arrive in write order (index {expected_index})"
            );
            expected_index += 1;
        }
    }

    assert_eq!(
        expected_index, MSG_COUNT,
        "timed out or truncated read: got {expected_index} of {MSG_COUNT} messages"
    );

    Ok(())
}
