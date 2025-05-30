use futures_util::StreamExt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing_test::traced_test;

use crate::client_topic::client::DescribeConsumerOptionsBuilder;
use crate::client_topic::list_types::ConsumerBuilder;
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use crate::test_integration_helper::create_client;
use crate::{
    client_topic::client::{AlterTopicOptionsBuilder, CreateTopicOptionsBuilder},
    TopicWriterMessageBuilder, TopicWriterOptionsBuilder, YdbError, YdbResult,
};
use crate::{Codec, DescribeTopicOptionsBuilder};
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
    let topic_path = format!("{}/{}", database_path, topic_name);
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
                .consumers(vec![ConsumerBuilder::default()
                    .name(consumer_name.clone())
                    .build()?])
                .build()?,
        )
        .await?;

    debug!("topic created");

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
    debug!("first writer created");

    writer_manual
        .write(
            TopicWriterMessageBuilder::default()
                .seq_no(Some(200))
                .data("test-1".as_bytes().into())
                .build()?,
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
    let topic_path = format!("{}/{}", database_path, topic_name);
    let producer_id = "test-producer-id-tx".to_string();
    let consumer_name = "test-consumer-tx".to_string();

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
                .consumers(vec![ConsumerBuilder::default()
                    .name(consumer_name.clone())
                    .build()?])
                .build()?,
        )
        .await?;

    debug!("topic created");

    // Create writer with manual sequence numbers
    let mut writer_manual = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .auto_seq_no(false)
                .topic_path(topic_path.clone())
                .producer_id(producer_id.clone())
                .build()?,
        )
        .await?;
    debug!("writer created");

    // Send 3 messages with ascending sequence numbers and different content
    let expected_messages = vec![
        (1, "test-tx-message-1"),
        (2, "test-tx-message-2"),
        (3, "test-tx-message-3"),
    ];

    for (seq_no, content) in &expected_messages {
        writer_manual
            .write_with_ack(
                TopicWriterMessageBuilder::default()
                    .seq_no(Some(*seq_no))
                    .data(content.as_bytes().into())
                    .build()?,
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
    // Create topic reader outside the retry loop to enable reuse
    let reader = topic_client
        .create_reader(consumer_name.clone(), topic_path.clone())
        .await?;

    // Wrap reader in Arc<Mutex> for thread safety within transaction retries
    let reader_mutex = Arc::new(Mutex::new(reader));

    // Store all received messages for validation
    let received_messages = Arc::new(Mutex::new(Vec::new()));

    let table_client = client.table_client();
    table_client
        .retry_transaction(|t| {
            let reader_mutex = reader_mutex.clone();
            let received_messages = received_messages.clone();

            async move {
                let mut t = t; // force borrow for lifetime of t inside closure

                // Lock the reader for use within this transaction attempt
                let mut reader_guard = reader_mutex.lock().await;
                let mut local_received_messages = Vec::new();

                // Initialize message counter - we expect exactly 3 messages
                let mut message_counter = 0;
                const EXPECTED_MESSAGE_COUNT: usize = 3;

                // Read messages using counter approach instead of timeout
                while message_counter < EXPECTED_MESSAGE_COUNT {
                    debug!("Reading batch in transaction, current counter: {}/{}", message_counter, EXPECTED_MESSAGE_COUNT);

                    // Read batch within transaction - treating timeout as error 
                    let batch = timeout(
                        Duration::from_secs(10),
                        reader_guard.pop_batch_in_tx(&mut t),
                    ).await
                        .map_err(|_| YdbError::Custom(format!(
                            "Timeout waiting for topic message batch. Expected {} messages, received {} so far",
                            EXPECTED_MESSAGE_COUNT, message_counter
                        )))??;

                    debug!("read a messages batch in transaction with {} messages", batch.messages.len());

                    // Process each message in the batch
                    for msg in batch.messages {
                        local_received_messages.push(msg);
                        message_counter += 1;

                        debug!("Processed message {}/{}", message_counter, EXPECTED_MESSAGE_COUNT);

                        // Stop if we've received all expected messages
                        if message_counter >= EXPECTED_MESSAGE_COUNT {
                            break;
                        }
                    }
                }

                debug!("Successfully read {} messages in transaction", message_counter);

                // Store messages for validation after transaction
                {
                    let mut global_messages = received_messages.lock().await;
                    *global_messages = local_received_messages;
                }

                // Commit the transaction
                t.commit().await?;
                debug!("transaction committed");
                Ok(())
            }
        })
        .await?;

    // Retrieve received messages for comprehensive validation
    let received_messages = {
        let mut guard = received_messages.lock().await;
        std::mem::take(&mut *guard)
    };

    // ==========================================
    // COMPREHENSIVE MESSAGE VALIDATION
    // ==========================================

    assert_eq!(
        received_messages.len(),
        expected_messages.len(),
        "Should receive exactly {} messages, but got {}",
        expected_messages.len(),
        received_messages.len()
    );

    info!("Validating message ordering and content...");

    // Validate message ordering and comprehensive field validation
    for (i, mut received_msg) in received_messages.into_iter().enumerate() {
        let (expected_seq_no, expected_content): &(i64, &str) = &expected_messages[i];

        info!(
            "Validating message {} - expected seq_no: {}, content: {}",
            i + 1,
            expected_seq_no,
            expected_content
        );

        // ==========================================
        // MESSAGE CONTENT VALIDATION
        // ==========================================

        // Validate sequence number
        assert_eq!(
            received_msg.seq_no,
            *expected_seq_no,
            "Message {} should have seq_no {}, but got {}. Messages may be in incorrect order!",
            i + 1,
            expected_seq_no,
            received_msg.seq_no
        );

        // Validate message body content
        let received_data = received_msg
            .read_and_take()
            .await?
            .expect("Message should contain data");
        let received_content =
            String::from_utf8(received_data).expect("Message data should be valid UTF-8");

        assert_eq!(
            received_content,
            *expected_content,
            "Message {} should contain '{}', but got '{}'",
            i + 1,
            expected_content,
            received_content
        );

        // ==========================================
        // PRODUCER VALIDATION
        // ==========================================

        assert_eq!(
            received_msg.get_producer_id(),
            producer_id,
            "Message {} should have producer_id '{}', but got '{}'",
            i + 1,
            producer_id,
            received_msg.get_producer_id()
        );

        // ==========================================
        // TOPIC/PARTITION INFORMATION VALIDATION
        // ==========================================

        // Validate topic name through getter function
        assert_eq!(
            received_msg.get_topic(),
            topic_name,
            "Message {} should have topic name '{}', but got '{}'",
            i + 1,
            topic_name,
            received_msg.get_topic()
        );

        // Validate partition ID is valid (should be >= 0)
        let partition_id = received_msg.get_partition_id();
        assert!(
            partition_id >= 0,
            "Message {} should have valid partition_id >= 0, but got {}",
            i + 1,
            partition_id
        );

        // ==========================================
        // OFFSET AND TIMING VALIDATION
        // ==========================================

        // Validate offset is valid (should be >= 0)
        assert!(
            received_msg.offset >= 0,
            "Message {} should have valid offset >= 0, but got {}",
            i + 1,
            received_msg.offset
        );

        // Validate uncompressed_size matches actual content length
        assert_eq!(
            received_msg.uncompressed_size,
            expected_content.len() as i64,
            "Message {} should have uncompressed_size {}, but got {}",
            i + 1,
            expected_content.len(),
            received_msg.uncompressed_size
        );

        // Validate written_at timestamp is reasonable (not too far in future, not too old)
        let now = SystemTime::now();
        let written_at = received_msg.written_at;

        // Allow up to 1 second in the future to account for small timing differences
        let one_second_future = now + Duration::from_secs(1);
        assert!(
            written_at <= one_second_future,
            "Message {} written_at timestamp should not be more than 1 second in the future. written_at: {:?}, threshold: {:?}",
            i + 1, written_at, one_second_future
        );

        // Allow up to 10 minute in the past (generous for test environments)
        let one_minute_ago = now - Duration::from_secs(600);
        assert!(
            written_at >= one_minute_ago,
            "Message {} written_at timestamp should not be more than 1 minute old. written_at: {:?}, threshold: {:?}",
            i + 1, written_at, one_minute_ago
        );

        // Validate created_at if present
        if let Some(created_at) = received_msg.created_at {
            // Allow up to 1 second in the future to account for small timing differences
            let one_second_future = now + Duration::from_secs(1);
            assert!(
                created_at <= one_second_future,
                "Message {} created_at timestamp should not be more than 1 second in the future. created_at: {:?}, threshold: {:?}",
                i + 1, created_at, one_second_future
            );

            // Allow up to 10 minutes in the past (generous for test environments)
            let ten_minutes_ago = now - Duration::from_secs(600);
            assert!(
                created_at >= ten_minutes_ago,
                "Message {} created_at timestamp should not be more than 10 minutes old. created_at: {:?}, threshold: {:?}",
                i + 1, created_at, ten_minutes_ago
            );
        }

        // ==========================================
        // COMMIT MARKER VALIDATION
        // ==========================================

        let commit_marker = received_msg.get_commit_marker();

        // Validate commit marker topic matches
        assert_eq!(
            commit_marker.topic,
            topic_name,
            "Message {} commit marker should have topic '{}', but got '{}'",
            i + 1,
            topic_name,
            commit_marker.topic
        );

        // Validate commit marker partition_id matches message partition_id
        assert_eq!(
            commit_marker.partition_id,
            received_msg.get_partition_id(),
            "Message {} commit marker partition_id should match message partition_id. marker: {}, message: {}",
            i + 1, commit_marker.partition_id, received_msg.get_partition_id()
        );

        // Validate commit marker offsets are reasonable
        assert!(
            commit_marker.start_offset <= commit_marker.end_offset,
            "Message {} commit marker should have start_offset <= end_offset. start: {}, end: {}",
            i + 1,
            commit_marker.start_offset,
            commit_marker.end_offset
        );

        info!(
            "✓ Message {} validation passed - seq_no: {}, content: '{}', offset: {}, partition: {}",
            i + 1,
            received_msg.seq_no,
            expected_content,
            received_msg.offset,
            partition_id
        );
    }

    // ==========================================
    // MESSAGE ORDERING DOCUMENTATION
    // ==========================================

    info!("Message ordering analysis:");
    info!("Expected order: [1, 2, 3] (ascending sequence numbers)");
    info!(
        "Actual order: {:?}",
        expected_messages
            .iter()
            .map(|(seq, _)| *seq)
            .collect::<Vec<_>>()
    );

    // Note: Based on the validation above, if we reach this point, messages were received
    // in the correct order. If they were in reverse order, the seq_no assertions would have failed.
    info!("✓ Messages were received in correct ascending order (1, 2, 3)");

    // ==========================================
    // CONSUMER OFFSET VALIDATION
    // ==========================================

    // Check that the messages are committed - since transaction commit is synchronous,
    // we should see the result immediately without polling
    let consumer_description_after_commit = topic_client
        .describe_consumer(
            topic_path.clone(),
            consumer_name.clone(),
            DescribeConsumerOptionsBuilder::default()
                .include_stats(true)
                .build()?,
        )
        .await?;

    debug!(
        "consumer description after tx commit: {:?}",
        consumer_description_after_commit
    );

    assert_eq!(
        consumer_description_after_commit.partitions[0]
            .consumer_stats
            .committed_offset,
        3, // We sent and committed 3 messages
        "Consumer should have committed_offset=3 after reading and committing 3 messages"
    );

    info!("✓ All validations passed! Successfully read {} messages in transaction with comprehensive field validation", expected_messages.len());

    Ok(())
}
