use futures_util::StreamExt;
use tracing_test::traced_test;

use crate::client_topic::list_types::ConsumerBuilder;
use crate::test_integration_helper::create_client;
use crate::{client_topic::client::TopicOptionsBuilder, TopicWriterMessageBuilder, TopicWriterOptionsBuilder, YdbError, YdbResult};
use tracing::{trace, warn};
use ydb_grpc::ydb_proto::topic::stream_read_message::init_request::TopicReadSettings;
use ydb_grpc::ydb_proto::topic::{stream_read_message, stream_write_message};
use ydb_grpc::ydb_proto::topic::v1::topic_service_client::TopicServiceClient;
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;

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
    let mut grpc_client = topic_client
        .raw_client_connection()
        .await?
        .get_grpc_service();

    let mut topic_messages = start_read_topic(
        grpc_client,
        consumer_name,
        topic_path,
    ).await?;

    println!("message: {:?}", topic_messages.recv().await);

    Ok(())
}

async fn start_read_topic(
    mut grpc_topic_service: TopicServiceClient<InterceptedChannel>,
    consumer: String,
    topic_path: String,
)->YdbResult<tokio::sync::mpsc::UnboundedReceiver<stream_read_message::read_response::MessageData>>{
    let (reader_stream_tx, reader_stream_rx): (
        tokio::sync::mpsc::UnboundedSender<stream_read_message::FromClient>,
        tokio::sync::mpsc::UnboundedReceiver<stream_read_message::FromClient>,
    ) = tokio::sync::mpsc::unbounded_channel();

    let init_request = stream_read_message::from_client::ClientMessage::InitRequest(
        stream_read_message::InitRequest{
            topics_read_settings: vec![
                TopicReadSettings{
                    path: topic_path,
                    ..TopicReadSettings::default()
                }
            ],
            consumer,
            ..stream_read_message::InitRequest::default()
        }
    );

    let mess = stream_read_message::FromClient{
        client_message: Some(init_request),
    };

    reader_stream_tx.send(mess).expect("failed to send init message from test topic reader");


    let request_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(reader_stream_rx);
    let mut reader_stream = grpc_topic_service.stream_read(request_stream).await?.into_inner();

    let _init_response = reader_stream.next().await.ok_or(YdbError::custom("failed receive init response in test reader"))??;

    let data_request = stream_read_message::from_client::ClientMessage::ReadRequest(
        stream_read_message::ReadRequest{
            bytes_size: 1024*1024,
            ..stream_read_message::ReadRequest::default()
        }
    );

    let mess = stream_read_message::FromClient{
        client_message: Some(data_request),
    };
    reader_stream_tx.send(mess).expect("failed to send data request in test topic reader");

    let (topic_messages_tx, topic_messages_rx): (
        tokio::sync::mpsc::UnboundedSender<stream_read_message::read_response::MessageData>,
        tokio::sync::mpsc::UnboundedReceiver<stream_read_message::read_response::MessageData>,
    ) = tokio::sync::mpsc::unbounded_channel();


    tokio::spawn(async move {
        loop {
            let mess = reader_stream.next().await;
            let mess = match mess {
                Some(Ok(mess)) => mess,
                mess => {
                    trace!("stop to receive reader stream mess in test: {:?}", mess);
                    return
                }
            };

            let mess = if let stream_read_message::FromServer{server_message: Some(mess), ..} = mess {
                mess
            } else {
                warn!("failed decode server message in test topic reader: {:?}", mess);
                return
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