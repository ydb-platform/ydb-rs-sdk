// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.

use crate::client_topic::common::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::client_topic::topicwriter::init_writer::RawInitResponse;
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::{grpc_wrapper, YdbError, YdbResult};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::UNIX_EPOCH;
use futures_util::{StreamExt, TryStreamExt};
use ydb_grpc::ydb_proto::topic::stream_write_message;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::{InitRequest, WriteRequest};
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::{message_data, MessageData};

#[allow(dead_code)]
pub struct TopicWriter {
    pub partition_id: i64,
    pub session_id: String,
    pub last_seq_num_received: i64,

    /* closed, closeReason, background, stream */
    stream:
        AsyncGrpcStreamWrapper<stream_write_message::FromClient, stream_write_message::FromServer>,
    pub writer_options: TopicWriterOptions,

    pub(crate) connection_manager: GrpcConnectionManager,
}

#[allow(dead_code)]
pub enum AckInfo {
    SuccessfullySent,
    Error,
}

#[allow(dead_code)]
pub struct AckFuture {}

impl Future for AckFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        unimplemented!("prototype")
    }
}

impl TopicWriter {
    pub(crate) async fn new(
        writer_options: TopicWriterOptions,
        connection_manager: GrpcConnectionManager,
    ) -> YdbResult<Self> {
        let mut topic_service = connection_manager
            .get_auth_service(grpc_wrapper::raw_topic_service::client::RawTopicClient::new)
            .await?;
        let init_request_body = InitRequest {
            path: writer_options.topic_path.clone(),
            producer_id: writer_options.producer_id.clone().unwrap(), // TODO: handle somehow
            write_session_meta: writer_options.session_metadata.clone().unwrap_or_default(),
            get_last_seq_no: true,
            partitioning: None, // TODO: pass it
        };

        let mut stream = topic_service.stream_write().await?;

        stream.send(stream_write_message::FromClient {
            client_message: Some(ClientMessage::InitRequest(init_request_body)),
        }).await?;

        let init_response = RawInitResponse::try_from(stream.receive().await.ok_or(
            YdbError::Custom("No response for writer init message".to_string()),
        )??)?;

        Ok(Self {
            partition_id: init_response.partition_id,
            session_id: init_response.session_id,
            last_seq_num_received: init_response.last_seq_no,
            stream,
            writer_options,
            connection_manager,
        })
    }

    pub async fn write(&mut self, message: TopicWriterMessage) -> YdbResult<()> {
        let vec_len = message.data.len();
        let req = WriteRequest{
            messages: vec![MessageData{
                seq_no: 3,
                created_at: Some(ydb_grpc::google_proto_workaround::protobuf::Timestamp {
                    seconds: message.created_at.duration_since(UNIX_EPOCH)?.as_secs() as i64,
                    nanos: message.created_at.duration_since(UNIX_EPOCH)?.as_nanos() as i32,
                }),
                data: message.data,
                uncompressed_size: vec_len as i64,
                partitioning: Some(message_data::Partitioning::MessageGroupId(self.writer_options.producer_id.clone().unwrap_or_default())),
            }],
            codec: 1
        };

        self.stream.send(stream_write_message::FromClient{
            client_message: Some(ClientMessage::WriteRequest(req))
        }).await?;


        Ok(())
    }

    pub async fn write_with_ack(&self, _message: TopicWriterMessage) -> YdbResult<AckInfo> {
        unimplemented!("prototype")
    }

    pub async fn write_with_ack_future(
        &self,
        _message: TopicWriterMessage,
    ) -> YdbResult<AckFuture> {
        unimplemented!("prototype")
    }

    pub async fn flush(&self) -> YdbResult<()> {
        unimplemented!("prototype")
    }

    async fn connection(
        &self,
    ) -> YdbResult<grpc_wrapper::raw_topic_service::client::RawTopicClient> {
        self.connection_manager
            .get_auth_service(grpc_wrapper::raw_topic_service::client::RawTopicClient::new) // TODO: maybe just call it one time at init
            .await
    }
}
