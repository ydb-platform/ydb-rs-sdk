use futures_util::stream::iter;
use std::thread;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::trace;
use ydb_grpc::ydb_proto::topic::stream_write_message;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::{FromClient, InitRequest};
use ydb_grpc::ydb_proto::topic::v1::topic_service_client::TopicServiceClient;

use crate::client_topic::common::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::raw_topic_service::create_topic::RawCreateTopicRequest;
use crate::grpc_wrapper::raw_topic_service::delete_topic::RawDropTopicRequest;
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;

pub(crate) struct RawTopicClient {
    service: TopicServiceClient<InterceptedChannel>,
}

impl RawTopicClient {
    pub fn new(service: InterceptedChannel) -> Self {
        Self {
            service: TopicServiceClient::new(service),
        }
    }

    pub async fn create_topic(&mut self, req: RawCreateTopicRequest) -> RawResult<()> {
        request_without_result!(
            self.service.create_topic,
            req => ydb_grpc::ydb_proto::topic::CreateTopicRequest
        );
    }

    pub async fn delete_topic(&mut self, req: RawDropTopicRequest) -> RawResult<()> {
        request_without_result!(
            self.service.drop_topic,
            req => ydb_grpc::ydb_proto::topic::DropTopicRequest
        );
    }

    pub async fn stream_write(
        &mut self,
    ) -> RawResult<
        AsyncGrpcStreamWrapper<stream_write_message::FromClient, stream_write_message::FromServer>,
    > {
        let (tx, rx): (
            tokio::sync::mpsc::UnboundedSender<stream_write_message::FromClient>,
            tokio::sync::mpsc::UnboundedReceiver<stream_write_message::FromClient>,
        ) = tokio::sync::mpsc::unbounded_channel();

        /* let (mock_tx, _): (
            tokio::sync::mpsc::UnboundedSender<stream_write_message::FromClient>,
            tokio::sync::mpsc::UnboundedReceiver<stream_write_message::FromClient>,
        ) = tokio::sync::mpsc::unbounded_channel();

         drop(tx); */ // uncomment these lines to make it reach println!("Successful initialization");

        let request_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        let response_stream = self
            .service
            .stream_write(request_stream)
            .await?
            .into_inner();

        println!("Successful initialization");

        Ok(AsyncGrpcStreamWrapper::<
            stream_write_message::FromClient,
            stream_write_message::FromServer,
        >::new(mock_tx, response_stream)) // pass tx instead of mock_tx in case of proper solution

        /*bidirectional_streaming_request!(
            self.service.stream_write,
            stream_write_message::FromClient,
            stream_write_message::FromServer
        );*/
    }

    /*
    pub async fn do_write_handshake(&mut self, writer_options: TopicWriterOptions) -> RawResult<RawInitResponse> {
        Ok(RawInitResponse {
            last_seq_no: 0,
            session_id: "".to_string(),
            partition_id: 0,
            supported_codecs: Default::default(),
        })
    }

    pub async fn do_single_write_request(&mut self, msg: TopicWriterMessage, writer_options: &TopicWriterOptions) -> RawResult<()> {
        let vec_len = msg.data.len();
        let req = WriteRequest {
            messages: vec![MessageData {
                seq_no: 3,
                created_at: Some(ydb_grpc::google_proto_workaround::protobuf::Timestamp {
                    seconds: msg.created_at.duration_since(UNIX_EPOCH)?.as_secs() as i64,
                    nanos: msg.created_at.duration_since(UNIX_EPOCH)?.as_nanos() as i32,
                }),
                data: msg.data,
                uncompressed_size: vec_len as i64,
                partitioning: Some(message_data::Partitioning::MessageGroupId(writer_options.producer_id.clone().unwrap_or_default())),
            }],
            codec: 1,
        };

        let mut result = self.service.stream_write(iter(vec![
            FromClient
            {
                client_message:
                Some(ClientMessage::WriteRequest(req))
            }])).await?;

        let _ignored = result.get_mut().message().await?;

        let msg = result.get_mut().message().await?;
        let issue = msg.unwrap().issues[0].clone();
        println!("{}", issue.message);

        Ok(())
    } */
}

impl GrpcServiceForDiscovery for RawTopicClient {
    fn get_grpc_discovery_service() -> Service {
        Service::Topic
    }
}
