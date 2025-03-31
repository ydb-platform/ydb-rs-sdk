use tracing::trace;
use ydb_grpc::ydb_proto::topic::stream_write_message;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage;

use ydb_grpc::ydb_proto::topic::stream_write_message::InitRequest;
use ydb_grpc::ydb_proto::topic::v1::topic_service_client::TopicServiceClient;

use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::raw_topic_service::alter_topic::RawAlterTopicRequest;
use crate::grpc_wrapper::raw_topic_service::create_topic::RawCreateTopicRequest;
use crate::grpc_wrapper::raw_topic_service::describe_topic::{
    RawDescribeTopicRequest, RawDescribeTopicResult,
};
use crate::grpc_wrapper::raw_topic_service::drop_topic::RawDropTopicRequest;
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

    pub async fn alter_topic(&mut self, req: RawAlterTopicRequest) -> RawResult<()> {
        request_without_result!(
            self.service.alter_topic,
            req => ydb_grpc::ydb_proto::topic::AlterTopicRequest
        );
    }

    pub async fn describe_topic(
        &mut self,
        req: RawDescribeTopicRequest,
    ) -> RawResult<RawDescribeTopicResult> {
        request_with_result!(
            self.service.describe_topic,
            req => ydb_grpc::ydb_proto::topic::DescribeTopicRequest,
            ydb_grpc::ydb_proto::topic::DescribeTopicResult => RawDescribeTopicResult
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
        init_req_body: InitRequest,
    ) -> RawResult<
        AsyncGrpcStreamWrapper<stream_write_message::FromClient, stream_write_message::FromServer>,
    > {
        let (tx, rx): (
            tokio::sync::mpsc::UnboundedSender<stream_write_message::FromClient>,
            tokio::sync::mpsc::UnboundedReceiver<stream_write_message::FromClient>,
        ) = tokio::sync::mpsc::unbounded_channel();

        let mess = stream_write_message::FromClient {
            client_message: Some(ClientMessage::InitRequest(init_req_body)),
        };
        tx.send(mess)?;

        let request_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        let stream_writer_result = self.service.stream_write(request_stream).await;
        let response_stream = stream_writer_result?.into_inner();

        Ok(AsyncGrpcStreamWrapper::<
            stream_write_message::FromClient,
            stream_write_message::FromServer,
        >::new(tx, response_stream)) // pass tx instead of mock_tx in case of proper solution
    }

    // use for tests only, while reader not ready
    pub(crate) fn get_grpc_service(&self) -> TopicServiceClient<InterceptedChannel> {
        self.service.clone()
    }
}

impl GrpcServiceForDiscovery for RawTopicClient {
    fn get_grpc_discovery_service() -> Service {
        Service::Topic
    }
}
