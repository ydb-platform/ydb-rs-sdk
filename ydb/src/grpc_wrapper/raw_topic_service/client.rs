use tracing::trace;

use ydb_grpc::ydb_proto::topic::v1::topic_service_client::TopicServiceClient;

use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_topic_service::create_topic::{RawCreateTopicRequest, RawCreateTopicResult};
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

    pub async fn create_topic(&mut self, req: RawCreateTopicRequest) -> RawResult<RawCreateTopicResult> {
        request_with_result!(
            self.service.create_topic,
            req => ydb_grpc::ydb_proto::topic::CreateTopicRequest,
            ydb_grpc::ydb_proto::topic::CreateTopicResult => RawCreateTopicResult
        );
    }
}
