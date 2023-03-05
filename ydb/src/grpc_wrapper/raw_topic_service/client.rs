use ydb_grpc::ydb_proto::topic::v1::topic_service_client::TopicServiceClient;
use tracing::trace;

use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::raw_topic_service::create_topic::{RawCreateTopicRequest};
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

    pub async fn delete_topic(&mut self, req: RawDropTopicRequest) -> RawResult<()>{
        request_without_result!(
            self.service.drop_topic,
            req => ydb_grpc::ydb_proto::topic::DropTopicRequest
        );
    }
}

impl GrpcServiceForDiscovery for RawTopicClient{
    fn get_grpc_discovery_service() -> Service {
        Service::Topic
    }
}
