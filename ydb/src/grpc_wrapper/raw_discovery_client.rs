use crate::grpc_wrapper::grpc::grpc_read_operation_result;
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use crate::grpc_wrapper::grpc_limits::WithGrpcMaxMessageSize;
use crate::YdbResult;
use itertools::Itertools;
use ydb_grpc::ydb_proto::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_grpc::ydb_proto::discovery::{ListEndpointsRequest, ListEndpointsResult};

pub struct GrpcDiscoveryClient {
    service: DiscoveryServiceClient<InterceptedChannel>,
}

impl WithGrpcMaxMessageSize for GrpcDiscoveryClient {
    fn with_grpc_max_message_size(mut self, bytes: usize) -> Self {
        self.service = self
            .service
            .max_decoding_message_size(bytes)
            .max_encoding_message_size(bytes);
        self
    }
}

impl GrpcDiscoveryClient {
    pub(crate) fn new(channel: InterceptedChannel) -> Self {
        Self {
            service: DiscoveryServiceClient::new(channel),
        }
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn list_endpoints(
        &mut self,
        database: String,
    ) -> YdbResult<Vec<EndpointInfo>> {
        let req = ListEndpointsRequest {
            database,
            ..ListEndpointsRequest::default()
        };
        let resp = self.service.list_endpoints(req).await?;
        let result: ListEndpointsResult = grpc_read_operation_result(resp)?;

        let res = result
            .endpoints
            .into_iter()
            .map(|item| EndpointInfo {
                fqdn: item.address,
                port: item.port,
                ssl: item.ssl,
                location: item.location,
            })
            .collect_vec();
        Ok(res)
    }
}

pub(crate) struct EndpointInfo {
    pub(crate) fqdn: String,
    pub(crate) port: u32,
    pub(crate) ssl: bool,
    pub(crate) location: String,
}

impl GrpcServiceForDiscovery for GrpcDiscoveryClient {
    fn get_grpc_discovery_service() -> Service {
        Service::Discovery
    }
}
