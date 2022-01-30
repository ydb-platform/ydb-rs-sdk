use crate::credentials::Credentials;
use crate::errors::YdbResult;
use crate::internal::client_common::{DBCredentials, TokenCache};
use crate::internal::client_table::TableClient;
use std::sync::Arc;

use crate::internal::discovery::{Discovery, Service};
use crate::internal::grpc;
use crate::internal::load_balancer::{LoadBalancer, SharedLoadBalancer};
use crate::internal::middlewares::AuthService;

use crate::internal::grpc::create_grpc_client;
use ydb_protobuf::generated::ydb::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::generated::ydb::discovery::{
    ListEndpointsRequest, ListEndpointsResult, WhoAmIRequest, WhoAmIResult,
};

pub(crate) type Middleware = AuthService;

pub(crate) struct ClientFabric {
    credentials: DBCredentials,
    load_balancer: SharedLoadBalancer,
    discovery: Arc<Box<dyn Discovery>>,
}

impl ClientFabric {
    pub fn new(
        credentials: Box<dyn Credentials>,
        database: String,
        discovery: Box<dyn Discovery>,
    ) -> YdbResult<Self> {
        let discovery = Arc::new(discovery);
        return Ok(ClientFabric {
            credentials: DBCredentials {
                token_cache: TokenCache::new(credentials)?,
                database,
            },
            load_balancer: SharedLoadBalancer::new(discovery.as_ref()),
            discovery,
        });
    }

    pub(crate) fn table_client(&self) -> TableClient {
        return TableClient::new(self.credentials.clone(), self.discovery.clone());
    }

    pub(crate) async fn endpoints(
        self: &Self,
        req: ListEndpointsRequest,
    ) -> YdbResult<ListEndpointsResult> {
        grpc::grpc_read_operation_result(self.client_discovery().await?.list_endpoints(req).await?)
    }

    pub async fn who_am_i(self: Self, req: WhoAmIRequest) -> YdbResult<WhoAmIResult> {
        grpc::grpc_read_operation_result(self.client_discovery().await?.who_am_i(req).await?)
    }

    // clients
    async fn client_discovery(self: &Self) -> YdbResult<DiscoveryServiceClient<Middleware>> {
        return create_grpc_client(
            self.load_balancer.endpoint(Service::Discovery)?,
            self.credentials.clone(),
            DiscoveryServiceClient::new,
        )
        .await;
    }
}
