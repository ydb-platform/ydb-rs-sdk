use crate::credentials::{credencials_ref, CredentialsRef};
use crate::errors::YdbResult;
use crate::internal::client_common::{DBCredentials, TokenCache};
use crate::internal::client_table::TableClient;
use crate::internal::discovery::{Discovery, Service, TimerDiscovery};
use crate::internal::grpc;
use crate::internal::grpc::create_grpc_client;
use crate::internal::load_balancer::{LoadBalancer, SharedLoadBalancer};
use crate::internal::middlewares::AuthService;
use crate::internal::waiter::Waiter;
use crate::{Credentials, StaticToken};
use std::sync::Arc;
use std::time::Duration;
use tracing::trace;
use ydb_protobuf::ydb_proto::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::ydb_proto::discovery::{
    ListEndpointsRequest, ListEndpointsResult, WhoAmIRequest, WhoAmIResult,
};

pub(crate) type Middleware = AuthService;

pub struct Client {
    credentials: DBCredentials,
    load_balancer: SharedLoadBalancer,
    discovery: Arc<Box<dyn Discovery>>,
}

impl Client {
    pub(crate) fn new_internal(
        credentials: DBCredentials,
        discovery: Box<dyn Discovery>,
    ) -> YdbResult<Self> {
        let discovery = Arc::new(discovery);

        return Ok(Client {
            credentials,
            load_balancer: SharedLoadBalancer::new(discovery.as_ref()),
            discovery,
        });
    }

    // wait about all background process get first succesfull result and client fully
    // available to work
    pub async fn wait(&self) -> YdbResult<()> {
        trace!("waiting_token");
        self.credentials.token_cache.wait().await?;
        trace!("wait discovery");
        self.discovery.wait().await?;

        trace!("wait balancer");
        self.load_balancer.wait().await?;
        return Ok(());
    }

    pub fn table_client(&self) -> TableClient {
        return TableClient::new(self.credentials.clone(), self.discovery.clone());
    }

    pub(crate) async fn endpoints(self: &Self) -> YdbResult<ListEndpointsResult> {
        let req = ListEndpointsRequest {
            database: self.credentials.database.clone(),
            ..ListEndpointsRequest::default()
        };
        grpc::grpc_read_operation_result(self.client_discovery().await?.list_endpoints(req).await?)
    }

    pub(crate) async fn who_am_i(&self, req: WhoAmIRequest) -> YdbResult<WhoAmIResult> {
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
