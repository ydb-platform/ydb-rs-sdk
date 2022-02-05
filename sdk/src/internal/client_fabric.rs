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
use ydb_protobuf::generated::ydb::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::generated::ydb::discovery::{
    ListEndpointsRequest, ListEndpointsResult, WhoAmIRequest, WhoAmIResult,
};

pub(crate) type Middleware = AuthService;

pub struct ClientBuilder {
    credentials: CredentialsRef,
    database: String,
    discovery_interval: Duration,
    endpoint: String,
}

impl ClientBuilder {
    pub fn build(self) -> YdbResult<Client> {
        let db_cred = DBCredentials {
            token_cache: TokenCache::new(self.credentials.clone())?,
            database: self.database.clone(),
        };

        let discovery = TimerDiscovery::new(
            db_cred.clone(),
            self.endpoint.as_str(),
            self.discovery_interval,
        )?;

        return Client::new_internal(db_cred, Box::new(discovery));
    }

    pub fn new() -> Self {
        Self {
            credentials: credencials_ref(StaticToken::from("")),
            database: "/local".to_string(),
            discovery_interval: Duration::from_secs(60),
            endpoint: "grpc://localhost:2135".to_string(),
        }
    }

    pub fn with_credentials<T: 'static + Credentials>(mut self, cred: T) -> Self {
        self.credentials = credencials_ref(cred);
        return self;
    }

    pub fn with_database(mut self, database: String) -> Self {
        self.database = database;
        return self;
    }

    pub fn with_endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = endpoint;
        return self;
    }
}

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
        self.credentials.token_cache.wait().await?;
        self.discovery.wait().await?;
        return Ok(());
    }

    pub fn table_client(&self) -> TableClient {
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
