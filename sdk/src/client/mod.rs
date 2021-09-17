mod internal;
mod trait_operation;

use crate::client::internal::AuthService;
use crate::credentials::Credencials;
use crate::errors::Result;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tower::ServiceBuilder;
use ydb_protobuf::generated::ydb::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::generated::ydb::discovery::{
    EndpointInfo, ListEndpointsRequest, ListEndpointsResult, WhoAmIRequest, WhoAmIResult,
};
use ydb_protobuf::generated::ydb::table::v1::table_service_client::TableServiceClient;
use ydb_protobuf::generated::ydb::table::{
    CreateSessionRequest, CreateSessionResult, ExecuteDataQueryRequest, ExecuteQueryResult,
};

pub struct Client {
    start_endpoint: EndpointInfo,
    cred: Box<dyn Credencials>,
    database: String,

    // state
    channel: Option<Channel>,
}

impl Client {
    pub fn new(
        start_endpoint: EndpointInfo,
        cred: Box<dyn Credencials>,
        database: &str,
    ) -> Result<Self> {
        return Ok(Client {
            start_endpoint,
            cred,
            database: database.to_string(),

            channel: None,
        });
    }

    // usable functions
    pub(crate) async fn create_session(
        self: &mut Self,
        client: &mut TableServiceClient<AuthService>,
        req: CreateSessionRequest,
    ) -> Result<CreateSessionResult> {
        internal::grpc_read_result(client.create_session(req).await?)
    }

    pub(crate) async fn execute(
        self: &mut Self,
        client: &mut TableServiceClient<AuthService>,
        req: ExecuteDataQueryRequest,
    ) -> Result<ExecuteQueryResult> {
        internal::grpc_read_result(client.execute_data_query(req).await?)
    }

    pub async fn endpoints(
        self: &mut Self,
        req: ListEndpointsRequest,
    ) -> Result<ListEndpointsResult> {
        internal::grpc_read_result(self.client_discovery()?.list_endpoints(req).await?)
    }

    pub async fn who_am_i(self: &mut Self, req: WhoAmIRequest) -> Result<WhoAmIResult> {
        internal::grpc_read_result(self.client_discovery()?.who_am_i(req).await?)
    }

    // clients
    fn client_discovery(self: &mut Self) -> Result<DiscoveryServiceClient<AuthService>> {
        return self.create_grpc_client(DiscoveryServiceClient::new);
    }

    pub(crate) fn client_table(self: &mut Self) -> Result<TableServiceClient<AuthService>> {
        return self.create_grpc_client(TableServiceClient::new);
    }

    // helpers for clients
    fn channel(self: &mut Self, endpoint_info: &EndpointInfo) -> Result<Channel> {
        if let Some(ch) = &self.channel {
            return Ok(ch.clone());
        }

        let uri = http::uri::Uri::builder()
            .scheme(if endpoint_info.ssl { "https" } else { "http" })
            .authority(format!("{}:{}", endpoint_info.address, endpoint_info.port).as_bytes())
            .path_and_query("")
            .build()?;

        let channel = Endpoint::from(uri)
            .tls_config(ClientTlsConfig::new())?
            .connect_lazy()?;

        self.channel = Some(channel.clone());
        return Ok(channel);
    }

    fn create_grpc_client<T, CB>(self: &mut Self, new_func: CB) -> Result<T>
    where
        CB: FnOnce(AuthService) -> T,
    {
        let cred = self.cred.clone();
        let database = self.database.clone();
        let auth_service_create = |ch| {
            return AuthService::new(ch, cred.clone(), database.as_str());
        };

        let channel = self.channel(&self.start_endpoint.clone())?;

        let auth_ch = ServiceBuilder::new()
            .layer_fn(auth_service_create)
            .service(channel);

        return Ok(new_func(auth_ch));
    }
}

mod test {
    use super::*;
    use crate::credentials::CommandLineYcToken;
    use once_cell::sync::Lazy;
    use std::ops::Deref;
    use std::sync::Mutex;
    use ydb_protobuf::generated::ydb::table::transaction_control::TxSelector;
    use ydb_protobuf::generated::ydb::table::transaction_settings::TxMode;
    use ydb_protobuf::generated::ydb::table::{
        OnlineModeSettings, TransactionControl, TransactionSettings,
    };

    static CRED: Lazy<Mutex<CommandLineYcToken>> =
        Lazy::new(|| Mutex::new(crate::credentials::CommandLineYcToken::new()));

    fn create_client() -> Result<Client> {
        // let token = crate::credentials::StaticToken::from(std::env::var("IAM_TOKEN")?.as_str());
        // let token = crate::credentials::CommandLineYcToken::new();
        let database = std::env::var("DB_NAME")?;
        let token = CRED.lock()?.deref().clone();
        let token = Box::new(token);

        return Client::new(
            EndpointInfo {
                address: std::env::var("DB_ENDPOINT")?,
                port: 2135,
                load_factor: 0.0,
                ssl: true,
                service: vec![],
                location: "".to_string(),
                node_id: 0,
                ..EndpointInfo::default()
            },
            token,
            database.as_str(),
        );
    }

    #[tokio::test]
    async fn create_session() -> Result<()> {
        let mut client = create_client()?;
        let res = create_client()?
            .create_session(&mut client.client_table()?, CreateSessionRequest::default())
            .await?;
        println!("session: {:?}", res);
        Ok(())
    }

    #[tokio::test]
    async fn endpoints() -> Result<()> {
        let _res = create_client()?
            .endpoints(ListEndpointsRequest::default())
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn execute_data_query() -> Result<()> {
        let mut client = create_client()?;
        let mut table_client = client.client_table()?;
        let session = client
            .create_session(&mut table_client, CreateSessionRequest::default())
            .await?;
        println!("session: {:?}", session);
        let req = ExecuteDataQueryRequest {
            session_id: session.session_id,
            tx_control: Some(TransactionControl {
                commit_tx: true,
                tx_selector: Some(TxSelector::BeginTx(TransactionSettings {
                    tx_mode: Some(TxMode::OnlineReadOnly(OnlineModeSettings {
                        allow_inconsistent_reads: true,
                    })),
                })),
            }),
            query: Some(ydb_protobuf::generated::ydb::table::Query {
                query: Some(ydb_protobuf::generated::ydb::table::query::Query::YqlText(
                    "SELECT 1+1".to_string(),
                )),
            }),
            parameters: Default::default(),
            query_cache_policy: None,
            operation_params: None,
            collect_stats: 0,
        };
        let res = client.execute(&mut table_client, req).await?;
        println!("session: {:?}", res);
        Ok(())
    }

    #[tokio::test]
    async fn who_am_i() -> Result<()> {
        let res = create_client()?.who_am_i(WhoAmIRequest::default()).await?;
        assert!(res.user.len() > 0);
        Ok(())
    }
}
