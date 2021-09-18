use ydb_protobuf::generated::ydb::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::generated::ydb::discovery::{
    ListEndpointsRequest, ListEndpointsResult, WhoAmIRequest, WhoAmIResult,
};
use ydb_protobuf::generated::ydb::table::v1::table_service_client::TableServiceClient;
use ydb_protobuf::generated::ydb::table::CreateSessionRequest;

use crate::errors::Result;
use crate::internal::discovery::StaticDiscovery;
use crate::internal::grpc;
use crate::internal::grpc::ClientFabric;
use crate::internal::middlewares::AuthService;
use crate::internal::session::{Session, SessionPool};
use std::sync::Arc;

type Middleware = AuthService;

pub(crate) struct Client<CF: ClientFabric> {
    client_fabric: Arc<CF>,
    session_pool: Box<dyn SessionPool>,
}

impl<CF: ClientFabric> Client<CF> {
    pub fn new(grpc_client_fabric: CF, session_pool: Box<dyn SessionPool>) -> Result<Self> {
        let fabric = Arc::new(grpc_client_fabric);
        return Ok(Client {
            client_fabric: fabric,
            session_pool,
        });
    }

    pub(crate) async fn create_session(
        self: &mut Self,
        req: CreateSessionRequest,
    ) -> Result<Session> {
        self.session_pool
            .session(self.client_fabric.create(TableServiceClient::new)?, req)
            .await
    }

    pub(crate) async fn endpoints(
        self: &Self,
        req: ListEndpointsRequest,
    ) -> Result<ListEndpointsResult> {
        grpc::grpc_read_result(self.client_discovery()?.list_endpoints(req).await?)
    }

    pub async fn who_am_i(self: Self, req: WhoAmIRequest) -> Result<WhoAmIResult> {
        grpc::grpc_read_result(self.client_discovery()?.who_am_i(req).await?)
    }

    // clients
    fn client_discovery(self: &Self) -> Result<DiscoveryServiceClient<Middleware>> {
        return self.client_fabric.create(DiscoveryServiceClient::new);
    }
}

mod test {
    use super::*;
    use std::sync::Mutex;

    use once_cell::sync::Lazy;

    use ydb_protobuf::generated::ydb::table::transaction_control::TxSelector;
    use ydb_protobuf::generated::ydb::table::transaction_settings::TxMode;
    use ydb_protobuf::generated::ydb::table::{
        ExecuteDataQueryRequest, OnlineModeSettings, TransactionControl, TransactionSettings,
    };

    use crate::credentials::CommandLineYcToken;
    use crate::internal::client::Client;
    use crate::internal::grpc::SimpleGrpcClient;
    use crate::internal::session::SimpleSessionPool;
    use std::time::Duration;

    static CRED: Lazy<Mutex<CommandLineYcToken>> =
        Lazy::new(|| Mutex::new(crate::credentials::CommandLineYcToken::new()));

    fn create_client() -> Result<Client<SimpleGrpcClient>> {
        // let token = crate::credentials::StaticToken::from(std::env::var("IAM_TOKEN")?.as_str());
        // let token = crate::credentials::CommandLineYcToken::new();
        let database = std::env::var("DB_NAME")?;
        let credentials = CRED.lock()?.clone();
        let discovery = StaticDiscovery {
            endpoint: std::env::var("DB_ENDPOINT")?,
        };

        let grpc_client_fabric =
            SimpleGrpcClient::new(Box::new(discovery), Box::new(credentials), database);

        let session_pool = SimpleSessionPool::new();

        return Client::new(grpc_client_fabric, Box::new(session_pool));
    }

    #[tokio::test]
    async fn create_session() -> Result<()> {
        let mut res = create_client()?
            .create_session(CreateSessionRequest::default())
            .await?;
        println!("session: {:?}", res);
        drop(res);
        tokio::time::sleep(Duration::from_secs(1)).await;
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
        let mut session = client
            .create_session(CreateSessionRequest::default())
            .await?;
        println!("session: {:?}", session);
        let req = ExecuteDataQueryRequest {
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
            ..ExecuteDataQueryRequest::default()
        };
        let res = session.execute(req).await?;
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
