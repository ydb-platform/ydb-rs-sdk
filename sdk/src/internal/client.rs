use ydb_protobuf::generated::ydb::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::generated::ydb::discovery::{
    ListEndpointsRequest, ListEndpointsResult, WhoAmIRequest, WhoAmIResult,
};
use ydb_protobuf::generated::ydb::table::v1::table_service_client::TableServiceClient;
use ydb_protobuf::generated::ydb::table::{
    CreateSessionRequest, CreateSessionResult, ExecuteDataQueryRequest, ExecuteQueryResult,
};

use crate::errors::Result;
use crate::internal::discovery::StaticDiscovery;
use crate::internal::grpc;
use crate::internal::grpc::ClientFabric;
use crate::internal::middlewares::AuthService;

type Middleware = AuthService;

pub(crate) struct Client<CF: ClientFabric> {
    client_fabric: CF,
}

impl<CF: ClientFabric> Client<CF> {
    pub fn new(grpc_client_fabric: CF) -> Result<Self> {
        return Ok(Client {
            client_fabric: grpc_client_fabric,
        });
    }

    // usable functions
    pub(crate) async fn create_session(
        self: &Self,
        client: &mut TableServiceClient<AuthService>,
        req: CreateSessionRequest,
    ) -> Result<CreateSessionResult> {
        grpc::grpc_read_result(client.create_session(req).await?)
    }

    pub(crate) async fn endpoints(
        self: &Self,
        req: ListEndpointsRequest,
    ) -> Result<ListEndpointsResult> {
        grpc::grpc_read_result(self.client_discovery()?.list_endpoints(req).await?)
    }

    pub(crate) async fn execute(
        self: &Self,
        client: &mut TableServiceClient<AuthService>,
        req: ExecuteDataQueryRequest,
    ) -> Result<ExecuteQueryResult> {
        grpc::grpc_read_result(client.execute_data_query(req).await?)
    }

    pub async fn who_am_i(self: Self, req: WhoAmIRequest) -> Result<WhoAmIResult> {
        grpc::grpc_read_result(self.client_discovery()?.who_am_i(req).await?)
    }

    // clients
    fn client_discovery(self: &Self) -> Result<DiscoveryServiceClient<Middleware>> {
        return self.client_fabric.create(DiscoveryServiceClient::new);
    }

    pub(crate) fn client_table(self: &mut Self) -> Result<TableServiceClient<Middleware>> {
        return self.client_fabric.create(TableServiceClient::new);
    }
}

mod test {
    use super::*;
    use std::ops::Deref;
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

        return Client::new(grpc_client_fabric);
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
