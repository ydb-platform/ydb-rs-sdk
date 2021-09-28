use std::sync::Arc;

use ydb_protobuf::generated::ydb::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::generated::ydb::discovery::{
    ListEndpointsRequest, ListEndpointsResult, WhoAmIRequest, WhoAmIResult,
};
use ydb_protobuf::generated::ydb::table::v1::table_service_client::TableServiceClient;

use crate::errors::Result;
use crate::internal::discovery::Service;
use crate::internal::discovery::StaticDiscovery;
use crate::internal::grpc::ClientFabric;
use crate::internal::grpc_helper;
use crate::internal::middlewares::AuthService;
use crate::internal::session::{Session, SessionPool};
use crate::internal::transaction::{AutoCommit, Mode, Transaction};

type Middleware = AuthService;

pub(crate) struct Client<CF: ClientFabric> {
    client_fabric: Arc<CF>,
    session_pool: Box<dyn SessionPool>,
    error_on_truncate: bool,
}

impl<CF: ClientFabric> Client<CF> {
    pub fn new(grpc_client_fabric: CF, session_pool: Box<dyn SessionPool>) -> Result<Self> {
        let fabric = Arc::new(grpc_client_fabric);
        return Ok(Client {
            client_fabric: fabric,
            session_pool,
            error_on_truncate: true,
        });
    }

    #[allow(dead_code)]
    pub fn with_error_on_truncate(mut self, error_on_truncate: bool) -> Self {
        self.error_on_truncate = error_on_truncate;
        return self;
    }

    pub async fn create_autocommit_transaction(&self, mode: Mode) -> Result<AutoCommit> {
        return Ok(AutoCommit::new(self.session_pool.clone_pool(), mode)
            .with_error_on_truncate(self.error_on_truncate));
    }

    pub(crate) async fn create_session(self: &mut Self) -> Result<Session> {
        self.session_pool.session().await
    }

    pub(crate) async fn endpoints(
        self: &Self,
        req: ListEndpointsRequest,
    ) -> Result<ListEndpointsResult> {
        grpc_helper::grpc_read_result(self.client_discovery()?.list_endpoints(req).await?)
    }

    pub async fn who_am_i(self: Self, req: WhoAmIRequest) -> Result<WhoAmIResult> {
        grpc_helper::grpc_read_result(self.client_discovery()?.who_am_i(req).await?)
    }

    // clients
    fn client_discovery(self: &Self) -> Result<DiscoveryServiceClient<Middleware>> {
        return self
            .client_fabric
            .create(DiscoveryServiceClient::new, Service::Discovery);
    }
}

mod test {
    use std::collections::HashMap;

    use crate::internal::client::Client;
    use crate::internal::grpc::SimpleGrpcClientFabric;
    use crate::internal::query::Query;
    use crate::internal::session::SimpleSessionPool;
    use crate::internal::test_helpers::{CRED, DATABASE, START_ENDPOINT};
    use crate::types::YdbValue;

    use super::*;

    fn create_client() -> Result<Client<SimpleGrpcClientFabric>> {
        // let token = crate::credentials::StaticToken::from(std::env::var("IAM_TOKEN")?.as_str());
        // let token = crate::credentials::CommandLineYcToken::new();
        // let database = std::env::var("DB_NAME")?;
        let credentials = CRED.lock()?.clone();
        let discovery = StaticDiscovery::from_str(START_ENDPOINT.as_str())?;

        let grpc_client_fabric = SimpleGrpcClientFabric::new(
            Box::new(discovery),
            Box::new(credentials),
            DATABASE.clone(),
        );

        let table_client =
            grpc_client_fabric.create(TableServiceClient::new, Service::TableService)?;
        let session_pool = SimpleSessionPool::new(table_client);

        return Client::new(grpc_client_fabric, Box::new(session_pool));
    }

    #[tokio::test]
    async fn create_session() -> Result<()> {
        let res = create_client()?.create_session().await?;
        println!("session: {:?}", res);
        Ok(())
    }

    #[tokio::test]
    async fn endpoints() -> Result<()> {
        let _res = create_client()?
            .endpoints(ListEndpointsRequest::default())
            .await?;
        println!("{:?}", _res);
        Ok(())
    }

    #[tokio::test]
    async fn execute_data_query() -> Result<()> {
        let client = create_client()?;
        let mut transaction = client
            .create_autocommit_transaction(Mode::ReadOnline)
            .await?;
        let res = transaction.query("SELECT 1+1".into()).await?;
        assert_eq!(
            &YdbValue::Int32(2),
            res.first()
                .unwrap()
                .rows()
                .next()
                .unwrap()
                .get_field_index(0)
                .unwrap()
        );
        println!("result: {:?}", res);
        Ok(())
    }

    #[tokio::test]
    async fn execute_data_query_field_name() -> Result<()> {
        let client = create_client()?;
        let mut transaction = client
            .create_autocommit_transaction(Mode::ReadOnline)
            .await?;
        let res = transaction.query("SELECT 1+1 as s".into()).await?;
        assert_eq!(
            &YdbValue::Int32(2),
            res.first()
                .unwrap()
                .rows()
                .next()
                .unwrap()
                .get_field("s")
                .unwrap()
        );
        println!("result: {:?}", res);
        Ok(())
    }

    #[tokio::test]
    async fn execute_data_query_params() -> Result<()> {
        let client = create_client()?;
        let mut transaction = client
            .create_autocommit_transaction(Mode::ReadOnline)
            .await?;
        let mut params = HashMap::new();
        params.insert("$v".to_string(), YdbValue::Int32(3));
        let res = transaction
            .query(
                Query::new()
                    .with_query(
                        "
                DECLARE $v AS Int32;
                SELECT $v+$v
        "
                        .into(),
                    )
                    .with_params(params),
            )
            .await?;
        println!("result: {:?}", res);
        assert_eq!(
            &YdbValue::Int32(6),
            res.first()
                .unwrap()
                .rows()
                .next()
                .unwrap()
                .get_field_index(0)
                .unwrap()
        );
        Ok(())
    }

    #[tokio::test]
    async fn who_am_i() -> Result<()> {
        let res = create_client()?.who_am_i(WhoAmIRequest::default()).await?;
        assert!(res.user.len() > 0);
        Ok(())
    }
}
