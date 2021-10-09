use crate::credentials::Credentials;
use crate::errors::Result;
use crate::internal::client_common::DBCredentials;
use crate::internal::client_table::TableClient;
use crate::internal::discovery::StaticDiscovery;
use crate::internal::discovery::{Discovery, Service};
use crate::internal::grpc_helper;
use crate::internal::grpc_helper::create_grpc_client_old;
use crate::internal::load_balancer::{update_load_balancer, LoadBalancer, SharedLoadBalancer};
use crate::internal::middlewares::AuthService;
use crate::internal::transaction::{Mode, Transaction};
use ydb_protobuf::generated::ydb::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::generated::ydb::discovery::{
    ListEndpointsRequest, ListEndpointsResult, WhoAmIRequest, WhoAmIResult,
};

pub(crate) type Middleware = AuthService;

pub(crate) struct ClientFabric {
    credentials: DBCredentials,
    discovery: Box<dyn Discovery>,
    load_balancer: SharedLoadBalancer,
}

impl ClientFabric {
    pub fn new(
        credentials: Box<dyn Credentials>,
        database: String,
        discovery: Box<dyn Discovery>,
        load_balancer: Box<dyn LoadBalancer>,
    ) -> Result<Self> {
        let shared_load_balancer = SharedLoadBalancer::new(load_balancer);
        let background_lb = shared_load_balancer.clone();
        let discovery_sub = discovery.subscribe();
        tokio::spawn(async move { update_load_balancer(background_lb, discovery_sub) });

        return Ok(ClientFabric {
            credentials: DBCredentials {
                credentials,
                database,
            },
            discovery,
            load_balancer: shared_load_balancer,
        });
    }

    pub(crate) fn table_client(&self) -> TableClient {
        return TableClient::new(self.credentials.clone(), self.load_balancer.clone());
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
        return create_grpc_client_old(
            self.load_balancer.endpoint(Service::Discovery)?,
            self.credentials.credentials.clone(),
            self.credentials.database.clone(),
            DiscoveryServiceClient::new,
        );
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::internal::client_fabric::ClientFabric;
    use crate::internal::query::Query;
    use crate::internal::test_helpers::{CRED, DATABASE, START_ENDPOINT};
    use crate::types::YdbValue;

    use super::*;
    use crate::internal::load_balancer::RandomLoadBalancer;
    use http::Uri;
    use std::iter::FromIterator;
    use std::str::FromStr;

    fn create_client() -> Result<ClientFabric> {
        // let token = crate::credentials::StaticToken::from(std::env::var("IAM_TOKEN")?.as_str());
        // let token = crate::credentials::CommandLineYcToken::new();
        // let database = std::env::var("DB_NAME")?;
        let endpoint_uri = Uri::from_str(START_ENDPOINT.as_str())?;
        let credentials = Box::new(CRED.lock()?.clone());
        // let discovery = TimerDiscovery::new(
        //     credentials.clone(),
        //     DATABASE.clone(),
        //     START_ENDPOINT.as_str(),
        //     Duration::from_secs(60),
        // )?;
        let discovery = StaticDiscovery::from_str(START_ENDPOINT.as_str())?;
        let mut load_balancer = Box::new(RandomLoadBalancer::new());
        load_balancer
            .set_discovery_state(&*discovery.subscribe().borrow())
            .unwrap();

        return ClientFabric::new(
            credentials,
            DATABASE.clone(),
            Box::new(discovery),
            load_balancer,
        );
    }

    #[tokio::test]
    async fn create_session() -> Result<()> {
        let res = create_client()?.table_client().create_session().await?;
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
            .table_client()
            .create_autocommit_transaction(Mode::ReadOnline);
        let mut res = transaction.query("SELECT 1+1".into()).await?;
        assert_eq!(
            YdbValue::Int32(2),
            res.first()
                .unwrap()
                .rows()
                .next()
                .unwrap()
                .remove_field(0)
                .unwrap()
        );
        println!("result: {:?}", res);
        Ok(())
    }

    #[tokio::test]
    async fn execute_data_query_field_name() -> Result<()> {
        let client = create_client()?;
        let mut transaction = client
            .table_client()
            .create_autocommit_transaction(Mode::ReadOnline);
        let mut res = transaction.query("SELECT 1+1 as s".into()).await?;
        assert_eq!(
            YdbValue::Int32(2),
            res.first()
                .unwrap()
                .rows()
                .next()
                .unwrap()
                .remove_field_by_name("s")
                .unwrap()
        );
        println!("result: {:?}", res);
        Ok(())
    }

    #[tokio::test]
    async fn execute_data_query_params() -> Result<()> {
        let client = create_client()?;
        let mut transaction = client
            .table_client()
            .create_autocommit_transaction(Mode::ReadOnline);
        let mut params = HashMap::new();
        params.insert("$v".to_string(), YdbValue::Int32(3));
        let mut res = transaction
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
            YdbValue::Int32(6),
            res.first()
                .unwrap()
                .rows()
                .next()
                .unwrap()
                .remove_field(0)
                .unwrap()
        );
        Ok(())
    }

    #[tokio::test]
    async fn select_list() -> Result<()> {
        let client = create_client()?;
        let mut transaction = client
            .table_client()
            .create_autocommit_transaction(Mode::ReadOnline);
        let res = transaction
            .query(
                Query::new()
                    .with_query(
                        "
DECLARE $l AS List<Int32>;

SELECT $l AS l;
"
                        .into(),
                    )
                    .with_params(HashMap::from_iter([(
                        "$l".into(),
                        YdbValue::List(Vec::from([
                            YdbValue::Int32(1),
                            YdbValue::Int32(2),
                            YdbValue::Int32(3),
                        ])),
                    )])),
            )
            .await?;
        println!("{:?}", res);
        let res = res.results.unwrap().into_iter().next().unwrap();
        assert_eq!(1, res.columns().len());
        Ok(())
    }

    #[tokio::test]
    async fn who_am_i() -> Result<()> {
        let res = create_client()?.who_am_i(WhoAmIRequest::default()).await?;
        assert!(res.user.len() > 0);
        Ok(())
    }
}
