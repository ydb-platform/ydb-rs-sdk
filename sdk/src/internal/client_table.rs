use crate::errors::*;
use crate::internal::client_common::DBCredentials;
use crate::internal::client_fabric::Middleware;
use crate::internal::discovery::Service;
use crate::internal::grpc::create_grpc_client;
use crate::internal::load_balancer::{LoadBalancer, SharedLoadBalancer};
use crate::internal::session::Session;
use crate::internal::session_pool::SessionPool;
use crate::internal::transaction::{AutoCommit, Mode, Transaction};
use ydb_protobuf::generated::ydb::table::v1::table_service_client::TableServiceClient;

pub(crate) struct TableClient {
    load_balancer: SharedLoadBalancer,
    credencials: DBCredentials,
    error_on_truncate: bool,
    session_pool: SessionPool,
}

impl TableClient {
    pub(crate) fn create_autocommit_transaction(&self, mode: Mode) -> impl Transaction {
        AutoCommit::new(self.session_pool.clone(), mode)
    }

    pub(crate) async fn create_session(&mut self) -> Result<Session> {
        return self.session_pool.session().await;
    }

    fn grpc_client(&self) -> Result<TableServiceClient<Middleware>> {
        create_grpc_client(
            self.load_balancer.endpoint(Service::Table)?,
            self.credencials.clone(),
            TableServiceClient::new,
        )
    }

    pub(crate) fn new(credencials: DBCredentials, load_balancer: SharedLoadBalancer) -> Self {
        return Self {
            load_balancer: load_balancer.clone(),
            credencials: credencials.clone(),
            error_on_truncate: false,
            session_pool: SessionPool::new(load_balancer.clone(), credencials.clone()),
        };
    }

    #[allow(dead_code)]
    pub fn with_error_on_truncate(mut self, error_on_truncate: bool) -> Self {
        self.error_on_truncate = error_on_truncate;
        return self;
    }
}
