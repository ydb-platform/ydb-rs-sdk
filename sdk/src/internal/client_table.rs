use crate::errors::*;
use crate::internal::client_common::DBCredentials;
use crate::internal::client_fabric::Middleware;
use crate::internal::discovery::Service;

use crate::internal::load_balancer::{SharedLoadBalancer};
use crate::internal::session::Session;
use crate::internal::session_pool::SessionPool;
use crate::internal::transaction::{AutoCommit, Mode, SerializableReadWriteTx, Transaction};
use ydb_protobuf::generated::ydb::table::v1::table_service_client::TableServiceClient;
use crate::internal::channel_pool::ChannelPool;
use async_trait::async_trait;

pub(crate) struct TableClient {
    error_on_truncate: bool,
    session_pool: SessionPool,
    channel_pool: ChannelPool<TableServiceClient<Middleware>>,
}

impl TableClient {
    pub(crate) fn new(credencials: DBCredentials, load_balancer: SharedLoadBalancer) -> Self {
        let channel_pool =ChannelPool::new::<TableServiceClient<Middleware>>(load_balancer.clone(), credencials.clone(), Service::Table, TableServiceClient::new);

        return Self {
            error_on_truncate: false,
            session_pool: SessionPool::new(Box::new(channel_pool.clone())),
            channel_pool,
        };
    }

    #[allow(dead_code)]
    pub(crate) fn with_max_active_session(mut self, size: usize)->Self {
        self.session_pool = self.session_pool.with_max_active_sessions(size);
        return self;
    }

    pub fn create_autocommit_transaction(&self, mode: Mode) -> impl Transaction {
        AutoCommit::new(self.channel_pool.clone(), self.session_pool.clone(), mode).with_error_on_truncate(self.error_on_truncate)
    }

    pub fn create_multiquery_transaction(&self) -> impl Transaction {
        SerializableReadWriteTx::new(self.channel_pool.clone(), self.session_pool.clone()).with_error_on_truncate(self.error_on_truncate)
    }

    pub(crate) async fn create_session(&mut self) -> Result<Session> {
        return self.session_pool.session().await;
    }

    #[allow(dead_code)]
    pub fn with_error_on_truncate(mut self, error_on_truncate: bool) -> Self {
        self.error_on_truncate = error_on_truncate;
        return self;
    }
}
