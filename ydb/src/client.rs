use crate::client_common::DBCredentials;
use crate::client_coordination::client::CoordinationClient;
use crate::client_operation::OperationClient;
use crate::client_query::{QueryClient, QuerySessionPoolSettings};
use crate::client_scheme::client::SchemeClient;
use crate::client_table::TableClient;
use crate::discovery::Discovery;
use crate::errors::YdbResult;
use crate::load_balancer::SharedLoadBalancer;
use crate::session_pool::QuerySessionPool;
use crate::waiter::Waiter;

use std::sync::Arc;
use std::time::Duration;

use crate::client_topic::client::TopicClient;
use crate::client_topic::compression::{default_executor, Executor};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use tracing::trace;

/// YDB client
pub struct Client {
    credentials: DBCredentials,
    load_balancer: SharedLoadBalancer,
    discovery: Arc<Box<dyn Discovery>>,
    timeouts: TimeoutSettings,
    connection_manager: GrpcConnectionManager,
    executor: Arc<dyn Executor>,
    session_pool: Option<QuerySessionPool>,
}

impl Client {
    pub(crate) fn new(
        credentials: DBCredentials,
        discovery: Arc<Box<dyn Discovery>>,
        connection_manager: GrpcConnectionManager,
        load_balancer: SharedLoadBalancer,
        executor: Option<Arc<dyn Executor>>,
    ) -> YdbResult<Self> {
        let executor = match executor {
            Some(e) => e,
            None => default_executor()?,
        };

        Ok(Client {
            credentials,
            load_balancer,
            discovery,
            timeouts: TimeoutSettings::default(),
            connection_manager,
            executor,
            session_pool: None,
        })
    }

    /// Configure a shared session pool (CreateSession + AttachSession) for table and query clients.
    pub async fn with_session_pool(
        self,
        settings: QuerySessionPoolSettings,
    ) -> YdbResult<Self> {
        let pool = QuerySessionPool::new_explicit(
            self.connection_manager.clone(),
            self.timeouts,
            self.discovery.clone(),
            settings,
        )
        .await?;
        Ok(Self {
            session_pool: Some(pool),
            ..self
        })
    }

    /// Pool stats when [`Self::with_session_pool`] was configured.
    pub fn session_pool_stats(&self) -> Option<crate::session_pool::QuerySessionPoolStats> {
        self.session_pool.as_ref().map(|pool| pool.stats())
    }

    pub fn database(&self) -> String {
        self.credentials.database.clone()
    }

    /// Create instance of client for table service
    pub fn table_client(&self) -> TableClient {
        TableClient::new(
            self.connection_manager.clone(),
            self.timeouts,
            self.discovery.clone(),
            self.session_pool.clone(),
        )
    }

    /// Create instance of client for query service.
    pub fn query_client(&self) -> QueryClient {
        let client = QueryClient::new(
            self.connection_manager.clone(),
            self.timeouts,
            self.discovery.clone(),
        );
        if let Some(pool) = &self.session_pool {
            client.with_shared_session_pool(pool.clone())
        } else {
            client
        }
    }

    /// Create instance of client for directory service
    pub fn scheme_client(&self) -> SchemeClient {
        SchemeClient::new(self.timeouts, self.connection_manager.clone())
    }

    /// Create instance of client for topic service
    pub fn topic_client(&self) -> TopicClient {
        TopicClient::new(
            self.timeouts,
            self.connection_manager.clone(),
            self.credentials.token_cache.clone(),
            self.executor.clone(),
        )
    }

    /// Create instance of client for coordination service
    pub fn coordination_client(&self) -> CoordinationClient {
        CoordinationClient::new(self.timeouts, self.connection_manager.clone())
    }

    /// Create instance of client for operation service (list/get/forget long-running operations).
    pub fn operation_client(&self) -> OperationClient {
        OperationClient::new(self.timeouts, self.connection_manager.clone())
    }

    pub fn with_timeouts(mut self, timeouts: TimeoutSettings) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Wait initialization completed
    ///
    /// Wait all background process get first successfully result and client fully
    /// available to work.
    pub async fn wait(&self) -> YdbResult<()> {
        trace!("waiting_token");
        self.credentials.token_cache.wait().await?;
        trace!("wait discovery");
        self.discovery.wait().await?;

        trace!("wait balancer");
        self.load_balancer.wait().await?;
        Ok(())
    }
}

#[cfg(test)]
impl Client {
    pub(crate) fn connection_manager_for_test(&self) -> GrpcConnectionManager {
        self.connection_manager.clone()
    }
}

const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(600);

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
#[derive(Copy, Clone, Debug)]
pub struct TimeoutSettings {
    pub operation_timeout: Duration,
}

impl TimeoutSettings {
    pub(crate) fn operation_params(&self) -> RawOperationParams {
        RawOperationParams::new_with_timeouts(self.operation_timeout, self.operation_timeout)
    }

    pub(crate) fn execute_script_operation_params(&self) -> RawOperationParams {
        RawOperationParams::for_execute_script(self.operation_timeout, self.operation_timeout)
    }
}

impl Default for TimeoutSettings {
    fn default() -> Self {
        TimeoutSettings {
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
        }
    }
}
