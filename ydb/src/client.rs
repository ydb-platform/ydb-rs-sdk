use crate::RetrySettings;
use crate::client_common::DBCredentials;
use crate::client_coordination::client::CoordinationClient;
use crate::client_operation::OperationClient;
use crate::client_query::QueryClient;
use crate::client_scheme::client::SchemeClient;
use crate::client_table::TableClient;
use crate::discovery::Discovery;
use crate::errors::YdbResult;
use crate::load_balancer::SharedLoadBalancer;
use crate::session_pool::{SessionPool, default_session_pool_settings};
pub use crate::session_pool::{SessionPoolSettings, SessionPoolStats};
use crate::waiter::Waiter;

use std::sync::Arc;
use std::time::Duration;

use crate::client_topic::client::TopicClient;
use crate::client_topic::compression::{Executor, default_executor};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use tracing::instrument;
use tracing::trace;

/// YDB client.
///
/// The built-in session pool defaults to a limit of **50** concurrent sessions (shared by
/// table and query clients). The legacy table-only pool used **1000**; use
/// [`Self::with_session_pool`] with an explicit limit when migrating high-concurrency workloads.
pub struct Client {
    credentials: DBCredentials,
    load_balancer: SharedLoadBalancer,
    discovery: Arc<dyn Discovery>,
    connection_manager: GrpcConnectionManager,
    executor: Arc<dyn Executor>,
    session_pool: SessionPool,
    retry_settings: RetrySettings,
}

impl Client {
    pub(crate) fn new(
        credentials: DBCredentials,
        discovery: Arc<dyn Discovery>,
        connection_manager: GrpcConnectionManager,
        load_balancer: SharedLoadBalancer,
        executor: Option<Arc<dyn Executor>>,
        retry_settings: RetrySettings,
    ) -> YdbResult<Self> {
        let executor = match executor {
            Some(e) => e,
            None => default_executor()?,
        };

        let session_pool = SessionPool::new_explicit_sync(
            connection_manager.clone(),
            discovery.clone(),
            default_session_pool_settings(),
        );

        Ok(Client {
            credentials,
            load_balancer,
            discovery,
            connection_manager,
            executor,
            session_pool,
            retry_settings,
        })
    }

    /// Return a child driver that shares sessions and connections but uses a different retry budget.
    ///
    /// All service clients created from the returned [`Client`] consult `budget` before each retry
    /// (table, query one-shot, [`crate::QueryClient::retry_tx`], operation service, and similar).
    pub fn clone_with_retry_settings(&self, retry_settings: RetrySettings) -> Self {
        Self {
            credentials: self.credentials.clone(),
            load_balancer: self.load_balancer.clone(),
            discovery: self.discovery.clone(),
            connection_manager: self.connection_manager.clone(),
            executor: self.executor.clone(),
            session_pool: self.session_pool.clone(),
            retry_settings,
        }
    }

    /// Replace the driver session pool (CreateSession + AttachSession) and optionally warm it up.
    ///
    /// Table and query clients created from this driver share the same pool.
    ///
    #[instrument(name = "ydb.Driver.WithSessionPool", skip_all, fields(db.system.name = "ydb", db.namespace = %self.credentials.database), err)]
    pub async fn with_session_pool(self, settings: SessionPoolSettings) -> YdbResult<Self> {
        let session_pool = SessionPool::new_explicit(
            self.connection_manager.clone(),
            self.discovery.clone(),
            settings,
        )
        .await?;
        Ok(Self {
            session_pool,
            ..self
        })
    }

    /// Session pool counters for the driver (shared by table and query clients).
    pub fn session_pool_stats(&self) -> SessionPoolStats {
        self.session_pool.stats()
    }

    pub fn database(&self) -> String {
        self.credentials.database.clone()
    }

    /// Create instance of client for table service
    #[instrument(name = "ydb.Driver.TableClient", skip_all, fields(db.system.name = "ydb", db.namespace = %self.credentials.database))]
    pub fn table_client(&self) -> TableClient {
        TableClient::new(
            self.connection_manager.clone(),
            self.session_pool.clone(),
            self.retry_settings.clone(),
        )
    }

    /// Create instance of client for query service.
    #[instrument(name = "ydb.Driver.QueryClient", skip_all, fields(db.system.name = "ydb", db.namespace = %self.credentials.database))]
    pub fn query_client(&self) -> QueryClient {
        QueryClient::new(
            self.connection_manager.clone(),
            self.session_pool.clone(),
            self.retry_settings.clone(),
        )
    }

    /// Create instance of client for directory service
    #[instrument(name = "ydb.Driver.SchemeClient", skip_all, fields(db.system.name = "ydb", db.namespace = %self.credentials.database))]
    pub fn scheme_client(&self) -> SchemeClient {
        SchemeClient::new(self.connection_manager.clone())
    }

    /// Create instance of client for topic service
    #[instrument(name = "ydb.Driver.TopicClient", skip_all, fields(db.system.name = "ydb", db.namespace = %self.credentials.database))]
    pub fn topic_client(&self) -> TopicClient {
        TopicClient::new(
            self.connection_manager.clone(),
            self.credentials.token_cache.clone(),
            self.executor.clone(),
        )
    }

    /// Create instance of client for coordination service
    #[instrument(name = "ydb.Driver.CoordinationClient", skip_all, fields(db.system.name = "ydb", db.namespace = %self.credentials.database))]
    pub fn coordination_client(&self) -> CoordinationClient {
        CoordinationClient::new(self.connection_manager.clone())
    }

    /// Create instance of client for operation service (list/get/forget long-running operations).
    #[instrument(name = "ydb.Driver.OperationClient", skip_all, fields(db.system.name = "ydb", db.namespace = %self.credentials.database))]
    pub fn operation_client(&self) -> OperationClient {
        OperationClient::new(self.connection_manager.clone(), self.retry_settings.clone())
    }

    /// Wait initialization completed
    ///
    /// Wait all background process get first successfully result and client fully
    /// available to work.
    #[instrument(name = "ydb.Driver.Initialize", skip_all, fields(db.system.name = "ydb", db.namespace = %self.credentials.database), err)]
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

const DEFAULT_OPERATION_TIMEOUT: Option<Duration> = None;

#[derive(Copy, Clone, Debug)]
pub(crate) struct TimeoutSettings {
    pub operation_timeout: Option<Duration>,
}

impl TimeoutSettings {
    pub(crate) fn operation_params(&self) -> RawOperationParams {
        match self.operation_timeout {
            Some(timeout) => RawOperationParams::new_with_timeouts(timeout, timeout),
            None => RawOperationParams::sync_unlimited(),
        }
    }

    pub(crate) fn execute_script_operation_params(&self) -> RawOperationParams {
        match self.operation_timeout {
            Some(timeout) => RawOperationParams::for_execute_script(timeout, timeout),
            None => RawOperationParams::for_execute_script_unlimited(),
        }
    }
}

impl Default for TimeoutSettings {
    fn default() -> Self {
        TimeoutSettings {
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
        }
    }
}
