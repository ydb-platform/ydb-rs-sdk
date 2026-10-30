use crate::RetrySettings;
use crate::client_common::DBCredentials;
use crate::client_coordination::client::CoordinationClient;
use crate::client_operation::OperationClient;
use crate::client_query::QueryClient;
use crate::client_scheme::client::SchemeClient;
use crate::client_table::TableClient;
use crate::discovery::Discovery;
use crate::driver_lifecycle::DriverLifecycle;
use crate::errors::YdbResult;
use crate::load_balancer::SharedLoadBalancer;
use crate::session_pool::SessionPool;
pub use crate::session_pool::{SessionPoolSettings, SessionPoolStats};
use crate::waiter::Waiter;

use std::sync::Arc;
use std::time::Duration;

use crate::client_metrics::names::MetricsNames;
use crate::client_topic::client::TopicClient;
use crate::client_topic::compression::{Executor, default_executor};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use tracing::{instrument, trace};

/// YDB client.
///
/// The built-in session pool defaults to a limit of **50** concurrent sessions (shared by
/// table and query clients). The legacy table-only pool used **1000**; use
/// [`crate::ClientBuilder::with_session_pool`] with an explicit limit when migrating
/// high-concurrency workloads.
pub struct Client {
    credentials: DBCredentials,
    connection_manager: GrpcConnectionManager,
    executor: Arc<dyn Executor>,
    session_pool: SessionPool,
    retry_settings: RetrySettings,
    metrics_names: MetricsNames,
    lifecycle: DriverLifecycle,
}

pub(crate) struct ClientInitSettings {
    pub executor: Option<Arc<dyn Executor>>,
    pub retry_settings: RetrySettings,
    pub metrics_names: MetricsNames,
    pub session_pool_settings: SessionPoolSettings,
}

impl Client {
    pub(crate) async fn init(
        credentials: DBCredentials,
        discovery: Arc<dyn Discovery>,
        connection_manager: GrpcConnectionManager,
        load_balancer: SharedLoadBalancer,
        settings: ClientInitSettings,
    ) -> YdbResult<Self> {
        let executor = match settings.executor {
            Some(e) => e,
            None => default_executor()?,
        };

        let session_pool = SessionPool::new_explicit_sync(
            connection_manager.clone(),
            discovery.clone(),
            settings.session_pool_settings,
        );

        Self::wait_until_ready(&credentials, &discovery, &load_balancer).await?;
        session_pool.warm_up().await?;

        Ok(Client {
            credentials,
            connection_manager,
            executor,
            session_pool,
            retry_settings: settings.retry_settings,
            metrics_names: settings.metrics_names,
            lifecycle: DriverLifecycle::new(),
        })
    }

    /// Replace the driver-wide retry budget.
    ///
    /// Service clients created afterward use these settings for table, query, operation, and
    /// similar retries.
    pub fn with_retry_settings(mut self, retry_settings: RetrySettings) -> Self {
        self.retry_settings = retry_settings;
        self
    }

    /// Session pool counters for the driver (shared by table and query clients).
    pub fn session_pool_stats(&self) -> SessionPoolStats {
        self.session_pool.stats()
    }

    /// Stop the shared session pool and wait for accepted session cleanup attempts to finish.
    ///
    /// This consumes the driver and rejects new work through service clients sharing its shutdown
    /// state. Existing sessions, transactions, streams, readers, writers, and coordination
    /// sessions remain usable so they can finish or stop. Shutdown waits for session leases before
    /// deleting idle sessions. Topic readers, writers, and coordination sessions are not awaited;
    /// shutdown reports their final counts if they remain.
    /// Shutdown must run while the Tokio runtime that created the driver is still alive.
    #[instrument(name = "ydb.Driver.Shutdown", skip_all, fields(db.system.name = "ydb", db.namespace = %self.credentials.database), err)]
    pub async fn shutdown(mut self) -> YdbResult<()> {
        self.lifecycle.start_shutdown();
        self.session_pool.shutdown().await?;
        self.lifecycle.complete_shutdown();

        let live_resources = self.lifecycle.live_resources();
        if live_resources.is_empty() {
            Ok(())
        } else {
            Err(crate::YdbError::custom(format!(
                "client shutdown completed with live resources: {live_resources}"
            )))
        }
    }

    pub fn database(&self) -> String {
        self.credentials.database.clone()
    }

    /// Create instance of client for table service
    #[instrument(name = "ydb.Driver.TableClient", skip_all, fields(db.system.name = "ydb", db.namespace = %self.credentials.database))]
    pub fn table_client(&self) -> TableClient {
        self.metrics_names
            .client_new_table_client_counter
            .increment(1);
        TableClient::new(
            self.connection_manager.clone(),
            self.session_pool.clone(),
            self.retry_settings.clone(),
            &self.lifecycle,
        )
    }

    /// Create instance of client for query service.
    #[instrument(name = "ydb.Driver.QueryClient", skip_all, fields(db.system.name = "ydb", db.namespace = %self.credentials.database))]
    pub fn query_client(&self) -> QueryClient {
        self.metrics_names
            .client_new_query_client_counter
            .increment(1);
        QueryClient::new(
            self.connection_manager.clone(),
            self.session_pool.clone(),
            self.retry_settings.clone(),
            self.metrics_names.clone(),
            &self.lifecycle,
        )
    }

    /// Create instance of client for directory service
    #[instrument(name = "ydb.Driver.SchemeClient", skip_all, fields(db.system.name = "ydb", db.namespace = %self.credentials.database))]
    pub fn scheme_client(&self) -> SchemeClient {
        self.metrics_names
            .client_new_scheme_client_counter
            .increment(1);
        SchemeClient::new(self.connection_manager.clone(), &self.lifecycle)
    }

    /// Create instance of client for topic service
    #[instrument(name = "ydb.Driver.TopicClient", skip_all, fields(db.system.name = "ydb", db.namespace = %self.credentials.database))]
    pub fn topic_client(&self) -> TopicClient {
        self.metrics_names
            .client_new_topic_client_counter
            .increment(1);
        TopicClient::new(
            self.connection_manager.clone(),
            self.credentials.token_cache.clone(),
            self.executor.clone(),
            &self.lifecycle,
        )
    }

    /// Create instance of client for coordination service
    #[instrument(name = "ydb.Driver.CoordinationClient", skip_all, fields(db.system.name = "ydb", db.namespace = %self.credentials.database))]
    pub fn coordination_client(&self) -> CoordinationClient {
        CoordinationClient::new(self.connection_manager.clone(), &self.lifecycle)
    }

    /// Create instance of client for operation service (list/get/forget long-running operations).
    #[instrument(name = "ydb.Driver.OperationClient", skip_all, fields(db.system.name = "ydb", db.namespace = %self.credentials.database))]
    pub fn operation_client(&self) -> OperationClient {
        OperationClient::new(
            self.connection_manager.clone(),
            self.retry_settings.clone(),
            &self.lifecycle,
        )
    }

    /// Wait initialization completed
    ///
    /// Wait all background process get first successfully result and client fully
    /// available to work.
    #[instrument(name = "ydb.Driver.Initialize", skip_all, fields(db.system.name = "ydb", db.namespace = %credentials.database), err)]
    async fn wait_until_ready(
        credentials: &DBCredentials,
        discovery: &Arc<dyn Discovery>,
        load_balancer: &SharedLoadBalancer,
    ) -> YdbResult<()> {
        trace!("waiting_token");
        credentials.token_cache.wait().await?;
        trace!("wait discovery");
        discovery.wait().await?;

        trace!("wait balancer");
        load_balancer.wait().await?;
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
