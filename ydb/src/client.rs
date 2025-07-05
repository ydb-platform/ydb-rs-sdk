use crate::client_common::DBCredentials;
use crate::client_coordination::client::CoordinationClient;
use crate::client_scheme::client::SchemeClient;
use crate::client_table::TableClient;
use crate::discovery::Discovery;
use crate::errors::YdbResult;
use crate::load_balancer::SharedLoadBalancer;
use crate::waiter::Waiter;

use std::sync::Arc;
use std::time::Duration;

use crate::client_topic::client::TopicClient;
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
}

impl Client {
    pub(crate) fn new(
        credentials: DBCredentials,
        discovery: Arc<Box<dyn Discovery>>,
        connection_manager: GrpcConnectionManager,
    ) -> YdbResult<Self> {
        let discovery_ref = discovery.as_ref().as_ref();

        Ok(Client {
            credentials,
            load_balancer: SharedLoadBalancer::new(discovery_ref),
            discovery,
            timeouts: TimeoutSettings::default(),
            connection_manager,
        })
    }

    pub fn database(&self) -> String {
        self.credentials.database.clone()
    }

    /// Create instance of client for table service
    pub fn table_client(&self) -> TableClient {
        TableClient::new(self.connection_manager.clone(), self.timeouts)
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
        )
    }

    /// Create instance of client for coordination service
    pub fn coordination_client(&self) -> CoordinationClient {
        CoordinationClient::new(self.timeouts, self.connection_manager.clone())
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

const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(1);

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
#[derive(Copy, Clone, Debug)]
pub struct TimeoutSettings {
    pub operation_timeout: Duration,
}

impl TimeoutSettings {
    pub(crate) fn operation_params(&self) -> RawOperationParams {
        RawOperationParams::new_with_timeouts(self.operation_timeout, self.operation_timeout)
    }
}

impl Default for TimeoutSettings {
    fn default() -> Self {
        TimeoutSettings {
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
        }
    }
}
