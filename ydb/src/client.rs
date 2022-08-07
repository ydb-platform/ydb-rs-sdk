use crate::client_common::DBCredentials;
use crate::client_scheme::client::SchemeClient;
use crate::client_table::TableClient;
use crate::discovery::Discovery;
use crate::errors::YdbResult;
use crate::load_balancer::SharedLoadBalancer;
use crate::middlewares::AuthService;
use crate::waiter::Waiter;

use std::sync::Arc;
use std::time::Duration;

use crate::grpc_connection_manager::GrpcConnectionManager;
use tracing::trace;

pub(crate) type Middleware = AuthService;

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
        discovery: Box<dyn Discovery>,
        connection_manager: GrpcConnectionManager,
    ) -> YdbResult<Self> {
        let discovery = Arc::new(discovery);
        let discovery_ref = discovery.as_ref().as_ref();
        return Ok(Client {
            credentials,
            load_balancer: SharedLoadBalancer::new(discovery_ref),
            discovery,
            timeouts: TimeoutSettings::default(),
            connection_manager,
        });
    }

    pub(crate) fn database(&self) -> String {
        self.credentials.database.clone()
    }

    /// Create instance of client for table service
    pub fn table_client(&self) -> TableClient {
        TableClient::new(
            self.credentials.clone(),
            self.discovery.clone(),
            self.timeouts,
        )
    }

    /// Create instance of client for directory service
    pub fn scheme_client(&self) -> SchemeClient {
        SchemeClient::new(self.timeouts, self.connection_manager.clone())
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

#[non_exhaustive]
#[derive(Copy, Clone, Debug)]
pub struct TimeoutSettings {
    pub operation_timeout: Duration,
}

impl Default for TimeoutSettings {
    fn default() -> Self {
        TimeoutSettings {
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
        }
    }
}
