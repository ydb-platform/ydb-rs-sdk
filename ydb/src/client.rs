use crate::client_common::DBCredentials;
use crate::client_table::TableClient;
use crate::discovery::Discovery;
use crate::errors::YdbResult;
use crate::load_balancer::SharedLoadBalancer;
use crate::middlewares::AuthService;
use crate::waiter::Waiter;

use std::sync::Arc;
use std::time::Duration;

use crate::grpc::GrpcClientFabric;
use tracing::trace;

pub(crate) type Middleware = AuthService;

/// YDB client
pub struct Client {
    grpc_client_fabric: GrpcClientFabric,
    load_balancer: SharedLoadBalancer,
    discovery: Arc<Box<dyn Discovery>>,
    timeouts: TimeoutSettings,
}

impl Client {
    pub(crate) fn new(
        grpc_client_fabric: GrpcClientFabric,
        discovery: Box<dyn Discovery>,
    ) -> YdbResult<Self> {
        let discovery = Arc::new(discovery);

        return Ok(Client {
            grpc_client_fabric,
            load_balancer: SharedLoadBalancer::new(discovery.as_ref()),
            discovery,
            timeouts: TimeoutSettings::default(),
        });
    }

    /// Create instance of client for table service
    pub fn table_client(&self) -> TableClient {
        return TableClient::new(
            self.grpc_client_fabric.clone(),
            self.discovery.clone(),
            self.timeouts,
        );
    }

    pub fn with_timeouts(mut self, timeouts: TimeoutSettings) -> Self {
        self.timeouts = timeouts;
        return self;
    }

    /// Wait initialization completed
    ///
    /// Wait all background process get first successfully result and client fully
    /// available to work.
    pub async fn wait(&self) -> YdbResult<()> {
        trace!("waiting grpc_client_fabric");
        self.grpc_client_fabric.wait().await?;
        trace!("wait discovery");
        self.discovery.wait().await?;

        trace!("wait balancer");
        self.load_balancer.wait().await?;
        return Ok(());
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
        return TimeoutSettings {
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
        };
    }
}
