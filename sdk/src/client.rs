use crate::errors::YdbResult;
use crate::client_common::DBCredentials;
use crate::client_table::TableClient;
use crate::discovery::Discovery;
use crate::load_balancer::SharedLoadBalancer;
use crate::middlewares::AuthService;
use crate::waiter::Waiter;

use std::sync::Arc;

use tracing::trace;

pub(crate) type Middleware = AuthService;

/// YDB client
pub struct Client {
    credentials: DBCredentials,
    load_balancer: SharedLoadBalancer,
    discovery: Arc<Box<dyn Discovery>>,
}

impl Client {
    pub(crate) fn new(
        credentials: DBCredentials,
        discovery: Box<dyn Discovery>,
    ) -> YdbResult<Self> {
        let discovery = Arc::new(discovery);

        return Ok(Client {
            credentials,
            load_balancer: SharedLoadBalancer::new(discovery.as_ref()),
            discovery,
        });
    }

    /// Create instance of client for table service
    pub fn table_client(&self) -> TableClient {
        return TableClient::new(self.credentials.clone(), self.discovery.clone());
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
        return Ok(());
    }
}
