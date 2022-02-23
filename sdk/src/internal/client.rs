use crate::errors::YdbResult;
use crate::internal::client_common::DBCredentials;
use crate::internal::client_table::TableClient;
use crate::internal::discovery::Discovery;
use crate::internal::load_balancer::SharedLoadBalancer;
use crate::internal::middlewares::AuthService;
use crate::internal::waiter::Waiter;

use std::sync::Arc;

use tracing::trace;

pub(crate) type Middleware = AuthService;

pub struct Client {
    credentials: DBCredentials,
    load_balancer: SharedLoadBalancer,
    discovery: Arc<Box<dyn Discovery>>,
}

impl Client {
    pub(crate) fn new_internal(
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

    /// Create instance of table client
    pub fn table_client(&self) -> TableClient {
        return TableClient::new(self.credentials.clone(), self.discovery.clone());
    }

    /// Wait initialization completed
    ///
    /// Wait all background process get first successfully result and client fully
    /// available to work
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
