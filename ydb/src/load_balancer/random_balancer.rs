use std::sync::Arc;

use http::Uri;

use crate::{
    grpc_wrapper::raw_services::Service, waiter::WaiterImpl, DiscoveryState, Waiter, YdbError,
    YdbResult,
};

use super::LoadBalancer;

#[derive(Clone)]
pub(crate) struct RandomLoadBalancer {
    pub(super) discovery_state: Arc<DiscoveryState>,
    pub(super) waiter: Arc<WaiterImpl>,
}

impl RandomLoadBalancer {
    pub(crate) fn new() -> Self {
        Self {
            discovery_state: Arc::new(DiscoveryState::default()),
            waiter: Arc::new(WaiterImpl::new()),
        }
    }
}

impl LoadBalancer for RandomLoadBalancer {
    fn endpoint(&self, service: Service) -> YdbResult<Uri> {
        let nodes = self.discovery_state.get_nodes(&service);
        match nodes {
            None => Err(YdbError::Custom(format!(
                "no endpoints for service: '{}'",
                service
            ))),
            Some(nodes) => {
                if !nodes.is_empty() {
                    let index = rand::random::<usize>() % nodes.len();
                    let node = &nodes[index % nodes.len()];
                    Ok(node.uri.clone())
                } else {
                    Err(YdbError::Custom(format!(
                        "empty endpoint list for service: {}",
                        service
                    )))
                }
            }
        }
    }

    fn set_discovery_state(&mut self, discovery_state: &Arc<DiscoveryState>) -> YdbResult<()> {
        self.discovery_state = discovery_state.clone();
        if !self.discovery_state.is_empty() {
            self.waiter.set_received(Ok(()))
        }
        Ok(())
    }

    fn waiter(&self) -> Box<dyn Waiter> {
        Box::new(self.waiter.clone())
    }
}

#[async_trait::async_trait]
impl Waiter for RandomLoadBalancer {
    async fn wait(&self) -> YdbResult<()> {
        self.waiter.wait().await
    }
}
