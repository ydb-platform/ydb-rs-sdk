use std::sync::{Arc, RwLock};

use http::Uri;

use crate::{grpc_wrapper::raw_services::Service, Discovery, DiscoveryState, Waiter, YdbResult};

use super::{random_balancer::RandomLoadBalancer, update_load_balancer, LoadBalancer};

#[derive(Clone)]
pub(crate) struct SharedLoadBalancer {
    inner: Arc<RwLock<Box<dyn LoadBalancer>>>,
}

impl SharedLoadBalancer {
    pub(crate) fn new(discovery: &dyn Discovery) -> Self {
        Self::new_with_balancer_and_updater(Box::new(RandomLoadBalancer::new()), discovery)
    }

    pub(crate) fn new_with_balancer(load_balancer: Box<dyn LoadBalancer>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(load_balancer)),
        }
    }

    pub(crate) fn new_with_balancer_and_updater(
        load_balancer: Box<dyn LoadBalancer>,
        discovery: &dyn Discovery,
    ) -> Self {
        let mut shared_lb = Self::new_with_balancer(load_balancer);
        let shared_lb_updater = shared_lb.clone();
        let discovery_receiver = discovery.subscribe();
        let _ = shared_lb.set_discovery_state(&discovery.state());
        tokio::spawn(
            async move { update_load_balancer(shared_lb_updater, discovery_receiver).await },
        );
        shared_lb
    }
}

impl LoadBalancer for SharedLoadBalancer {
    fn endpoint(&self, service: Service) -> YdbResult<Uri> {
        self.inner.read()?.endpoint(service)
    }

    fn set_discovery_state(&mut self, discovery_state: &Arc<DiscoveryState>) -> YdbResult<()> {
        self.inner.write()?.set_discovery_state(discovery_state)
    }

    fn waiter(&self) -> Box<dyn Waiter> {
        return self.inner.read().unwrap().waiter();
    }
}

#[async_trait::async_trait]
impl Waiter for SharedLoadBalancer {
    async fn wait(&self) -> YdbResult<()> {
        let waiter = self.inner.read()?.waiter();
        return waiter.wait().await;
    }
}
