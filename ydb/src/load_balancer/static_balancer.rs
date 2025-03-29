use std::sync::Arc;

use http::Uri;

use crate::{
    grpc_wrapper::raw_services::Service, waiter::WaiterImpl, DiscoveryState, Waiter, YdbError,
    YdbResult,
};

use super::LoadBalancer;

pub(crate) struct StaticLoadBalancer {
    endpoint: Uri,
}

impl StaticLoadBalancer {
    #[allow(dead_code)]
    pub(crate) fn new(endpoint: Uri) -> Self {
        Self { endpoint }
    }
}

impl LoadBalancer for StaticLoadBalancer {
    fn endpoint(&self, _: Service) -> YdbResult<Uri> {
        Ok(self.endpoint.clone())
    }

    fn set_discovery_state(&mut self, _: &Arc<DiscoveryState>) -> YdbResult<()> {
        Err(YdbError::Custom(
            "static balancer no way to update state".into(),
        ))
    }

    fn waiter(&self) -> Box<dyn Waiter> {
        let waiter = WaiterImpl::new();
        waiter.set_received(Ok(()));
        Box::new(waiter)
    }
}

#[async_trait::async_trait]
impl Waiter for StaticLoadBalancer {
    async fn wait(&self) -> YdbResult<()> {
        Ok(())
    }
}
