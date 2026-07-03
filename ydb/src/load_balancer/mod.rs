use std::sync::Arc;

use futures_util::{stream::BoxStream, StreamExt};
use http::Uri;

use crate::{grpc_wrapper::raw_services::Service, DiscoveryState, Waiter, YdbResult};

#[cfg(test)]
pub mod balancer_test;
pub mod nearest_dc_balancer;
pub mod random_balancer;
pub mod shared_balancer;
pub mod static_balancer;

pub(crate) use shared_balancer::SharedLoadBalancer;
pub(crate) use static_balancer::StaticLoadBalancer;

#[mockall::automock]
pub(crate) trait LoadBalancer: Send + Sync + Waiter {
    fn endpoint(&self, service: Service) -> YdbResult<Uri>;
    fn set_discovery_state(&mut self, discovery_state: &Arc<DiscoveryState>) -> YdbResult<()>;
    fn waiter(&self) -> Box<dyn Waiter>; // need for wait ready in without read lock
}

#[async_trait::async_trait]
impl Waiter for MockLoadBalancer {
    async fn wait(&self) -> YdbResult<()> {
        Ok(())
    }
}

pub(crate) async fn update_load_balancer(
    mut lb: impl LoadBalancer,
    mut receiver: BoxStream<'static, Arc<DiscoveryState>>,
) {
    while let Some(state) = receiver.next().await {
        let _ = lb.set_discovery_state(&state);
    }
}
