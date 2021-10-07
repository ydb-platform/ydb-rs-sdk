use crate::errors::*;
use crate::internal::discovery::{DiscoveryState, Service};
use http::Uri;
use mockall;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, RwLock};
use tokio::sync::watch::Receiver;

#[mockall::automock]
pub(crate) trait LoadBalancer {
    fn endpoint(&self, service: Service) -> Result<Uri>;
    fn set_discovery_state(&mut self, discovery_state: &Arc<DiscoveryState>) -> Result<()>;
}

#[derive(Clone)]
pub(crate) struct SharedLoadBalancer {
    inner: Arc<RwLock<Box<dyn LoadBalancer>>>,
}

impl SharedLoadBalancer {
    pub(crate) fn new(load_balancer: Box<dyn LoadBalancer>) -> Self {
        return Self {
            inner: Arc::new(RwLock::new(load_balancer)),
        };
    }
}

impl LoadBalancer for SharedLoadBalancer {
    fn endpoint(&self, service: Service) -> Result<Uri> {
        return self.inner.read()?.endpoint(service);
    }

    fn set_discovery_state(&mut self, discovery_state: &Arc<DiscoveryState>) -> Result<()> {
        self.inner.write()?.set_discovery_state(discovery_state)
    }
}

pub(crate) struct StaticLoadBalancer {
    endpoint: Uri,
}

impl StaticLoadBalancer {
    #[allow(dead_code)]
    pub(crate) fn new(endpoint: Uri) -> Self {
        return Self { endpoint };
    }
}

impl LoadBalancer for StaticLoadBalancer {
    fn endpoint(&self, _: Service) -> Result<Uri> {
        return Ok(self.endpoint.clone());
    }

    fn set_discovery_state(&mut self, _: &Arc<DiscoveryState>) -> Result<()> {
        Err(Error::Custom(
            "static balancer no way to update state".into(),
        ))
    }
}

pub(crate) struct RoundRobin {
    counter: AtomicUsize,
    discovery_state: Arc<DiscoveryState>,
}

impl LoadBalancer for RoundRobin {
    fn endpoint(&self, service: Service) -> Result<Uri> {
        let counter = self.counter.fetch_add(1, Relaxed);
        let nodes = self.discovery_state.services.get(&service);
        match nodes {
            None => Err(Error::Custom(
                format!("no endpoints for service: '{}'", service).into(),
            )),
            Some(nodes) => {
                if nodes.len() > 0 {
                    let node = &nodes[counter % nodes.len()];
                    Ok(node.uri.clone())
                } else {
                    Err(Error::Custom(
                        format!("empty endpoint list for service: {}", service).into(),
                    ))
                }
            }
        }
    }

    fn set_discovery_state(&mut self, discovery_state: &Arc<DiscoveryState>) -> Result<()> {
        self.discovery_state = discovery_state.clone();
        Ok(())
    }
}

pub(crate) async fn update_load_balancer(
    mut lb: impl LoadBalancer,
    mut receiver: Receiver<Arc<DiscoveryState>>,
) {
    while receiver.changed().await.is_ok() {
        // clone for prevent block send side while update current lb
        let state = receiver.borrow_and_update().clone();
        lb.set_discovery_state(&state);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::internal::discovery::NodeInfo;
    use crate::internal::discovery::Service::Table;
    use mockall::predicate;
    use std::str::FromStr;

    #[test]
    fn shared_load_balancer() -> UnitResult {
        let endpoint_counter = Arc::new(AtomicUsize::new(0));
        let test_uri = Uri::from_str("http://test.com")?;

        let mut lb_mock = MockLoadBalancer::new();
        let endpoint_counter_mock = endpoint_counter.clone();
        let test_uri_mock = test_uri.clone();

        lb_mock.expect_endpoint().returning(move |_service| {
            endpoint_counter_mock.fetch_add(1, Relaxed);
            return Ok(test_uri_mock.clone());
        });

        let s1 = SharedLoadBalancer::new(Box::new(lb_mock));
        let s2 = s1.clone();

        assert_eq!(test_uri, s1.endpoint(Table)?);
        assert_eq!(test_uri, s2.endpoint(Table)?);
        assert_eq!(endpoint_counter.load(Relaxed), 2);
        return UNIT_OK;
    }

    #[tokio::test]
    async fn update_load_balancer_test() -> UnitResult {
        let original_discovery_state = Arc::new(DiscoveryState::default());
        let (mut sender, receiver) = tokio::sync::watch::channel(original_discovery_state.clone());

        let new_discovery_state = Arc::new(DiscoveryState::default().with_node_info(
            Table,
            NodeInfo::new(Uri::from_str("http://test.com").unwrap()),
        ));

        let mut lb_mock = MockLoadBalancer::new();
        lb_mock
            .expect_set_discovery_state()
            .with(predicate::eq(original_discovery_state.clone()))
            .times(1)
            .returning(move |new_state: &Arc<DiscoveryState>| UNIT_OK);
        lb_mock
            .expect_set_discovery_state()
            .with(predicate::eq(new_discovery_state.clone()))
            .times(1)
            .returning(move |new_state: &Arc<DiscoveryState>| UNIT_OK);

        let mut shared_lb = SharedLoadBalancer::new(lb_mock);
        tokio::spawn(async { update_load_balancer(shared_lb, receiver).await });

        return UNIT_OK;
    }
}
