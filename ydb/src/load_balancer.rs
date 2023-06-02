use crate::discovery::{Discovery, DiscoveryState};
use crate::errors::*;
use http::Uri;

use crate::grpc_wrapper::raw_services::Service;
use crate::waiter::{Waiter, WaiterImpl};
use std::sync::{Arc, RwLock};
use tokio::sync::watch::Receiver;

#[mockall::automock]
pub(crate) trait LoadBalancer: Send + Sync + Waiter {
    fn endpoint(&self, service: Service) -> YdbResult<Uri>;
    fn set_discovery_state(&mut self, discovery_state: &Arc<DiscoveryState>) -> YdbResult<()>;
    fn waiter(&self) -> Box<dyn Waiter>; // need for wait ready in without read lock
}

#[async_trait::async_trait]
impl Waiter for MockLoadBalancer {
    async fn wait(&self) -> YdbResult<()> {
        return Ok(());
    }
}

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
        return Ok(());
    }
}

#[derive(Clone)]
pub(crate) struct RandomLoadBalancer {
    discovery_state: Arc<DiscoveryState>,
    waiter: Arc<WaiterImpl>,
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
        return self.waiter.wait().await;
    }
}

pub(crate) async fn update_load_balancer(
    mut lb: impl LoadBalancer,
    mut receiver: Receiver<Arc<DiscoveryState>>,
) {
    loop {
        // clone for prevent block send side while update current lb
        let state = receiver.borrow_and_update().clone();
        let _ = lb.set_discovery_state(&state);
        if receiver.changed().await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::discovery::NodeInfo;
    use crate::grpc_wrapper::raw_services::Service::Table;
    use mockall::predicate;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::time::Duration;
    use tracing::trace;

    #[test]
    fn shared_load_balancer() -> YdbResult<()> {
        let endpoint_counter = Arc::new(AtomicUsize::new(0));
        let test_uri = Uri::from_str("http://test.com")?;

        let mut lb_mock = MockLoadBalancer::new();
        let endpoint_counter_mock = endpoint_counter.clone();
        let test_uri_mock = test_uri.clone();

        lb_mock.expect_endpoint().returning(move |_service| {
            endpoint_counter_mock.fetch_add(1, Relaxed);
            Ok(test_uri_mock.clone())
        });

        let s1 = SharedLoadBalancer::new_with_balancer(Box::new(lb_mock));

        #[allow(clippy::redundant_clone)]
        let s2 = s1.clone();

        assert_eq!(test_uri, s1.endpoint(Table)?);
        assert_eq!(test_uri, s2.endpoint(Table)?);
        assert_eq!(endpoint_counter.load(Relaxed), 2);
        Ok(())
    }

    #[tokio::test]
    async fn update_load_balancer_test() -> YdbResult<()> {
        let original_discovery_state = Arc::new(DiscoveryState::default());
        let (sender, receiver) = tokio::sync::watch::channel(original_discovery_state.clone());

        let new_discovery_state = Arc::new(DiscoveryState::default().with_node_info(
            Table,
            NodeInfo::new(Uri::from_str("http://test.com").unwrap()),
        ));

        let (first_update_sender, first_update_receiver) = tokio::sync::oneshot::channel();
        let (second_update_sender, second_update_receiver) = tokio::sync::oneshot::channel();
        let (updater_finished_sender, updater_finished_receiver) =
            tokio::sync::oneshot::channel::<()>();

        let mut first_update_sender = Some(first_update_sender);
        let mut second_update_sender = Some(second_update_sender);
        let mut lb_mock = MockLoadBalancer::new();
        lb_mock
            .expect_set_discovery_state()
            .with(predicate::eq(original_discovery_state.clone()))
            .times(1)
            .returning(move |_| {
                trace!("first set");
                first_update_sender.take().unwrap().send(()).unwrap();
                Ok(())
            });

        lb_mock
            .expect_set_discovery_state()
            .with(predicate::eq(new_discovery_state.clone()))
            .times(1)
            .returning(move |_| {
                trace!("second set");
                second_update_sender.take().unwrap().send(()).unwrap();
                Ok(())
            });

        let shared_lb = SharedLoadBalancer::new_with_balancer(Box::new(lb_mock));

        tokio::spawn(async move {
            trace!("updater start");
            update_load_balancer(shared_lb, receiver).await;
            trace!("updater finished");
            updater_finished_sender.send(()).unwrap();
        });

        tokio::spawn(async move {
            first_update_receiver.await.unwrap();
            sender.send(new_discovery_state).unwrap();
            second_update_receiver.await.unwrap();
            drop(sender);
        });

        tokio::select! {
            _ = updater_finished_receiver =>{}
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                panic!("test failed");
            }
        }
        // updater_finished_receiver.await.unwrap();
        Ok(())
    }

    #[test]
    fn random_load_balancer() -> YdbResult<()> {
        let one = Uri::from_str("http://one:213")?;
        let two = Uri::from_str("http://two:213")?;
        let load_balancer = RandomLoadBalancer {
            discovery_state: Arc::new(
                DiscoveryState::default()
                    .with_node_info(Table, NodeInfo::new(one.clone()))
                    .with_node_info(Table, NodeInfo::new(two.clone())),
            ),
            waiter: Arc::new(WaiterImpl::new()),
        };

        let mut map = HashMap::new();
        map.insert(one.to_string(), 0);
        map.insert(two.to_string(), 0);

        for _ in 0..100 {
            let u = load_balancer.endpoint(Table)?;
            let val = *map.get_mut(u.to_string().as_str()).unwrap();
            map.insert(u.to_string(), val + 1);
        }

        assert_eq!(map.len(), 2);
        assert!(*map.get(one.to_string().as_str()).unwrap() > 30);
        assert!(*map.get(two.to_string().as_str()).unwrap() > 30);
        Ok(())
    }
}
