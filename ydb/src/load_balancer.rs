use crate::discovery::{Discovery, DiscoveryState, NodeInfo};
use crate::errors::*;
use http::Uri;
use itertools::Itertools;
use rand::thread_rng;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::grpc_wrapper::raw_services::Service;
use crate::waiter::{AllWaiter, Waiter, WaiterImpl};
use std::collections::HashMap;
use std::net::ToSocketAddrs;
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
        Ok(())
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
        Ok(())
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
        self.waiter.wait().await
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

pub(crate) struct BalancerConfig {
    fallback_strategy: FallbackStrategy,
    fallback_balancer: Option<Box<dyn LoadBalancer>>,
}

// What will balancer do if there is no available endpoints at local dc
#[derive(PartialEq, Eq)]
pub(crate) enum FallbackStrategy {
    Error,            // Just throw error
    BalanceWithOther, // Use another balancer
}

impl Default for BalancerConfig {
    fn default() -> Self {
        BalancerConfig {
            fallback_strategy: FallbackStrategy::BalanceWithOther,
            fallback_balancer: Some(Box::new(RandomLoadBalancer::new())),
        }
    }
}

pub(crate) struct NearestDCBalancer {
    discovery_state: Arc<DiscoveryState>,
    waiter: Arc<WaiterImpl>,
    config: BalancerConfig,
    preferred_endpoints: Vec<NodeInfo>,
    location: String,
}

impl NearestDCBalancer {
    pub(crate) fn new(config: BalancerConfig) -> YdbResult<Self> {
        match config.fallback_balancer.as_ref() {
            Some(_) => {
                if config.fallback_strategy == FallbackStrategy::Error {
                    return Err(YdbError::Custom(
                        "fallback strategy is Error but balancer was provided".to_string(),
                    ));
                }
            }
            None => {
                if config.fallback_strategy == FallbackStrategy::BalanceWithOther {
                    return Err(YdbError::Custom(
                        "no fallback balancer was provided".to_string(),
                    ));
                }
            }
        }
        Ok(Self {
            discovery_state: Arc::new(DiscoveryState::default()),
            waiter: Arc::new(WaiterImpl::new()),
            config,
            preferred_endpoints: Vec::new(),
            location: String::new(),
        })
    }
}

#[async_trait::async_trait]
impl Waiter for NearestDCBalancer {
    async fn wait(&self) -> YdbResult<()> {
        self.waiter().wait().await
    }
}

impl LoadBalancer for NearestDCBalancer {
    fn endpoint(&self, service: Service) -> YdbResult<Uri> {
        self.get_endpoint(service)
    }

    fn set_discovery_state(&mut self, discovery_state: &Arc<DiscoveryState>) -> YdbResult<()> {
        match self.config.fallback_balancer.as_mut() {
            Some(balancer) => balancer.set_discovery_state(discovery_state)?,
            None => (),
        }
        self.discovery_state = discovery_state.clone();
        if !self.discovery_state.is_empty() {
            self.waiter.set_received(Ok(()))
        }
        self.adjust_local_dc()?;
        self.adjust_preferred_endpoints()
    }

    fn waiter(&self) -> Box<dyn Waiter> {
        let self_waiter = Box::new(self.waiter.clone());
        match self.config.fallback_balancer.as_ref() {
            Some(balancer) => Box::new(AllWaiter::new(vec![self_waiter, balancer.waiter()])),
            None => self_waiter,
        }
    }
}

const NODES_PER_DC: usize = 5;

impl NearestDCBalancer {
    fn get_endpoint(&self, service: Service) -> YdbResult<Uri> {
        for ep in self.preferred_endpoints.iter() {
            return YdbResult::Ok(ep.uri.clone());
        }

        match self.config.fallback_strategy {
            FallbackStrategy::Error => Err(YdbError::custom(format!(
                "no available endpoints for service:{} in local dc:{}",
                service, self.location
            ))),
            FallbackStrategy::BalanceWithOther => {
                self.config
                    .fallback_balancer
                    .as_ref()
                    .unwrap() // unwrap is safe [checks inside ::new()]
                    .endpoint(service)
            }
        }
    }

    fn adjust_local_dc(&mut self) -> YdbResult<()> {
        let nodes = self.get_nodes()?;
        let mut dc_to_nodes = self.split_endpoints_by_location(nodes);
        let mut to_check = Vec::with_capacity(NODES_PER_DC * dc_to_nodes.keys().len());
        dc_to_nodes
            .iter_mut()
            .for_each(|(_, endpoints)| to_check.append(self.get_random_endpoints(endpoints)));
        let local_dc = self.find_local_dc(&to_check)?;
        self.location = local_dc;
        Ok(())
    }

    fn adjust_preferred_endpoints(&mut self) -> YdbResult<()> {
        self.preferred_endpoints = self
            .get_nodes()?
            .into_iter()
            .filter(|ep| ep.location == self.location)
            .map(|ep| ep.clone())
            .collect_vec();
        Ok(())
    }

    fn get_nodes(&self) -> YdbResult<&Vec<NodeInfo>> {
        let nodes = self.discovery_state.get_all_nodes();
        match nodes {
            None => Err(YdbError::Custom(format!(
                "no endpoints on discovery update"
            ))),
            Some(nodes) => Ok(nodes),
        }
    }

    fn split_endpoints_by_location<'a>(
        &'a self,
        nodes: &'a Vec<NodeInfo>,
    ) -> HashMap<String, Vec<&NodeInfo>> {
        let mut dc_to_eps = HashMap::<String, Vec<&NodeInfo>>::new();
        nodes.into_iter().for_each(|info| {
            if let Some(nodes) = dc_to_eps.get_mut(&info.location) {
                nodes.push(info);
            } else {
                dc_to_eps.insert(info.location.clone(), vec![info]);
            }
        });
        dc_to_eps
    }

    fn get_random_endpoints<'a>(
        &'a self,
        dc_endpoints: &'a mut Vec<&'a NodeInfo>,
    ) -> &mut Vec<&NodeInfo> {
        use rand::seq::SliceRandom;
        dc_endpoints.shuffle(&mut thread_rng());
        dc_endpoints.truncate(NODES_PER_DC);
        dc_endpoints
    }

    fn find_local_dc(&self, to_check: &[&NodeInfo]) -> YdbResult<String> {
        let addr_to_node = self.addr_to_node(to_check);
        if addr_to_node.is_empty() {
            return Err(YdbError::Custom(format!("no ip addresses for endpoints")));
        }
        let addrs = addr_to_node.keys();
        match self.find_fastest_address(addrs.collect()) {
            Some(fastest_address) => Ok(addr_to_node[&fastest_address].location.clone()),
            None => Err(YdbError::custom("could not find fastest address")),
        }
    }

    fn addr_to_node<'a>(&'a self, nodes: &[&'a NodeInfo]) -> HashMap<String, &NodeInfo> {
        let mut addr_to_node = HashMap::<String, &NodeInfo>::with_capacity(2 * nodes.len()); // ipv4 + ipv6
        nodes.into_iter().for_each(|info| {
            let host: &str;
            let port: u16;
            match info.uri.host() {
                Some(uri_host) => host = uri_host,
                None => return,
            }
            match info.uri.port() {
                Some(uri_port) => port = uri_port.as_u16(),
                None => return,
            }
            let _ = (host, port).to_socket_addrs().and_then(|addrs| {
                for addr in addrs {
                    addr_to_node.insert(addr.to_string(), info);
                }
                Ok(())
            });
        });
        addr_to_node
    }

    fn find_fastest_address(&self, addrs: Vec<&String>) -> Option<String> {
        let stop_measure = CancellationToken::new();
        let (start_measure, _) = tokio::sync::broadcast::channel::<()>(1);
        let (addr_sender, mut addr_reciever) = tokio::sync::mpsc::channel::<Option<String>>(1);
        let addr_count = addrs.len();
        let mut nursery = JoinSet::new();

        for addr in addrs {
            let (mut wait_for_start, stop_measure, addr, addr_sender) = (
                start_measure.subscribe(),
                stop_measure.clone(),
                addr.clone(),
                addr_sender.clone(),
            );

            nursery.spawn(async move {
                let _ = wait_for_start.recv().await;
                tokio::select! {
                    connection_result = TcpStream::connect(addr.clone()) =>{
                        match connection_result{
                            Ok(mut connection) => {
                                let _ = connection.shutdown().await;
                                let _ = addr_sender.send(Some(addr)).await;
                            },
                            Err(_) => {let _ = addr_sender.send(None).await;},
                        }
                    }
                    _ = stop_measure.cancelled() => {
                        ();
                    }
                }
            });
        }

        tokio::task::block_in_place(move || {
            let _ = start_measure.send(());
            Handle::current().block_on(async {
                for _ in 0..addr_count {
                    match self.wait_for_single_address(&mut addr_reciever).await {
                        Some(address) => {
                            stop_measure.cancel();
                            self.join_all(&mut nursery).await;
                            return Some(address);
                        }
                        None => continue,
                    }
                }
                None
            })
        })
    }

    async fn wait_for_single_address(
        &self,
        addr_reciever: &mut mpsc::Receiver<Option<String>>,
    ) -> Option<String> {
        match addr_reciever.recv().await {
            Some(maybe_address) => maybe_address,
            None => unreachable!(), // no channel closing while awaiting address
        }
    }

    async fn join_all(&self, awaitable: &mut JoinSet<()>) {
        while let Some(_) = awaitable.join_next().await {}
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
            NodeInfo::new(Uri::from_str("http://test.com").unwrap(), String::new()),
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
                    .with_node_info(Table, NodeInfo::new(one.clone(), String::new()))
                    .with_node_info(Table, NodeInfo::new(two.clone(), String::new())),
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
