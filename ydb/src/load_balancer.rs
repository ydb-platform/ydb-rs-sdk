use crate::discovery::{Discovery, DiscoveryState, NodeInfo};
use crate::errors::*;
use crate::grpc_wrapper::raw_services::Service;
use crate::waiter::{AllWaiter, Waiter, WaiterImpl};
use http::Uri;
use itertools::Itertools;
use rand::seq::IteratorRandom;
use rand::{seq::SliceRandom, thread_rng};
use std::borrow::{Borrow, BorrowMut};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::{
        broadcast, mpsc, watch,
        watch::{Receiver, Sender},
        Mutex,
    },
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

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
}

#[derive(Default)]
struct BalancerState {
    preferred_endpoints: Vec<NodeInfo>,
}

// What will balancer do if there is no available endpoints at local dc
pub(crate) enum FallbackStrategy {
    Error,                                   // Just throw error
    BalanceWithOther(Box<dyn LoadBalancer>), // Use another balancer
}

impl Default for BalancerConfig {
    fn default() -> Self {
        BalancerConfig {
            fallback_strategy: FallbackStrategy::BalanceWithOther(Box::new(
                RandomLoadBalancer::new(),
            )),
        }
    }
}

pub(crate) struct NearestDCBalancer {
    discovery_state: Arc<DiscoveryState>,
    state_sender: Sender<Arc<DiscoveryState>>,
    ping_token: CancellationToken,
    waiter: Arc<WaiterImpl>,
    config: BalancerConfig,
    balancer_state: Arc<Mutex<BalancerState>>,
}

impl NearestDCBalancer {
    pub(crate) fn new(config: BalancerConfig) -> YdbResult<Self> {
        let discovery_state = Arc::new(DiscoveryState::default());
        let balancer_state = Arc::new(Mutex::new(BalancerState::default()));
        let balancer_state_updater = balancer_state.clone();
        let (state_sender, state_reciever) = watch::channel(discovery_state.clone());

        let ping_token = CancellationToken::new();
        let ping_token_clone = ping_token.clone();

        let waiter = Arc::new(WaiterImpl::new());
        let waiter_clone = waiter.clone();

        tokio::spawn(async move {
            Self::adjust_local_dc(
                balancer_state_updater,
                state_reciever,
                ping_token_clone,
                waiter_clone,
            )
            .await
        });

        Ok(Self {
            discovery_state,
            state_sender,
            ping_token,
            waiter,
            config,
            balancer_state,
        })
    }
}

impl Drop for NearestDCBalancer {
    fn drop(&mut self) {
        self.ping_token.cancel();
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
        match self.config.fallback_strategy.borrow_mut() {
            FallbackStrategy::BalanceWithOther(balancer) => {
                balancer.set_discovery_state(discovery_state)?
            }
            FallbackStrategy::Error => (),
        }
        self.discovery_state = discovery_state.clone();
        let _ = self.state_sender.send(discovery_state.clone());
        Ok(())
    }

    fn waiter(&self) -> Box<dyn Waiter> {
        let self_waiter = Box::new(self.waiter.clone());
        match self.config.fallback_strategy.borrow() {
            FallbackStrategy::BalanceWithOther(balancer) => {
                Box::new(AllWaiter::new(vec![self_waiter, balancer.waiter()]))
            }
            FallbackStrategy::Error => self_waiter,
        }
    }
}

const NODES_PER_DC: usize = 5;
const PING_TIMEOUT_SECS: u64 = 60;

impl NearestDCBalancer {
    fn get_endpoint(&self, service: Service) -> YdbResult<Uri> {
        match self.balancer_state.try_lock() {
            Ok(state_guard) => {
                match state_guard
                    .borrow()
                    .preferred_endpoints
                    .choose(&mut thread_rng())
                {
                    Some(ep) => return YdbResult::Ok(ep.uri.clone()),
                    None => (),
                }
                match self.config.fallback_strategy.borrow() {
                    FallbackStrategy::Error => Err(YdbError::custom(format!(
                        "no available endpoints for service:{}",
                        service
                    ))),
                    FallbackStrategy::BalanceWithOther(balancer) => {
                        info!("trying fallback balancer...");
                        balancer.endpoint(service)
                    }
                }
            }
            Err(_) => Err(YdbError::Custom(
                "balancer is updating its state".to_string(),
            )),
        }
    }

    async fn adjust_local_dc(
        balancer_state: Arc<Mutex<BalancerState>>,
        mut state_reciever: watch::Receiver<Arc<DiscoveryState>>,
        stop_ping_process: CancellationToken,
        waiter: Arc<WaiterImpl>,
    ) {
        loop {
            tokio::select! {
                _ = stop_ping_process.cancelled() => {
                    return
                }
                result = state_reciever.changed() =>{
                    if result.is_err(){ // sender have been dropped
                        return
                    }
                }
            }
            let new_discovery_state = state_reciever.borrow_and_update().clone();
            match Self::extract_nodes(&new_discovery_state) {
                Ok(some_nodes) => {
                    let mut dc_to_nodes = Self::split_endpoints_by_location(some_nodes);
                    let mut to_check = Vec::with_capacity(NODES_PER_DC * dc_to_nodes.keys().len());
                    dc_to_nodes.iter_mut().for_each(|(_, endpoints)| {
                        to_check.append(Self::get_random_endpoints(endpoints))
                    });
                    match Self::find_local_dc(&to_check).await {
                        Ok(dc) => {
                            info!("found new local dc:{}", dc);
                            Self::adjust_preferred_endpoints(&balancer_state, some_nodes, dc).await;
                            waiter.set_received(Ok(()));
                        }
                        Err(err) => {
                            error!("error on search local dc:{}", err);
                            continue;
                        }
                    }
                }
                Err(_) => continue,
            }
        }
    }

    async fn adjust_preferred_endpoints(
        balancer_state: &Arc<Mutex<BalancerState>>,
        new_nodes: &Vec<NodeInfo>,
        local_dc: String,
    ) {
        info!("adjusting endpoints");
        let new_preferred_endpoints = new_nodes
            .into_iter()
            .filter(|ep| ep.location == local_dc)
            .map(|ep| ep.clone())
            .collect_vec();
        (balancer_state.lock().await) // fast lock
            .borrow_mut()
            .preferred_endpoints = new_preferred_endpoints;
    }

    fn extract_nodes(from_state: &Arc<DiscoveryState>) -> YdbResult<&Vec<NodeInfo>> {
        let nodes = from_state.get_all_nodes();
        match nodes {
            None => Err(YdbError::Custom(format!(
                "no endpoints on discovery update"
            ))),
            Some(nodes) => Ok(nodes),
        }
    }

    fn split_endpoints_by_location<'a>(
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

    fn get_random_endpoints<'a>(dc_endpoints: &'a mut Vec<&'a NodeInfo>) -> &mut Vec<&NodeInfo> {
        dc_endpoints.shuffle(&mut thread_rng());
        dc_endpoints.truncate(NODES_PER_DC);
        dc_endpoints
    }

    async fn find_local_dc(to_check: &[&NodeInfo]) -> YdbResult<String> {
        let addr_to_node = Self::addr_to_node(to_check);
        if addr_to_node.is_empty() {
            return Err(YdbError::Custom(format!("no ip addresses for endpoints")));
        }
        let addrs = addr_to_node.keys();
        match Self::find_fastest_address(addrs.collect(), Duration::from_secs(PING_TIMEOUT_SECS))
            .await
        {
            Ok(fastest_address) => Ok(addr_to_node[&fastest_address].location.clone()),
            Err(err) => {
                error!("could not find fastest address:{}", err);
                Err(err)
            }
        }
    }

    fn addr_to_node<'a>(nodes: &[&'a NodeInfo]) -> HashMap<String, &'a NodeInfo> {
        let mut addr_to_node = HashMap::<String, &NodeInfo>::with_capacity(2 * nodes.len()); // ipv4 + ipv6
        nodes.into_iter().for_each(|info| {
            let host: &str;
            let port: u16;
            match info.uri.host() {
                Some(uri_host) => host = uri_host,
                None => {
                    warn!("no host for uri:{}", info.uri);
                    return;
                }
            }
            match info.uri.port() {
                Some(uri_port) => port = uri_port.as_u16(),
                None => {
                    warn!("no port for uri:{}", info.uri);
                    return;
                }
            }
            use std::net::ToSocketAddrs;
            let _ = (host, port).to_socket_addrs().and_then(|addrs| {
                for addr in addrs {
                    addr_to_node.insert(addr.to_string(), info);
                }
                Ok(())
            });
        });
        addr_to_node
    }

    async fn find_fastest_address(addrs: Vec<&String>, timeout: Duration) -> YdbResult<String> {
        // Cancellation flow: timeout -> address collector -> address producers
        let interrupt_via_timeout = CancellationToken::new();
        let interrupt_collector_future = interrupt_via_timeout.child_token();
        let stop_measure = interrupt_collector_future.child_token(); // (*)

        let (start_measure, _) = broadcast::channel::<()>(1);
        let buffer_cap = if addrs.len() > 0 { addrs.len() } else { 1 };
        let (addr_sender, mut addr_reciever) = mpsc::channel::<Option<String>>(buffer_cap);
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

        let wait_first_some_or_cancel = async {
            loop {
                tokio::select! {
                    biased; // check timeout first
                    _ = interrupt_collector_future.cancelled() =>{
                        Self::join_all(&mut nursery).await; // children will be cancelled due to tokens chaining, see (*)
                        return YdbResult::Err("cancelled".into())
                    }
                    address_reciever_option = addr_reciever.recv() =>{
                        match address_reciever_option {
                            Some(address_option) => {
                                match address_option {
                                   Some(address) =>{
                                    interrupt_collector_future.cancel(); // Cancel other producing children
                                    Self::join_all(&mut nursery).await;
                                    return YdbResult::Ok(address);
                                   },
                                   None => continue, // Some producer sent blank address -> wait others
                                }
                            },
                            None => return YdbResult::Err("no fastest address".into()), // Channel closed, all producers have done measures
                        }
                    }
                }
            }
        };

        let _ = start_measure.send(());

        match tokio::time::timeout(timeout, wait_first_some_or_cancel).await {
            Ok(address_option) => address_option,
            Err(_) => {
                interrupt_via_timeout.cancel();
                YdbResult::Err("timeout while detecting fastest address".into())
            }
        }
    }

    async fn join_all(awaitable: &mut JoinSet<()>) {
        while awaitable.join_next().await.is_some() {}
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use crate::discovery::NodeInfo;
    use crate::grpc_wrapper::raw_services::Service::Table;
    use mockall::predicate;
    use ntest::assert_true;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::time::timeout;
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

    #[test]
    fn split_by_location() -> YdbResult<()> {
        let nodes = vec![
            NodeInfo::new(Uri::from_str("http://one:213")?, "A".to_string()),
            NodeInfo::new(Uri::from_str("http://two:213")?, "A".to_string()),
            NodeInfo::new(Uri::from_str("http://three:213")?, "B".to_string()),
            NodeInfo::new(Uri::from_str("http://four:213")?, "B".to_string()),
            NodeInfo::new(Uri::from_str("http://five:213")?, "C".to_string()),
        ];
        let splitted = NearestDCBalancer::split_endpoints_by_location(&nodes);
        assert_eq!(splitted.keys().len(), 3);
        assert_eq!(splitted["A"].len(), 2);
        assert_eq!(splitted["B"].len(), 2);
        assert_eq!(splitted["C"].len(), 1);
        Ok(())
    }

    #[test]
    fn choose_random_endpoints() -> YdbResult<()> {
        let nodes = vec![
            NodeInfo::new(Uri::from_str("http://one:213")?, "C".to_string()),
            NodeInfo::new(Uri::from_str("http://two:213")?, "C".to_string()),
            NodeInfo::new(Uri::from_str("http://three:213")?, "C".to_string()),
            NodeInfo::new(Uri::from_str("http://four:213")?, "C".to_string()),
            NodeInfo::new(Uri::from_str("http://five:213")?, "C".to_string()),
            NodeInfo::new(Uri::from_str("http://seven:213")?, "C".to_string()),
            NodeInfo::new(Uri::from_str("http://eight:213")?, "C".to_string()),
            NodeInfo::new(Uri::from_str("http://nine:213")?, "C".to_string()),
        ];

        let mut refs = nodes.iter().collect_vec();
        let nodes_clone = refs.clone();
        let random_subset = NearestDCBalancer::get_random_endpoints(&mut refs);

        assert_eq!(random_subset.len(), NODES_PER_DC);
        for node in random_subset {
            assert_true!(nodes_clone.contains(node))
        }

        Ok(())
    }

    #[test]
    fn extract_addrs_and_map_them() -> YdbResult<()> {
        let one = NodeInfo::new(Uri::from_str("http://localhost:123")?, "C".to_string());
        let two = NodeInfo::new(Uri::from_str("http://localhost:321")?, "C".to_string());
        let nodes = vec![&one, &two];
        let map = NearestDCBalancer::addr_to_node(&nodes);

        assert_eq!(map.keys().len(), 4); // ipv4 + ipv6 on each
        assert_true!(map.keys().contains(&"127.0.0.1:123".to_string()));
        assert_true!(map.keys().contains(&"[::1]:123".to_string()));
        assert!(map["127.0.0.1:123"].eq(&one));
        assert!(map["127.0.0.1:123"].eq(map["[::1]:123"]));

        Ok(())
    }

    #[tokio::test]
    async fn detect_fastest_addr_just_some() -> YdbResult<()> {
        let l1 = TcpListener::bind("127.0.0.1:0").await?;
        let l2 = TcpListener::bind("127.0.0.1:0").await?;
        let l3 = TcpListener::bind("127.0.0.1:0").await?;

        let l1_addr = l1.local_addr()?;
        let l2_addr = l2.local_addr()?;
        let l3_addr = l3.local_addr()?;

        println!("Listener №1 on: {}", l1_addr);
        println!("Listener №2 on: {}", l2_addr);
        println!("Listener №3 on: {}", l3_addr);

        let nodes = vec![
            l1_addr.to_string(),
            l2_addr.to_string(),
            l3_addr.to_string(),
        ];

        for _ in 0..100 {
            let addr = NearestDCBalancer::find_fastest_address(
                nodes.iter().collect_vec(),
                Duration::from_secs(PING_TIMEOUT_SECS),
            )
            .await?;
            assert!(nodes.contains(&addr))
        }

        Ok(())
    }

    #[tokio::test]
    async fn detect_fastest_addr_with_fault() -> YdbResult<()> {
        let l1 = TcpListener::bind("127.0.0.1:0").await?;
        let l2 = TcpListener::bind("127.0.0.1:0").await?;
        let l3 = TcpListener::bind("127.0.0.1:0").await?;

        let l1_addr = l1.local_addr()?;
        let l2_addr = l2.local_addr()?;
        let l3_addr = l3.local_addr()?;

        println!("Listener №1 on: {}", l1_addr);
        println!("Listener №2 on: {}", l2_addr);
        println!("Listener №3 on: {}", l3_addr);

        let nodes = vec![
            l1_addr.to_string(),
            l2_addr.to_string(),
            l3_addr.to_string(),
        ];

        drop(l1);

        for _ in 0..100 {
            let addr = NearestDCBalancer::find_fastest_address(
                nodes.iter().collect_vec(),
                Duration::from_secs(PING_TIMEOUT_SECS),
            )
            .await?;
            assert!(nodes.contains(&addr) && addr != l1_addr.to_string())
        }

        Ok(())
    }

    #[tokio::test]
    async fn detect_fastest_addr_one_alive() -> YdbResult<()> {
        let l1 = TcpListener::bind("127.0.0.1:0").await?;
        let l2 = TcpListener::bind("127.0.0.1:0").await?;
        let l3 = TcpListener::bind("127.0.0.1:0").await?;

        let l1_addr = l1.local_addr()?;
        let l2_addr = l2.local_addr()?;
        let l3_addr = l3.local_addr()?;

        println!("Listener №1 on: {}", l1_addr);
        println!("Listener №2 on: {}", l2_addr);
        println!("Listener №3 on: {}", l3_addr);

        let nodes = vec![
            l1_addr.to_string(),
            l2_addr.to_string(),
            l3_addr.to_string(),
        ];

        drop(l1);
        drop(l2);

        for _ in 0..100 {
            let addr = NearestDCBalancer::find_fastest_address(
                nodes.iter().collect_vec(),
                Duration::from_secs(PING_TIMEOUT_SECS),
            )
            .await?;
            assert!(addr == l3_addr.to_string())
        }

        Ok(())
    }

    #[tokio::test]
    async fn detect_fastest_addr_timeout() -> YdbResult<()> {
        let l1 = TcpListener::bind("127.0.0.1:0").await?;
        let l2 = TcpListener::bind("127.0.0.1:0").await?;
        let l3 = TcpListener::bind("127.0.0.1:0").await?;

        let l1_addr = l1.local_addr()?;
        let l2_addr = l2.local_addr()?;
        let l3_addr = l3.local_addr()?;

        println!("Listener №1 on: {}", l1_addr);
        println!("Listener №2 on: {}", l2_addr);
        println!("Listener №3 on: {}", l3_addr);

        let nodes = vec![
            l1_addr.to_string(),
            l2_addr.to_string(),
            l3_addr.to_string(),
        ];

        drop(l1);
        drop(l2);
        drop(l3);

        let result = NearestDCBalancer::find_fastest_address(
            nodes.iter().collect_vec(),
            Duration::from_secs(3),
        )
        .await;
        match result {
            Ok(_) => unreachable!(),
            Err(err) => {
                assert_eq!(
                    err.to_string(),
                    "Custom(\"timeout while detecting fastest address\")"
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn no_addr_timeout() -> YdbResult<()> {
        let result =
            NearestDCBalancer::find_fastest_address(Vec::new(), Duration::from_secs(3)).await;
        match result {
            Ok(_) => unreachable!(),
            Err(err) => {
                assert_eq!(
                    err.to_string(),
                    "Custom(\"timeout while detecting fastest address\")"
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn detect_fastest_addr() -> YdbResult<()> {
        let l1 = TcpListener::bind("127.0.0.1:0").await?;
        let l2 = TcpListener::bind("127.0.0.1:0").await?;
        let l3 = TcpListener::bind("127.0.0.1:0").await?;

        let l1_addr = l1.local_addr()?;
        let l2_addr = l2.local_addr()?;
        let l3_addr = l3.local_addr()?;

        println!("Listener №1 on: {}", l1_addr);
        println!("Listener №2 on: {}", l2_addr);
        println!("Listener №3 on: {}", l3_addr);

        let nodes = vec![
            l1_addr.to_string(),
            l2_addr.to_string(),
            l3_addr.to_string(),
        ];

        drop(l1);
        drop(l2);
        drop(l3);

        let result = NearestDCBalancer::find_fastest_address(
            nodes.iter().collect_vec(),
            Duration::from_secs(3),
        )
        .await;
        match result {
            Ok(_) => unreachable!(),
            Err(err) => {
                assert_eq!(
                    err.to_string(),
                    "Custom(\"timeout while detecting fastest address\")"
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn adjusting_dc() -> YdbResult<()> {
        let l1 = TcpListener::bind("127.0.0.1:0").await?;
        let l2 = TcpListener::bind("127.0.0.1:0").await?;
        let l3 = TcpListener::bind("127.0.0.1:0").await?;

        let l1_addr = l1.local_addr()?;
        let l2_addr = l2.local_addr()?;
        let l3_addr = l3.local_addr()?;

        println!("Listener №1 on: {}", l1_addr);
        println!("Listener №2 on: {}", l2_addr);
        println!("Listener №3 on: {}", l3_addr);

        let discovery_state = Arc::new(DiscoveryState::default());
        let balancer_state = Arc::new(Mutex::new(BalancerState::default()));
        let balancer_state_updater = balancer_state.clone();
        let (state_sender, state_reciever) = watch::channel(discovery_state.clone());

        let ping_token = CancellationToken::new();
        let ping_token_clone = ping_token.clone();

        let waiter = Arc::new(WaiterImpl::new());
        let waiter_clone = waiter.clone();

        let updater = tokio::spawn(async move {
            NearestDCBalancer::adjust_local_dc(
                balancer_state_updater,
                state_reciever,
                ping_token_clone,
                waiter_clone,
            )
            .await
        });

        let updated_state = Arc::new(
            DiscoveryState::default()
                .with_node_info(
                    Table,
                    NodeInfo::new(
                        Uri::from_str(l1_addr.to_string().as_str()).unwrap(),
                        "A".to_string(),
                    ),
                )
                .with_node_info(
                    Table,
                    NodeInfo::new(
                        Uri::from_str(l2_addr.to_string().as_str()).unwrap(),
                        "B".to_string(),
                    ),
                )
                .with_node_info(
                    Table,
                    NodeInfo::new(
                        Uri::from_str(l2_addr.to_string().as_str()).unwrap(),
                        "C".to_string(),
                    ),
                ),
        );
        assert!(
            (balancer_state.lock().await)
                .borrow()
                .preferred_endpoints
                .len()
                == 0 // no endpoints
        );
        let _ = state_sender.send(updated_state);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_true!(timeout(Duration::from_secs(3), waiter.wait()).await.is_ok()); // should not wait
        assert!(
            (balancer_state.lock().await)
                .borrow()
                .preferred_endpoints
                .len()
                == 1 // only one endpoint in each dc
        );
        let updated_state_next = Arc::new(
            DiscoveryState::default()
                .with_node_info(
                    Table,
                    NodeInfo::new(
                        Uri::from_str(l1_addr.to_string().as_str()).unwrap(),
                        "A".to_string(),
                    ),
                )
                .with_node_info(
                    Table,
                    NodeInfo::new(
                        Uri::from_str(l2_addr.to_string().as_str()).unwrap(),
                        "A".to_string(),
                    ),
                ),
        );
        let _ = state_sender.send(updated_state_next);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_true!(timeout(Duration::from_secs(3), waiter.wait()).await.is_ok()); // should not wait
        assert!(
            (balancer_state.lock().await)
                .borrow()
                .preferred_endpoints
                .len()
                == 2 // both endpoints in same dc
        );
        ping_token.cancel(); // reciever stops wait for state change
        let _ = tokio::join!(updater); // should join
        Ok(())
    }

    #[tokio::test]
    async fn nearest_dc_balancer_integration_with_error_fallback() -> YdbResult<()> {
        let balancer = NearestDCBalancer::new(BalancerConfig {
            fallback_strategy: FallbackStrategy::Error,
        })
        .unwrap();

        let sh = SharedLoadBalancer::new_with_balancer(Box::new(balancer));

        match sh.endpoint(Table) {
            Ok(_) => unreachable!(),
            Err(err) => assert_eq!(
                err.to_string(),
                "Custom(\"no available endpoints for service:table_service\")".to_string()
            ),
        }
        Ok(())
    }

    #[tokio::test]
    async fn nearest_dc_balancer_integration_with_other_fallback_error() -> YdbResult<()> {
        let balancer = NearestDCBalancer::new(BalancerConfig::default()).unwrap();

        let sh = SharedLoadBalancer::new_with_balancer(Box::new(balancer));

        match sh.endpoint(Table) {
            Ok(_) => unreachable!(),
            Err(err) => assert_eq!(
                err.to_string(),
                "Custom(\"empty endpoint list for service: table_service\")".to_string()
            ),
        }
        Ok(())
    }

    #[tokio::test]
    async fn nearest_dc_balancer_integration() -> YdbResult<()> {
        let l1 = TcpListener::bind("127.0.0.1:0").await?;
        let l2 = TcpListener::bind("127.0.0.1:0").await?;

        let l1_addr = l1.local_addr()?;
        let l2_addr = l2.local_addr()?;

        println!("Listener №1 on: {}", l1_addr);
        println!("Listener №2 on: {}", l2_addr);

        let balancer = NearestDCBalancer::new(BalancerConfig {
            fallback_strategy: FallbackStrategy::Error,
        })
        .unwrap();

        let sh = SharedLoadBalancer::new_with_balancer(Box::new(balancer));
        let self_updater = sh.clone();
        let (state_sender, state_reciever) =
            watch::channel::<Arc<DiscoveryState>>(Arc::new(DiscoveryState::default()));

        tokio::spawn(async move { update_load_balancer(self_updater, state_reciever).await });

        match sh.endpoint(Table) {
            Ok(_) => unreachable!(),
            Err(err) => assert_eq!(
                err.to_string(),
                "Custom(\"no available endpoints for service:table_service\")".to_string()
            ),
        }

        let updated_state = Arc::new(
            DiscoveryState::default()
                .with_node_info(
                    Table,
                    NodeInfo::new(
                        Uri::from_str(l1_addr.to_string().as_str()).unwrap(),
                        "A".to_string(),
                    ),
                )
                .with_node_info(
                    Table,
                    NodeInfo::new(
                        Uri::from_str(l2_addr.to_string().as_str()).unwrap(),
                        "A".to_string(),
                    ),
                ),
        );

        let _ = state_sender.send(updated_state);

        sh.wait().await?;

        match sh.endpoint(Table) {
            Ok(uri) => {
                let addr = uri.host().unwrap();
                assert!(addr == "127.0.0.1" || addr == "[::1]")
            }
            Err(err) => unreachable!("{}", err.to_string()),
        }
        Ok(())
    }
}
