use crate::discovery::{Discovery, DiscoveryState, NodeInfo};
use crate::errors::*;
use crate::grpc_wrapper::raw_services::Service;
use crate::waiter::{AllWaiter, Waiter, WaiterImpl};
use http::Uri;
use itertools::Itertools;
use rand::thread_rng;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::watch::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Timeout;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::{error, warn};

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
    state_sender: Sender<Arc<DiscoveryState>>,
    adjust_local_dc_process_control: CancellationToken,
    waiter: Arc<WaiterImpl>,
    config: BalancerConfig,
    preferred_endpoints: Vec<NodeInfo>,
    location: Arc<Mutex<String>>,
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

        let discovery_state = Arc::new(DiscoveryState::default());
        let self_location = Arc::new(Mutex::new(String::new()));
        let location_updater = self_location.clone();
        let (state_sender, state_reciever) = watch::channel(discovery_state.clone());
        let adjust_local_dc_process_control = CancellationToken::new();
        let adjust_local_dc_process_control_clone = adjust_local_dc_process_control.clone();

        tokio::spawn(async move {
            Self::adjust_local_dc(
                location_updater,
                state_reciever,
                adjust_local_dc_process_control_clone,
            )
            .await
        });

        Ok(Self {
            discovery_state,
            state_sender,
            adjust_local_dc_process_control,
            waiter: Arc::new(WaiterImpl::new()),
            config,
            preferred_endpoints: Vec::new(),
            location: Arc::new(Mutex::new(String::new())),
        })
    }
}

impl Drop for NearestDCBalancer {
    fn drop(&mut self) {
        self.adjust_local_dc_process_control.cancel();
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
        let _ = self.state_sender.send(discovery_state.clone());
        self.adjust_preferred_endpoints()?;
        if !self.discovery_state.is_empty() {
            self.waiter.set_received(Ok(()))
        }
        Ok(())
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
const PING_TIMEOUT_SECS: u64 = 60;

impl NearestDCBalancer {
    fn get_endpoint(&self, service: Service) -> YdbResult<Uri> {
        for ep in self.preferred_endpoints.iter() {
            return YdbResult::Ok(ep.uri.clone());
        }

        match self.config.fallback_strategy {
            FallbackStrategy::Error => Err(YdbError::custom(format!(
                "no available endpoints for service:{}",
                service
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

    fn adjust_preferred_endpoints(&mut self) -> YdbResult<()> {
        let location = match self.location.try_lock() {
            Ok(location_guard) => (*location_guard).clone(),
            Err(_) => {
                info!("could not acquire lock on location");
                "".into()
            }
        };
        self.preferred_endpoints = Self::extract_nodes(&self.discovery_state)?
            .into_iter()
            .filter(|ep| ep.location == *location)
            .map(|ep| ep.clone())
            .collect_vec();
        Ok(())
    }

    async fn adjust_local_dc(
        self_location: Arc<Mutex<String>>,
        mut state_reciever: watch::Receiver<Arc<DiscoveryState>>,
        stop_ping_process: CancellationToken,
    ) {
        loop {
            let new_state = state_reciever.borrow_and_update().clone();
            match Self::extract_nodes(&new_state) {
                Ok(some_nodes) => {
                    let mut dc_to_nodes = Self::split_endpoints_by_location(some_nodes);
                    let mut to_check = Vec::with_capacity(NODES_PER_DC * dc_to_nodes.keys().len());
                    dc_to_nodes.iter_mut().for_each(|(_, endpoints)| {
                        to_check.append(Self::get_random_endpoints(endpoints))
                    });
                    match Self::find_local_dc(&to_check).await {
                        Ok(dc) => {
                            info!("found new local dc:{}", dc);
                            *self_location.lock().await = dc; // fast lock
                        }
                        Err(err) => {
                            error!("error on search local dc:{}", err);
                            continue;
                        }
                    }
                }
                Err(_) => continue,
            }
            if state_reciever.changed().await.is_err() {
                return;
            }
            if stop_ping_process.is_cancelled() {
                return;
            }
        }
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
        use rand::seq::SliceRandom;
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
        match Self::find_fastest_address(addrs.collect()).await {
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

    async fn find_fastest_address(addrs: Vec<&String>) -> YdbResult<String> {
        // Cancellation flow: timeout -> address collector -> address producers
        let interrupt_via_timeout = CancellationToken::new();
        let interrupt_collector_future = interrupt_via_timeout.child_token();
        let stop_measure = interrupt_collector_future.child_token(); // (*)

        let (start_measure, _) = tokio::sync::broadcast::channel::<()>(1);
        let (addr_sender, mut addr_reciever) = tokio::sync::mpsc::channel::<Option<String>>(1);
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

        match tokio::time::timeout(
            Duration::from_secs(PING_TIMEOUT_SECS),
            wait_first_some_or_cancel,
        )
        .await
        {
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
