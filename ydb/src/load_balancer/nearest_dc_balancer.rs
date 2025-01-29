use std::{
    borrow::{Borrow, BorrowMut},
    collections::HashMap,
    sync::Arc,
};

use http::Uri;
use itertools::Itertools;
use rand::{seq::SliceRandom, thread_rng};
use std::sync::RwLock;
use std::time::Duration;
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::{
        broadcast, mpsc,
        watch::{self, Sender},
    },
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    discovery::NodeInfo,
    grpc_wrapper::raw_services::Service,
    waiter::{AllWaiter, WaiterImpl},
    DiscoveryState, Waiter, YdbError, YdbResult,
};

use super::{random_balancer::RandomLoadBalancer, LoadBalancer};
pub(crate) struct BalancerConfig {
    pub(super) fallback_strategy: FallbackStrategy,
}

#[derive(Default)]
pub(super) struct BalancerState {
    pub(super) preferred_endpoints: Vec<NodeInfo>,
}

// What will balancer do if there is no available endpoints at local dc
#[allow(dead_code)]
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
    state_sender: Sender<Arc<DiscoveryState>>,
    adjusting_proccess_token: CancellationToken,
    waiter: Arc<WaiterImpl>,
    config: BalancerConfig,
    balancer_state: Arc<RwLock<BalancerState>>,
}

#[allow(dead_code)]
impl NearestDCBalancer {
    pub(crate) fn new(config: BalancerConfig) -> YdbResult<Self> {
        let discovery_state = Arc::new(DiscoveryState::default());
        let balancer_state = Arc::new(RwLock::new(BalancerState::default()));
        let balancer_state_updater = balancer_state.clone();
        let (state_sender, state_reciever) = watch::channel(discovery_state.clone());

        let adjusting_proccess_token = CancellationToken::new();
        let adjusting_proccess_token_clone = adjusting_proccess_token.clone();

        let waiter = Arc::new(WaiterImpl::new());
        let waiter_clone = waiter.clone();

        tokio::spawn(async move {
            Self::adjust_local_dc(
                balancer_state_updater,
                state_reciever,
                adjusting_proccess_token_clone,
                waiter_clone,
            )
            .await
        });

        Ok(Self {
            state_sender,
            adjusting_proccess_token,
            waiter,
            config,
            balancer_state,
        })
    }
}

impl Drop for NearestDCBalancer {
    fn drop(&mut self) {
        self.adjusting_proccess_token.cancel();
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

pub(super) const NODES_PER_DC: usize = 5;
pub(super) const PING_TIMEOUT_SECS: u64 = 60;

#[allow(dead_code)]
impl NearestDCBalancer {
    fn get_endpoint(&self, service: Service) -> YdbResult<Uri> {
        match self.balancer_state.read() {
            // Fast lock
            Ok(state_guard) => {
                if let Some(node) = state_guard
                    .borrow()
                    .preferred_endpoints
                    .choose(&mut thread_rng())
                {
                    return YdbResult::Ok(node.uri.clone());
                }
            }
            Err(err) => {
                error!("error on get_endpoint:{}", err);
                return Err(YdbError::Custom(
                    "could not acquire mutex on get_endpoint".to_string(),
                ));
            }
        };
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

    pub(super) async fn adjust_local_dc(
        balancer_state: Arc<RwLock<BalancerState>>,
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
                    dc_to_nodes.values_mut().for_each(|endpoints| {
                        to_check.append(Self::get_random_endpoints(endpoints))
                    });
                    match Self::find_local_dc(&to_check).await {
                        Ok(dc) => {
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
        balancer_state: &Arc<RwLock<BalancerState>>,
        new_nodes: &[NodeInfo],
        local_dc: String,
    ) {
        let new_preferred_endpoints = new_nodes
            .iter()
            .filter(|ep| ep.location == local_dc)
            .cloned()
            .collect_vec();
        // Fast lock
        match balancer_state.write() {
            Ok(mut guard) => guard.borrow_mut().preferred_endpoints = new_preferred_endpoints,
            Err(err) => {
                error!("error on adjust_preferred_endpoints:{}", err);
            }
        }
    }

    pub(super) fn extract_nodes(from_state: &Arc<DiscoveryState>) -> YdbResult<&Vec<NodeInfo>> {
        let nodes = from_state.get_all_nodes();
        match nodes {
            None => Err(YdbError::Custom(
                "no endpoints on discovery update".to_string(),
            )),
            Some(nodes) => Ok(nodes),
        }
    }

    pub(super) fn split_endpoints_by_location(
        nodes: &[NodeInfo],
    ) -> HashMap<String, Vec<&NodeInfo>> {
        let mut dc_to_eps = HashMap::<String, Vec<&NodeInfo>>::new();
        nodes.iter().for_each(|info| {
            if let Some(nodes) = dc_to_eps.get_mut(&info.location) {
                nodes.push(info);
            } else {
                dc_to_eps.insert(info.location.clone(), vec![info]);
            }
        });
        dc_to_eps
    }

    pub(super) fn get_random_endpoints<'a>(
        dc_endpoints: &'a mut Vec<&'a NodeInfo>,
    ) -> &'a mut Vec<&'a NodeInfo> {
        dc_endpoints.shuffle(&mut thread_rng());
        dc_endpoints.truncate(NODES_PER_DC);
        dc_endpoints
    }

    pub(super) async fn find_local_dc(to_check: &[&NodeInfo]) -> YdbResult<String> {
        let addr_to_node = Self::addr_to_node(to_check);
        if addr_to_node.is_empty() {
            return Err(YdbError::Custom(
                "no ip addresses for endpoints".to_string(),
            ));
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

    pub(super) fn addr_to_node<'a>(nodes: &[&'a NodeInfo]) -> HashMap<String, &'a NodeInfo> {
        let mut addr_to_node = HashMap::<String, &NodeInfo>::with_capacity(2 * nodes.len()); // ipv4 + ipv6
        nodes.iter().for_each(|info| {
            let host = match info.uri.host() {
                Some(uri_host) => uri_host,
                None => {
                    warn!("no host for uri:{}", info.uri);
                    return;
                }
            };
            let port = match info.uri.port() {
                Some(uri_port) => uri_port.as_u16(),
                None => {
                    warn!("no port for uri:{}", info.uri);
                    return;
                }
            };
            use std::net::ToSocketAddrs;
            let _ = (host, port).to_socket_addrs().map(|addrs| {
                for addr in addrs {
                    addr_to_node.insert(addr.to_string(), info);
                }
            });
        });
        addr_to_node
    }

    pub(super) async fn find_fastest_address(
        addrs: Vec<&String>,
        timeout: Duration,
    ) -> YdbResult<String> {
        // Cancellation flow: timeout -> address collector -> address producers
        let interrupt_via_timeout = CancellationToken::new();
        let interrupt_collector_future = interrupt_via_timeout.child_token();
        let stop_measure = interrupt_collector_future.child_token();

        let (start_measure, _) = broadcast::channel::<()>(1);
        let buffer_cap = if addrs.is_empty() { 1 } else { addrs.len() };
        let (addr_sender, mut addr_reciever) = mpsc::channel::<String>(buffer_cap);
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
                        if let Ok(mut connection) =  connection_result{
                            let _ = connection.shutdown().await;
                            let _ = addr_sender.send(addr).await;
                        }
                        // Send nothing if connection is faulty
                    }
                    _ = stop_measure.cancelled() => {
                         // Also do nothing if there is request to stop pings (balancer already got fastest address)
                    }
                }
            });
        }

        let wait_first_some_or_cancel = async {
            tokio::select! {
                biased; // check timeout first
                _ = interrupt_collector_future.cancelled() =>{
                    Self::join_all(&mut nursery).await; // Children will be cancelled due to tokens chaining
                    YdbResult::Err("cancelled".into())
                }
                address_option = addr_reciever.recv() =>{
                    match address_option {
                        Some(address) => {
                            interrupt_collector_future.cancel(); // Cancel other producing children
                            Self::join_all(&mut nursery).await;
                            YdbResult::Ok(address)
                        },
                        None => Err(YdbError::Custom("no fastest address".into())), // Channel closed, all producers have done measures
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
