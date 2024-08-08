use std::collections::HashSet;
use std::str::FromStr;
use std::sync::{Arc, RwLock, RwLockWriteGuard, Weak};

use async_trait::async_trait;
use http::uri::Authority;
use http::Uri;

use crate::errors::YdbResult;

use crate::waiter::Waiter;

use derivative::Derivative;
use itertools::Itertools;
use std::time::Duration;
use tokio::sync::watch::Receiver;
use tokio::sync::{watch, Mutex};

use crate::grpc_connection_manager::GrpcConnectionManager;

use crate::grpc_wrapper::raw_discovery_client::{EndpointInfo, GrpcDiscoveryClient};
use crate::grpc_wrapper::raw_services::Service;
use tracing::trace;

/// Current discovery state
#[derive(Clone, Debug, PartialEq)]
pub struct DiscoveryState {
    pub(crate) timestamp: std::time::Instant,
    nodes: Vec<NodeInfo>,

    pessimized_nodes: HashSet<Uri>,
    original_nodes: Vec<NodeInfo>,
}

impl DiscoveryState {
    pub(crate) fn new(timestamp: std::time::Instant, nodes: Vec<NodeInfo>) -> Self {
        let mut state = DiscoveryState {
            timestamp,
            nodes: Vec::new(),
            pessimized_nodes: HashSet::new(),
            original_nodes: nodes,
        };
        state.build_services();
        state
    }

    fn build_services(&mut self) {
        self.nodes.clear();

        for origin_node in self.original_nodes.iter() {
            if !self.pessimized_nodes.contains(&origin_node.uri) {
                self.nodes.push(origin_node.clone())
            }
        }

        // if all nodes pessimized - use full nodes set
        if self.nodes.is_empty() {
            self.nodes.clone_from(&self.original_nodes)
        }
    }

    pub(crate) fn get_nodes(&self, _service: &Service) -> Option<&Vec<NodeInfo>> {
        Some(&self.nodes)
    }

    pub(crate) fn get_all_nodes(&self) -> Option<&Vec<NodeInfo>> {
        Some(&self.nodes)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.nodes.len() == 0
    }

    // pessimize return true if state was changed
    pub(crate) fn pessimize(&mut self, uri: &Uri) -> bool {
        if self.pessimized_nodes.contains(uri) {
            return false;
        };

        self.pessimized_nodes.insert(uri.clone());
        self.build_services();
        true
    }

    // TODO: uncomment if need in read code or remove test
    #[cfg(test)]
    pub(crate) fn with_node_info(mut self, _service: Service, node_info: NodeInfo) -> Self {
        if !self.nodes.contains(&node_info) {
            self.nodes.push(node_info);
        }
        self
    }
}

impl Default for DiscoveryState {
    fn default() -> Self {
        DiscoveryState::new(std::time::Instant::now(), Vec::default())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NodeInfo {
    pub(crate) uri: Uri,
    pub(crate) location: String,
}

impl NodeInfo {
    pub(crate) fn new(uri: Uri, location: String) -> Self {
        Self { uri, location }
    }
}

/// Discovery YDB endpoints
#[async_trait]
pub trait Discovery: Send + Sync + Waiter {
    /// Pessimize the endpoint
    fn pessimization(&self, uri: &Uri);

    /// Subscribe to discovery changes
    fn subscribe(&self) -> tokio::sync::watch::Receiver<Arc<DiscoveryState>>;

    /// Get current discovery state
    fn state(&self) -> Arc<DiscoveryState>;
}

/// Always discovery once static node
///
/// Not used in prod, but may be good for tests
pub struct StaticDiscovery {
    sender: tokio::sync::watch::Sender<Arc<DiscoveryState>>,
    discovery_state: Arc<DiscoveryState>,
}

/// Stub discovery pointed to one endpoint for all services.
///
/// Example:
/// ```no_run
/// # use ydb::{ClientBuilder, StaticDiscovery, YdbResult};
///
/// # fn main()->YdbResult<()>{
/// let discovery = StaticDiscovery::new_from_str("grpc://localhost:2136")?;
/// let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/?database=/local")?.with_discovery(discovery).client()?;
/// # return Ok(());
/// # }
/// ```
impl StaticDiscovery {
    pub fn new_from_str<'a, T: Into<&'a str>>(endpoint: T) -> YdbResult<Self> {
        let endpoint = Uri::from_str(endpoint.into())?;
        let nodes = vec![NodeInfo::new(endpoint, String::new())];

        let state = DiscoveryState::new(std::time::Instant::now(), nodes);
        let state = Arc::new(state);
        let (sender, _) = tokio::sync::watch::channel(state.clone());
        Ok(StaticDiscovery {
            sender,
            discovery_state: state,
        })
    }
}

#[async_trait]
impl Discovery for StaticDiscovery {
    fn pessimization(&self, _uri: &Uri) {
        // pass
    }

    fn subscribe(&self) -> Receiver<Arc<DiscoveryState>> {
        self.sender.subscribe()
    }

    fn state(&self) -> Arc<DiscoveryState> {
        self.discovery_state.clone()
    }
}

#[async_trait]
impl Waiter for StaticDiscovery {
    async fn wait(&self) -> YdbResult<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct TimerDiscovery {
    state: Arc<DiscoverySharedState>,
}

impl TimerDiscovery {
    #[allow(dead_code)]
    pub(crate) fn new(
        connection_manager: GrpcConnectionManager,
        endpoint: &str,
        interval: Duration,
        token_waiter: Box<dyn Waiter>,
    ) -> YdbResult<Self> {
        let state = Arc::new(DiscoverySharedState::new(connection_manager, endpoint)?);
        let state_weak = Arc::downgrade(&state);
        tokio::spawn(async move {
            trace!("timer discovery wait token");
            let result = token_waiter.wait().await;
            trace!("timer discovery first token done with result: {:?}", result);
            drop(token_waiter);
            DiscoverySharedState::background_discovery(state_weak, interval).await;
        });
        Ok(TimerDiscovery { state })
    }

    #[allow(dead_code)]
    async fn discovery_now(&self) -> YdbResult<()> {
        self.state.discovery_now().await
    }
}

impl Discovery for TimerDiscovery {
    fn pessimization(&self, uri: &Uri) {
        self.state.pessimization(uri);

        // check if need force discovery
        let state = self.state();
        let pessimized_nodes_count = state
            .original_nodes
            .iter()
            .filter(|node| state.pessimized_nodes.contains(&node.uri))
            .count();
        if pessimized_nodes_count > 0 && pessimized_nodes_count >= state.original_nodes.len() / 2 {
            let shared_state_for_discovery = Arc::downgrade(&self.state);
            tokio::spawn(async move {
                if let Some(state) = shared_state_for_discovery.upgrade() {
                    let _ = state.discovery_now().await;
                }
            });
        }
    }

    fn subscribe(&self) -> Receiver<Arc<DiscoveryState>> {
        self.state.subscribe()
    }

    fn state(&self) -> Arc<DiscoveryState> {
        self.state.state()
    }
}

#[async_trait::async_trait]
impl Waiter for TimerDiscovery {
    async fn wait(&self) -> YdbResult<()> {
        self.state.wait().await
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
struct DiscoverySharedState {
    #[derivative(Debug = "ignore")]
    connection_manager: GrpcConnectionManager,
    discovery_uri: Uri,
    sender: tokio::sync::watch::Sender<Arc<DiscoveryState>>,

    discovery_process: Mutex<()>,
    discovery_state: RwLock<Arc<DiscoveryState>>,

    state_received: watch::Receiver<bool>,
    state_received_sender: watch::Sender<bool>,
}

impl DiscoverySharedState {
    fn new(connection_manager: GrpcConnectionManager, endpoint: &str) -> YdbResult<Self> {
        let state = Arc::new(DiscoveryState::new(std::time::Instant::now(), Vec::new()));
        let (sender, _) = watch::channel(state.clone());
        let (state_received_sender, state_received) = watch::channel(false);
        Ok(Self {
            connection_manager,
            discovery_uri: http::Uri::from_str(endpoint)?,
            sender,
            discovery_process: Mutex::new(()),
            discovery_state: RwLock::new(state),
            state_received,
            state_received_sender,
        })
    }

    #[tracing::instrument(skip(self))]
    async fn discovery_now(&self) -> YdbResult<()> {
        trace!("discovery locking");
        let discovery_lock = self.discovery_process.lock().await;

        trace!("creating grpc client");
        let start = std::time::Instant::now();
        let mut discovery_client = self
            .connection_manager
            .get_auth_service_to_node(GrpcDiscoveryClient::new, &self.discovery_uri)
            .await?;

        let res = discovery_client
            .list_endpoints(self.connection_manager.database().clone())
            .await?;
        let new_endpoints = Self::list_endpoints_to_node_infos(res)?;
        self.set_discovery_state(
            self.discovery_state.write().unwrap(),
            Arc::new(DiscoveryState::new(start, new_endpoints)),
        );

        // lock until exit
        drop(discovery_lock);
        Ok(())
    }

    fn set_discovery_state(
        &self,
        mut locked_state: RwLockWriteGuard<Arc<DiscoveryState>>,
        new_state: Arc<DiscoveryState>,
    ) {
        *locked_state = new_state.clone();
        let _ = self.sender.send(new_state);
        let _ = self.state_received_sender.send(true);
    }

    #[tracing::instrument(skip(state))]
    async fn background_discovery(state: Weak<DiscoverySharedState>, interval: Duration) {
        while let Some(state) = state.upgrade() {
            trace!("rekby-discovery");
            let res = state.discovery_now().await;
            trace!("rekby-res: {:?}", res);
            // return;
            tokio::time::sleep(interval).await;
        }
        trace!("stop background_discovery");
    }

    fn list_endpoints_to_node_infos(list: Vec<EndpointInfo>) -> YdbResult<Vec<NodeInfo>> {
        list.into_iter()
            .map(|item| match Self::endpoint_info_to_uri(&item) {
                Ok(uri) => YdbResult::<NodeInfo>::Ok(NodeInfo::new(uri, item.location.clone())),
                Err(err) => YdbResult::<NodeInfo>::Err(err),
            })
            .try_collect()
    }

    fn endpoint_info_to_uri(endpoint_info: &EndpointInfo) -> YdbResult<Uri> {
        let authority: Authority =
            Authority::from_str(format!("{}:{}", endpoint_info.fqdn, endpoint_info.port).as_str())?;

        Ok(Uri::builder()
            .scheme(if endpoint_info.ssl { "https" } else { "http" })
            .authority(authority)
            .path_and_query("")
            .build()?)
    }
}

#[async_trait]
impl Discovery for DiscoverySharedState {
    fn pessimization(&self, uri: &Uri) {
        // TODO: suppress force copy every time
        let lock = self.discovery_state.write().unwrap();
        let mut discovery_state = lock.as_ref().clone();
        if !discovery_state.pessimize(uri) {
            return;
        }
        let discovery_state = Arc::new(discovery_state);
        self.set_discovery_state(lock, discovery_state);
    }

    fn subscribe(&self) -> Receiver<Arc<DiscoveryState>> {
        self.sender.subscribe()
    }

    fn state(&self) -> Arc<DiscoveryState> {
        return self.discovery_state.read().unwrap().clone();
    }
}

#[async_trait::async_trait]
impl Waiter for DiscoverySharedState {
    async fn wait(&self) -> YdbResult<()> {
        trace!("start discovery shared state");
        let mut channel = self.state_received.clone();
        loop {
            trace!("loop");
            if *channel.borrow_and_update() {
                trace!("return ok");
                return Ok(());
            }
            channel.changed().await?
        }
    }
}

#[cfg(test)]
mod test {
    use crate::client_common::{DBCredentials, TokenCache};
    use crate::discovery::DiscoverySharedState;
    use crate::errors::YdbResult;
    use crate::grpc_connection_manager::GrpcConnectionManager;
    use crate::grpc_wrapper::auth::AuthGrpcInterceptor;
    use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
    use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
    use crate::test_helpers::test_client_builder;
    use http::Uri;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    #[ignore] // need YDB access
    async fn test_background_discovery() -> YdbResult<()> {
        let cred = DBCredentials {
            database: test_client_builder().database.clone(),
            token_cache: tokio::task::spawn_blocking(|| {
                TokenCache::new(test_client_builder().credentials.clone())
            })
            .await??,
        };

        let uri = Uri::from_str(test_client_builder().endpoint.as_str())?;
        let load_balancer =
            SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(uri)));

        let interceptor =
            MultiInterceptor::new().with_interceptor(AuthGrpcInterceptor::new(cred.clone())?);

        let connection_manager =
            GrpcConnectionManager::new(load_balancer, cred.database, interceptor, None);

        let discovery_shared =
            DiscoverySharedState::new(connection_manager, test_client_builder().endpoint.as_str())?;

        let state = Arc::new(discovery_shared);
        let mut rx = state.sender.subscribe();
        // skip initial value
        rx.borrow_and_update();

        let state_weak = Arc::downgrade(&state);
        tokio::spawn(async {
            DiscoverySharedState::background_discovery(state_weak, Duration::from_millis(50)).await;
        });

        // wait two updates
        for _ in 0..2 {
            rx.changed().await.unwrap();
            assert!(!rx.borrow().nodes.is_empty());
        }

        Ok(())
    }
}
