use std::collections::HashSet;
use std::str::FromStr;
use std::sync::{Arc, RwLock, RwLockWriteGuard, Weak};

use async_trait::async_trait;
use http::uri::Authority;
use http::Uri;
use strum::{Display, EnumIter, EnumString};

use ydb_protobuf::ydb_proto::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::ydb_proto::discovery::{EndpointInfo, ListEndpointsRequest, ListEndpointsResult};

use crate::errors::YdbResult;
use crate::internal::client_common::DBCredentials;
use crate::internal::grpc::{create_grpc_client, grpc_read_operation_result};
use crate::internal::waiter::Waiter;

use std::time::Duration;
use tokio::sync::watch::Receiver;
use tokio::sync::{watch, Mutex};

use tracing::{instrument, trace};

#[allow(dead_code)]
#[derive(Clone, Copy, Display, Debug, EnumIter, EnumString, Eq, Hash, PartialEq)]
pub(crate) enum Service {
    #[strum(serialize = "discovery")]
    Discovery,

    #[strum(serialize = "export")]
    Export,

    #[strum(serialize = "import")]
    Import,

    #[strum(serialize = "scripting")]
    Scripting,

    #[strum(serialize = "table_service")]
    Table,
}

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
        return state;
    }

    fn build_services(&mut self) {
        self.nodes.clear();

        for origin_node in self.original_nodes.iter() {
            if !self.pessimized_nodes.contains(&origin_node.uri) {
                self.nodes.push(origin_node.clone())
            }
        }

        // if all nodes pessimized - use full nodes set
        if self.nodes.len() == 0 {
            self.nodes.clone_from(&self.original_nodes)
        }
    }

    pub(crate) fn get_nodes(&self, _service: &Service) -> Option<&Vec<NodeInfo>> {
        Some(&self.nodes)
    }

    pub(crate) fn is_empty(&self) -> bool {
        return self.nodes.len() == 0;
    }

    // pessimize return true if state was changed
    pub(crate) fn pessimize(&mut self, uri: &Uri) -> bool {
        if self.pessimized_nodes.contains(uri) {
            return false;
        };

        self.pessimized_nodes.insert(uri.clone());
        self.build_services();
        return true;
    }

    pub(crate) fn with_node_info(mut self, _service: Service, node_info: NodeInfo) -> Self {
        if !self.nodes.contains(&node_info) {
            self.nodes.push(node_info);
        }
        return self;
    }
}

impl Default for DiscoveryState {
    fn default() -> Self {
        return DiscoveryState::new(std::time::Instant::now(), Vec::default());
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NodeInfo {
    pub(crate) uri: Uri,
}

impl NodeInfo {
    pub(crate) fn new(uri: Uri) -> Self {
        return Self { uri };
    }
}

#[async_trait]
pub trait Discovery: Send + Sync + Waiter {
    fn pessimization(&self, uri: &Uri);
    fn subscribe(&self) -> tokio::sync::watch::Receiver<Arc<DiscoveryState>>;
    fn state(&self) -> Arc<DiscoveryState>;
}

pub struct StaticDiscovery {
    sender: tokio::sync::watch::Sender<Arc<DiscoveryState>>,
    discovery_state: Arc<DiscoveryState>,
}

/// Stub discovery pointed to one endpoint for all services.
///
/// Example:
/// ```
/// # use ydb::{ClientBuilder, StaticDiscovery, YdbResult};
///
/// # fn main()->YdbResult<()>{
/// let discovery = StaticDiscovery::from_str("grpc://localhost:2136")?;
/// let client = ClientBuilder::from_str("grpc://localhost:2136/?database=/local")?.with_discovery(discovery).client()?;
/// # }
/// ```
impl StaticDiscovery {
    pub fn from_str<'a, T: Into<&'a str>>(endpoint: T) -> YdbResult<Self> {
        let endpoint = Uri::from_str(endpoint.into())?;
        let nodes = vec![NodeInfo {
            uri: endpoint.clone(),
        }];

        let state = DiscoveryState::new(std::time::Instant::now(), nodes);
        let state = Arc::new(state);
        let (sender, _) = tokio::sync::watch::channel(state.clone());
        return Ok(StaticDiscovery {
            sender,
            discovery_state: state,
        });
    }
}

#[async_trait]
impl Discovery for StaticDiscovery {
    fn pessimization(&self, _uri: &Uri) {
        // pass
    }

    fn subscribe(&self) -> Receiver<Arc<DiscoveryState>> {
        return self.sender.subscribe();
    }

    fn state(&self) -> Arc<DiscoveryState> {
        return self.discovery_state.clone();
    }
}

#[async_trait]
impl Waiter for StaticDiscovery {
    async fn wait(&self) -> YdbResult<()> {
        return Ok(());
    }
}

#[derive(Clone)]
pub(crate) struct TimerDiscovery {
    state: Arc<DiscoverySharedState>,
}

impl TimerDiscovery {
    #[allow(dead_code)]
    pub(crate) fn new(cred: DBCredentials, endpoint: &str, interval: Duration) -> YdbResult<Self> {
        let state = Arc::new(DiscoverySharedState::new(cred, endpoint)?);
        let state_weak = Arc::downgrade(&state);
        tokio::spawn(async move {
            DiscoverySharedState::background_discovery(state_weak, interval).await;
        });
        return Ok(TimerDiscovery { state });
    }

    #[allow(dead_code)]
    async fn discovery_now(&self) -> YdbResult<()> {
        return self.state.discovery_now().await;
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
                    let _ = state.discovery_now();
                }
            });
        }
    }

    fn subscribe(&self) -> Receiver<Arc<DiscoveryState>> {
        return self.state.subscribe();
    }

    fn state(&self) -> Arc<DiscoveryState> {
        return self.state.state();
    }
}

#[async_trait::async_trait]
impl Waiter for TimerDiscovery {
    async fn wait(&self) -> YdbResult<()> {
        return self.state.wait().await;
    }
}

#[derive(Debug)]
struct DiscoverySharedState {
    cred: DBCredentials,
    discovery_uri: Uri,
    sender: tokio::sync::watch::Sender<Arc<DiscoveryState>>,

    discovery_process: Mutex<()>,
    discovery_state: RwLock<Arc<DiscoveryState>>,

    state_received: watch::Receiver<bool>,
    state_received_sender: watch::Sender<bool>,
}

impl DiscoverySharedState {
    fn new(cred: DBCredentials, endpoint: &str) -> YdbResult<Self> {
        let state = Arc::new(DiscoveryState::new(std::time::Instant::now(), Vec::new()));
        let (sender, _) = watch::channel(state.clone());
        let (state_received_sender, state_received) = watch::channel(false);
        return Ok(Self {
            cred,
            discovery_uri: http::Uri::from_str(endpoint)?,
            sender,
            discovery_process: Mutex::new(()),
            discovery_state: RwLock::new(state),
            state_received,
            state_received_sender,
        });
    }

    #[tracing::instrument(skip(self))]
    async fn discovery_now(&self) -> YdbResult<()> {
        trace!("discovery locking");
        let discovery_lock = self.discovery_process.lock().await;

        trace!("creating grpc client");
        let start = std::time::Instant::now();
        let mut discovery_client = create_grpc_client(
            self.discovery_uri.clone(),
            self.cred.clone(),
            DiscoveryServiceClient::new,
        )
        .await?;

        trace!("send grpc request ListEndpointsRequest");
        let resp = discovery_client
            .list_endpoints(ListEndpointsRequest {
                database: self.cred.database.clone(),
                service: vec![],
            })
            .await?;

        let res: ListEndpointsResult = grpc_read_operation_result(resp)?;
        trace!("list endpoints: {:?}", res);
        let new_endpoints = Self::list_endpoints_to_node_infos(res)?;
        self.set_discovery_state(
            self.discovery_state.write().unwrap(),
            Arc::new(DiscoveryState::new(start, new_endpoints)),
        );

        // lock until exit
        drop(discovery_lock);
        return Ok(());
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
        if let Some(state) = state.upgrade() {
            // wait token before first discovery
            trace!("start wait token");
            state.cred.token_cache.wait().await.unwrap();
            trace!("token ready");
        }

        while let Some(state) = state.upgrade() {
            trace!("rekby-discovery");
            let res = state.discovery_now().await;
            trace!("rekby-res: {:?}", res);
            // return;
            tokio::time::sleep(interval).await;
        }
        trace!("stop background_discovery");
    }

    fn list_endpoints_to_node_infos(mut list: ListEndpointsResult) -> YdbResult<Vec<NodeInfo>> {
        let mut nodes = Vec::new();

        while let Some(endpoint_info) = list.endpoints.pop() {
            let uri = Self::endpoint_info_to_uri(&endpoint_info)?;
            nodes.push(NodeInfo { uri: uri.clone() });
        }

        return Ok(nodes);
    }

    fn endpoint_info_to_uri(endpoint_info: &EndpointInfo) -> YdbResult<Uri> {
        let authority: Authority = Authority::from_str(
            format!("{}:{}", endpoint_info.address, endpoint_info.port).as_str(),
        )?;

        return Ok(Uri::builder()
            .scheme(if endpoint_info.ssl { "https" } else { "http" })
            .authority(authority)
            .path_and_query("")
            .build()?);
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
        return self.sender.subscribe();
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
    use crate::errors::YdbResult;
    use crate::internal::client_common::{DBCredentials, TokenCache};
    use crate::internal::discovery::DiscoverySharedState;
    use crate::internal::test_helpers::CONNECTION_INFO;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_background_discovery() -> YdbResult<()> {
        let cred = DBCredentials {
            database: CONNECTION_INFO.database.clone(),
            token_cache: tokio::task::spawn_blocking(|| {
                TokenCache::new(CONNECTION_INFO.credentials.clone())
            })
            .await??,
        };
        let discovery_shared = DiscoverySharedState::new(cred, CONNECTION_INFO.endpoint.as_str())?;

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
            assert!(rx.borrow().nodes.len() >= 1);
        }

        return Ok(());
    }
}
