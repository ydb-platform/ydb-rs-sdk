use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, RwLock, RwLockWriteGuard, Weak};

use async_trait::async_trait;
use http::uri::Authority;
use http::Uri;
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

use ydb_protobuf::generated::ydb::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::generated::ydb::discovery::{
    EndpointInfo, ListEndpointsRequest, ListEndpointsResult,
};

use crate::errors::{Error, Result};
use crate::internal::grpc::{create_grpc_client, grpc_read_operation_result};
use std::iter::FromIterator;
use std::time::Duration;
use tokio::sync::watch::Receiver;
use crate::internal::client_common::DBCredentials;

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
pub(crate) struct DiscoveryState {
    pub timestamp: std::time::Instant,
    pub services: HashMap<Service, Vec<NodeInfo>>,

    pessimized_nodes: HashSet<Uri>,
    original_services: HashMap<Service, Vec<NodeInfo>>,
}

impl DiscoveryState {

    pub(crate) fn new(timestamp: std::time::Instant, services: HashMap<Service, Vec<NodeInfo>>)->Self{
        return DiscoveryState{
            timestamp,
            services: services.clone(),
            pessimized_nodes:HashSet::new(),
            original_services: services,
        }
    }

    // pessimize return true if state was changed
    pub(crate) fn pessimize(&mut self, uri: &Uri)->bool {
        if self.pessimized_nodes.contains(uri){
            return false
        };

        self.pessimized_nodes.insert(uri.clone());
        self.build_services();
        return true
    }

    fn build_services(&mut self){
        self.services.clear();

        for (service, origin_nodes) in self.original_services.iter() {
            let mut nodes = Vec::with_capacity(origin_nodes.len());

            for origin_node in origin_nodes.iter() {
                if !self.pessimized_nodes.contains(&origin_node.uri) {
                    nodes.push(origin_node.clone())
                }
            }

            // if all nodes pessimized - use full nodes set
            if nodes.len() == 0 {
                nodes.clone_from(origin_nodes)
            }

            self.services.insert(service.clone(), nodes);
        }
    }

    pub(crate) fn with_node_info(mut self, service: Service, node_info: NodeInfo) -> Self {
        if !self.services.contains_key(&service) {
            self.services.insert(service, Vec::new());
        };

        self.services.get_mut(&service).unwrap().push(node_info);

        return self;
    }
}

impl Default for DiscoveryState {
    fn default() -> Self {
        return DiscoveryState::new(std::time::Instant::now(), HashMap::default());
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
pub(crate) trait Discovery: Send + Sync {
    fn pessimization(&self, uri: &Uri);
    fn subscribe(&self) -> tokio::sync::watch::Receiver<Arc<DiscoveryState>>;
    fn state(&self)->Arc<DiscoveryState>;
}

pub(crate) struct StaticDiscovery {
    endpoint: Uri,
    sender: tokio::sync::watch::Sender<Arc<DiscoveryState>>,
    discovery_state: Arc<DiscoveryState>,
}

impl StaticDiscovery {
    pub(crate) fn from_str(endpoint: &str) -> Result<Self> {
        let endpoint = Uri::from_str(endpoint)?;
        let services = HashMap::from_iter(Service::iter().map(|service| {
            (
                service,
                vec![NodeInfo {
                    uri: endpoint.clone(),
                }],
            )
        }));

        let state = DiscoveryState::new(std::time::Instant::now(), services);
        let state = Arc::new(state);
        let (sender, _) = tokio::sync::watch::channel(state.clone());
        return Ok(StaticDiscovery { endpoint, sender, discovery_state: state });
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

#[derive(Clone)]
pub(crate) struct TimerDiscovery {
    state: Arc<DiscoverySharedState>,
}

impl TimerDiscovery {
    #[allow(dead_code)]
    pub(crate) fn new(
        cred: DBCredentials,
        database: String,
        endpoint: &str,
        interval: Duration,
    ) -> Result<Self> {
        let state = Arc::new(DiscoverySharedState::new(cred, database, endpoint)?);
        let state_weak = Arc::downgrade(&state);
        tokio::spawn(async move {
            DiscoverySharedState::background_discovery(state_weak, interval).await;
        });
        return Ok(TimerDiscovery {
            state,
        });
    }

    #[allow(dead_code)]
    async fn discovery_now(&self) -> Result<()> {
        return self.state.discovery_now().await;
    }
}

impl Discovery for TimerDiscovery {
    fn pessimization(&self, uri: &Uri) {
        self.state.pessimization(uri);
    }

    fn subscribe(&self) -> Receiver<Arc<DiscoveryState>> {
        todo!()
    }

    fn state(&self) -> Arc<DiscoveryState> {
        return self.state.state();
    }
}

struct DiscoverySharedState {
    cred: DBCredentials,
    discovery_uri: Uri,
    database: String,
    discovery_state: RwLock<Arc<DiscoveryState>>,
    sender: tokio::sync::watch::Sender<Arc<DiscoveryState>>,
    next_index_base: AtomicUsize,
}

impl DiscoverySharedState {
    fn new(cred: DBCredentials, database: String, endpoint: &str) -> Result<Self> {
        let state = Arc::new(DiscoveryState::new(std::time::Instant::now(),HashMap::new()));
        let (sender, _) = tokio::sync::watch::channel(state.clone());

        return Ok(Self {
            cred,
            database,
            discovery_uri: http::Uri::from_str(endpoint)?,
            discovery_state: RwLock::new(state),
            next_index_base: AtomicUsize::default(),
            sender,
        });
    }

    async fn discovery_now(&self) -> Result<()> {
        let start = std::time::Instant::now();
        let mut discovery_client = create_grpc_client(
            self.discovery_uri.clone(),
            self.cred.clone(),
            DiscoveryServiceClient::new,
        )?;

        let resp = discovery_client
            .list_endpoints(ListEndpointsRequest {
                database: self.database.clone(),
                service: vec![],
            })
            .await?;

        let res: ListEndpointsResult = grpc_read_operation_result(resp)?;
        println!("list endpoints: {:?}", res);
        let new_endpoints = Self::list_endpoints_to_services_map(res)?;
        self.set_discovery_state(self.discovery_state.write().unwrap(), DiscoveryState::new(start, new_endpoints));
        return Ok(());
    }

    fn set_discovery_state(&self, mut locked_state: RwLockWriteGuard<Arc<DiscoveryState>>, new_state: DiscoveryState){
        let new_state = Arc::new(new_state);
        *locked_state = new_state.clone();
        let _ = self.sender.send(new_state);
    }

    async fn background_discovery(state: Weak<DiscoverySharedState>, interval: Duration) {
        while let Some(state) = state.upgrade() {
            println!("rekby-discovery");
            let res = state.discovery_now().await;
            println!("rekby-res: {:?}", res);
            // return;
            tokio::time::sleep(interval).await;
        }
        println!("stop background_discovery");
    }

    fn list_endpoints_to_services_map(
        mut list: ListEndpointsResult,
    ) -> Result<HashMap<Service, Vec<NodeInfo>>> {
        let mut map = HashMap::new();

        while let Some(mut endpoint_info) = list.endpoints.pop() {
            let uri = Self::endpoint_info_to_uri(&endpoint_info)?;
            'services: while let Some(service_name) = endpoint_info.service.pop() {
                let service = match Service::from_str(service_name.as_str()) {
                    Ok(service) => service,
                    Err(err) => {
                        println!("can't match: '{}' ({})", service_name, err);
                        continue 'services;
                    }
                };
                let vec = if let Some(vec) = map.get_mut(&service) {
                    vec
                } else {
                    map.insert(service, Vec::new());
                    map.get_mut(&service).unwrap()
                };
                vec.push(NodeInfo { uri: uri.clone() });
            }
        }

        return Ok(map);
    }

    fn endpoint_info_to_uri(endpoint_info: &EndpointInfo) -> Result<Uri> {
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
        if discovery_state.pessimize(uri) {
            self.set_discovery_state(lock, discovery_state)
        }
    }

    fn subscribe(&self) -> Receiver<Arc<DiscoveryState>> {
        return self.sender.subscribe();
    }

    fn state(&self) -> Arc<DiscoveryState> {
        return self.discovery_state.read().unwrap().clone();
    }
}

#[cfg(test)]
mod test {
    use crate::errors::Result;
    use crate::internal::discovery::DiscoverySharedState;
    use crate::internal::test_helpers::{CRED, DATABASE, START_ENDPOINT};
    use std::sync::Arc;
    use std::time::Duration;
    use crate::internal::client_common::DBCredentials;

    #[tokio::test]
    async fn test_background_discovery() -> Result<()> {
        let cred = DBCredentials{database: DATABASE.clone(), credentials: Box::new(CRED.lock()?.clone())};
        let discovery_shared = DiscoverySharedState::new(
            cred,
            DATABASE.clone(),
            START_ENDPOINT.as_str(),
        )?;

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
            assert!(rx.borrow().services.len() > 1);
        }

        return Ok(());
    }
}
