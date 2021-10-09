use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, RwLock, Weak};

use async_trait::async_trait;
use http::uri::Authority;
use http::Uri;
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

use ydb_protobuf::generated::ydb::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::generated::ydb::discovery::{
    EndpointInfo, ListEndpointsRequest, ListEndpointsResult,
};

use crate::credentials::Credentials;
use crate::errors::{Error, Result};
use crate::internal::grpc_helper::{create_grpc_client_old, grpc_read_result};
use std::iter::FromIterator;
use std::time::Duration;
use tokio::sync::watch::Receiver;

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
}

impl DiscoveryState {
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
        return DiscoveryState {
            timestamp: std::time::Instant::now(),
            services: HashMap::new(),
        };
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
    fn endpoint(self: &Self, service: Service) -> Result<Uri>;
    fn subscribe(&self) -> tokio::sync::watch::Receiver<Arc<DiscoveryState>>;
}

pub(crate) struct StaticDiscovery {
    endpoint: Uri,
    sender: tokio::sync::watch::Sender<Arc<DiscoveryState>>,
}

impl StaticDiscovery {
    pub(crate) fn from_str(endpoint: &str) -> Result<Self> {
        let endpoint = Uri::from_str(endpoint)?;
        let state = DiscoveryState {
            timestamp: std::time::Instant::now(),
            services: HashMap::from_iter(Service::iter().map(|service| {
                (
                    service,
                    vec![NodeInfo {
                        uri: endpoint.clone(),
                    }],
                )
            })),
        };

        let (sender, _) = tokio::sync::watch::channel(Arc::new(state));
        return Ok(StaticDiscovery { endpoint, sender });
    }
}

#[async_trait]
impl Discovery for StaticDiscovery {
    fn endpoint(self: &Self, _service: Service) -> Result<Uri> {
        return Ok(self.endpoint.clone());
    }

    fn subscribe(&self) -> Receiver<Arc<DiscoveryState>> {
        return self.sender.subscribe();
    }
}

#[derive(Clone)]
pub(crate) struct TimerDiscovery {
    state: Arc<DiscoverySharedState>,
    sender: Arc<tokio::sync::watch::Sender<DiscoveryState>>,
}

impl TimerDiscovery {
    #[allow(dead_code)]
    pub(crate) fn new(
        cred: Box<dyn Credentials>,
        database: String,
        endpoint: &str,
        interval: Duration,
    ) -> Result<Self> {
        let state = Arc::new(DiscoverySharedState::new(cred, database, endpoint)?);
        let state_weak = Arc::downgrade(&state);
        tokio::spawn(async move {
            DiscoverySharedState::background_discovery(state_weak, interval).await;
        });
        let (sender, _) = tokio::sync::watch::channel(DiscoveryState::default());
        return Ok(TimerDiscovery {
            state,
            sender: Arc::new(sender),
        });
    }

    #[allow(dead_code)]
    async fn discovery_now(&self) -> Result<()> {
        return self.state.discovery_now().await;
    }
}

impl Discovery for TimerDiscovery {
    fn endpoint(self: &Self, service: Service) -> Result<Uri> {
        return self.state.endpoint(service);
    }

    fn subscribe(&self) -> Receiver<Arc<DiscoveryState>> {
        todo!()
    }
}

struct DiscoverySharedState {
    cred: Box<dyn Credentials>,
    database: String,
    discovery_state: RwLock<Arc<DiscoveryState>>,
    sender: tokio::sync::watch::Sender<Arc<DiscoveryState>>,
    next_index_base: AtomicUsize,
}

impl DiscoverySharedState {
    fn new(cred: Box<dyn Credentials>, database: String, endpoint: &str) -> Result<Self> {
        let mut map = HashMap::new();
        map.insert(
            Service::Discovery,
            vec![NodeInfo {
                uri: http::Uri::from_str(endpoint)?,
            }],
        );
        let state = Arc::new(DiscoveryState {
            timestamp: std::time::Instant::now(),
            services: map,
        });
        let (sender, _) = tokio::sync::watch::channel(state.clone());

        return Ok(Self {
            cred,
            database,
            discovery_state: RwLock::new(state),
            next_index_base: AtomicUsize::default(),
            sender,
        });
    }

    async fn discovery_now(&self) -> Result<()> {
        let start = std::time::Instant::now();
        let endpoint = self.endpoint(Service::Discovery)?;
        let mut discovery_client = create_grpc_client_old(
            endpoint,
            self.cred.clone(),
            self.database.clone(),
            DiscoveryServiceClient::new,
        )?;

        let resp = discovery_client
            .list_endpoints(ListEndpointsRequest {
                database: self.database.clone(),
                service: vec![],
            })
            .await?;

        let res: ListEndpointsResult = grpc_read_result(resp)?;
        println!("list endpoints: {:?}", res);
        let new_endpoints = Self::list_endpoints_to_services_map(res)?;
        let new_state = Arc::new(DiscoveryState {
            timestamp: start,
            services: new_endpoints,
        });
        let mut self_map = self.discovery_state.write()?;
        *self_map = new_state.clone();
        drop(self_map);
        let _ = self.sender.send(new_state);

        return Ok(());
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
    fn endpoint(self: &Self, service: Service) -> Result<Uri> {
        let base_index = self.next_index_base.fetch_add(1, Relaxed);

        let map = self.discovery_state.read()?;
        let nodes_info = map.services.get(&service);
        let nodes_info = match nodes_info {
            Some(endpoints) => endpoints,
            None => {
                return Err(Error::from(
                    format!("empty endpoints list for service {:?}", service).as_str(),
                ))
            }
        };
        return Ok(nodes_info[base_index % nodes_info.len()].uri.clone());
    }

    fn subscribe(&self) -> Receiver<Arc<DiscoveryState>> {
        return self.sender.subscribe();
    }
}

#[cfg(test)]
mod test {
    use crate::errors::Result;
    use crate::internal::discovery::DiscoverySharedState;
    use crate::internal::test_helpers::{CRED, DATABASE, START_ENDPOINT};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_background_discovery() -> Result<()> {
        let discovery_shared = DiscoverySharedState::new(
            Box::new(CRED.lock()?.clone()),
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
