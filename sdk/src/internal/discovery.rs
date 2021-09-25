use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, RwLock, Weak};

use async_trait::async_trait;
use http::uri::Authority;
use http::Uri;
use strum::{Display, EnumString};

use ydb_protobuf::generated::ydb::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::generated::ydb::discovery::{
    EndpointInfo, ListEndpointsRequest, ListEndpointsResult,
};

use crate::credentials::Credentials;
use crate::errors::{Error, Result};
use crate::internal::grpc_helper::{create_grpc_client, grpc_read_result};
use std::time::Duration;

#[derive(Clone, Copy, Display, Debug, EnumString, Eq, Hash, PartialEq)]
pub(crate) enum Service {
    #[strum(serialize = "discovery")]
    Discovery,

    // #[strum(serialize = "export")]
    // Export,
    //
    // #[strum(serialize = "import")]
    // Import,
    //
    // #[strum(serialize = "scripting")]
    // Scripting,
    //
    #[strum(serialize = "table_service")]
    TableService,
}

#[async_trait]
pub(crate) trait Discovery: Send + Sync {
    fn endpoint(self: &Self, service: Service) -> Result<Uri>;
}

pub(crate) struct StaticDiscovery {
    endpoint: Uri,
}

impl StaticDiscovery {
    pub(crate) fn from_str(endpoint: &str) -> Result<Self> {
        return Ok(StaticDiscovery {
            endpoint: Uri::from_str(endpoint)?,
        });
    }
}

#[async_trait]
impl Discovery for StaticDiscovery {
    fn endpoint(self: &Self, _service: Service) -> Result<Uri> {
        return Ok(self.endpoint.clone());
    }
}

#[derive(Clone)]
pub(crate) struct TimerDiscovery {
    state: Arc<DiscoverySharedState>,
}

impl TimerDiscovery {
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
        return Ok(TimerDiscovery { state });
    }

    async fn discovery_now(&self) -> Result<()> {
        return self.state.discovery_now().await;
    }
}

impl Discovery for TimerDiscovery {
    fn endpoint(self: &Self, service: Service) -> Result<Uri> {
        return self.state.endpoint(service);
    }
}

struct DiscoverySharedState {
    cred: Box<dyn Credentials>,
    database: String,
    endpoints: RwLock<HashMap<Service, Vec<Uri>>>,
    next_index_base: AtomicUsize,
}

impl DiscoverySharedState {
    fn new(cred: Box<dyn Credentials>, database: String, endpoint: &str) -> Result<Self> {
        let mut map = HashMap::new();
        map.insert(Service::Discovery, vec![http::Uri::from_str(endpoint)?]);
        return Ok(Self {
            cred,
            database,
            endpoints: RwLock::new(map),
            next_index_base: AtomicUsize::default(),
        });
    }

    async fn discovery_now(&self) -> Result<()> {
        let endpoint = self.endpoint(Service::Discovery)?;
        let mut discovery_client = create_grpc_client(
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
        let new_endpoints = Self::list_endpoints_to_hashmap(res)?;

        let mut self_map = self.endpoints.write()?;
        self_map.clone_from(&new_endpoints);

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

    fn list_endpoints_to_hashmap(
        mut list: ListEndpointsResult,
    ) -> Result<HashMap<Service, Vec<Uri>>> {
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
                vec.push(uri.clone());
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

        let map = self.endpoints.read()?;
        let endpoints = map.get(&service);
        let endpoints = match endpoints {
            Some(endpoints) => endpoints,
            None => {
                return Err(Error::from(
                    format!("empty endpoints list for service {:?}", service).as_str(),
                ))
            }
        };
        return Ok(endpoints[base_index % endpoints.len()].clone());
    }
}

mod test {
    use crate::errors::Result;
    use crate::internal::discovery::{DiscoverySharedState, Service};
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

        let map = discovery_shared.endpoints.read()?.clone();

        let state = Arc::new(discovery_shared);
        let state_weak = Arc::downgrade(&state);
        tokio::spawn(async {
            DiscoverySharedState::background_discovery(state_weak, Duration::from_millis(50)).await;
        });
        // return Ok(());

        let mut cnt = 0;

        while cnt < 2 {
            println!("rekby-check");
            if let mut endpoints = state.endpoints.write()? {
                if endpoints.get(&Service::Discovery).unwrap().len() > 1 {
                    endpoints.clone_from(&map);
                    cnt += 1;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        drop(state);
        tokio::time::sleep(Duration::from_millis(1000)).await;

        return Ok(());
    }
}
