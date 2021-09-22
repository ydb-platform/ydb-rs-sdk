use crate::credentials::Credentials;
use crate::errors::{Error, Result};
use crate::internal::grpc::{grpc_read_result, ClientFabric, SimpleGrpcClientFabric};
use async_trait::async_trait;
use http::uri::Authority;
use http::Uri;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::ops::Deref;
use std::str::FromStr;
use std::string::ToString;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::RwLock;
use strum::{Display, EnumString};
use ydb_protobuf::generated::ydb::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::generated::ydb::discovery::{
    EndpointInfo, ListEndpointsRequest, ListEndpointsResult,
};

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
    async fn discovery_now(&self) -> Result<()>;
}

pub(crate) struct StaticDiscovery {
    endpoint: Uri,
}

impl StaticDiscovery {
    fn from_uri(endpoint: Uri) -> Self {
        StaticDiscovery { endpoint }
    }

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
    async fn discovery_now(&self) -> Result<()> {
        return Ok(());
    }
}

pub(crate) struct TimerDiscovery {}

struct TimerDiscoveryShared {
    cred: Box<dyn Credentials>,
    database: String,
    endpoints: RwLock<HashMap<Service, Vec<Uri>>>,
    next_index_base: AtomicUsize,
}

#[async_trait]
impl Discovery for TimerDiscoveryShared {
    fn endpoint(self: &Self, service: Service) -> Result<Uri> {
        let base_index = self.next_index_base.fetch_add(1, Relaxed);

        let map = self.endpoints.read()?;
        let endpoints = map.get(&service);
        let endpoints = match endpoints {
            Some(endpoints) => endpoints,
            None => {
                return Err(Error::from(
                    format!("empty endpoints list for service {}", service).as_str(),
                ))
            }
        };
        return Ok(endpoints[base_index % endpoints.len()].clone());
    }

    async fn discovery_now(&self) -> Result<()> {
        let discovery_endpoint = self.endpoint(Service::Discovery)?;
        let fabric = SimpleGrpcClientFabric::new(
            Box::new(StaticDiscovery::from_uri(discovery_endpoint)),
            self.cred.clone(),
            self.database.clone(),
        );
        let mut discovery_client =
            fabric.create(DiscoveryServiceClient::new, Service::Discovery)?;

        let resp = discovery_client
            .list_endpoints(ListEndpointsRequest {
                database: self.database.clone(),
                service: vec![],
            })
            .await?;
        let mut res: ListEndpointsResult = grpc_read_result(resp)?;
        let mut new_endpoints = HashMap::new();
        while let Some(mut endpoint_info) = res.endpoints.pop() {
            let uri = endpoint_info_to_uri(&endpoint_info)?;
            while let Some(service_name) = endpoint_info.service.pop() {
                let service = Service::from_str(service_name.as_str())?;
                new_endpoints.insert(service_name, uri.clone());
            }
        }
        return Ok(());
    }
}

fn endpoint_info_to_uri(endpoint_info: &EndpointInfo) -> Result<Uri> {
    let authority: Authority =
        Authority::from_str(format!("{}:{}", endpoint_info.address, endpoint_info.port).as_str())?;

    return Ok(Uri::builder()
        .scheme(if endpoint_info.ssl { "https" } else { "http" })
        .authority(authority)
        .path_and_query("")
        .build()?);
}
