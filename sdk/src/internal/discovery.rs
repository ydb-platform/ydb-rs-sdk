use crate::errors::{Error, Result};
use async_trait::async_trait;
use http::Uri;
use std::collections::HashMap;
use std::ops::Deref;
use std::str::FromStr;
use std::string::ToString;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::RwLock;
use strum::Display;

#[derive(Clone, Copy, Display, Debug, Eq, Hash, PartialEq)]
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
pub(crate) trait Discovery: Sync {
    fn endpoint(self: &Self, service: Service) -> Result<Uri>;
    async fn discovery_now(&self) -> Result<()>;
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
    async fn discovery_now(&self) -> Result<()> {
        return Ok(());
    }
}

pub(crate) struct TimerDiscovery {}

struct TimerDiscoveryShared {
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
        todo!()
    }
}
