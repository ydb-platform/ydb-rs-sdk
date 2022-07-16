use crate::YdbResult;
use http::Uri;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

#[derive(Clone)]
pub(crate) struct ConnectionPool {
    state: Arc<Mutex<ConnectionPoolState>>,
}

impl ConnectionPool {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ConnectionPoolState::new())),
        }
    }

    pub(crate) async fn connection(&self, uri: &Uri) -> YdbResult<Channel> {
        let now = Instant::now();
        let mut lock = self.state.lock().unwrap();
        if let Some(ci) = lock.connections.get_mut(&uri) {
            ci.last_usage = now;
            return Ok(ci.channel.clone());
        };

        // TODO: replace lazy connection to real, without global block
        let channel = connect_lazy(uri.clone())?;
        let ci = ConnectionInfo {
            last_usage: now,
            channel: channel.clone(),
        };
        lock.connections.insert(uri.clone(), ci);
        return Ok(channel);
    }
}

struct ConnectionPoolState {
    connections: HashMap<Uri, ConnectionInfo>,
}

impl ConnectionPoolState {
    fn new() -> Self {
        return Self {
            connections: HashMap::new(),
        };
    }
}

struct ConnectionInfo {
    last_usage: Instant,
    channel: Channel,
}

fn connect_lazy(uri: Uri) -> YdbResult<Channel> {
    let tls = if let Some(scheme) = uri.scheme_str() {
        scheme == "https" || scheme == "grpcs"
    } else {
        false
    };

    let mut endpoint = Endpoint::from(uri);
    if tls {
        endpoint = endpoint.tls_config(ClientTlsConfig::new())?
    };
    endpoint = endpoint.tcp_keepalive(Some(Duration::from_secs(15))); // tcp keepalive similar to default in golang lib

    return Ok(endpoint.connect_lazy());
}
