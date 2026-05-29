use crate::parallel_endpoint_connect;
use crate::YdbResult;
use http::Uri;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tracing::trace;

#[derive(Clone)]
pub(crate) struct ConnectionPool {
    state: Arc<Mutex<ConnectionPoolState>>,
    tls_config: Arc<Option<ClientTlsConfig>>,
}

impl ConnectionPool {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ConnectionPoolState::new())),
            tls_config: None.into(),
        }
    }

    pub(crate) fn load_certificate(self, path: String) -> Self {
        let pem = std::fs::read_to_string(path).unwrap();
        trace!("loaded cert: {}", pem);
        let ca = Certificate::from_pem(pem);
        let config = ClientTlsConfig::new().ca_certificate(ca);
        Self {
            tls_config: Some(config).into(),
            ..self
        }
    }

    pub(crate) async fn connection(&self, uri: &Uri) -> YdbResult<Channel> {
        let now = Instant::now();

        {
            let mut lock = self.state.lock().unwrap();
            if let Some(ci) = lock.connections.get_mut(uri) {
                ci.last_usage = now;
                return Ok(ci.channel.clone());
            }
        }

        let channel = parallel_endpoint_connect::connect(uri.clone(), &self.tls_config).await?;

        let mut lock = self.state.lock().unwrap();
        if let Some(ci) = lock.connections.get_mut(uri) {
            ci.last_usage = now;
            return Ok(ci.channel.clone());
        }

        let ci = ConnectionInfo {
            last_usage: now,
            channel: channel.clone(),
        };
        lock.connections.insert(uri.clone(), ci);
        Ok(channel)
    }
}

struct ConnectionPoolState {
    connections: HashMap<Uri, ConnectionInfo>,
}

impl ConnectionPoolState {
    fn new() -> Self {
        Self {
            connections: HashMap::new(),
        }
    }
}

struct ConnectionInfo {
    last_usage: Instant,
    channel: Channel,
}
