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
    connecting: Arc<tokio::sync::Mutex<HashMap<Uri, Arc<tokio::sync::OnceCell<Channel>>>>>,
    tls_config: Arc<Option<ClientTlsConfig>>,
}

impl ConnectionPool {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ConnectionPoolState::new())),
            connecting: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
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

        let connect_once = {
            let mut connecting = self.connecting.lock().await;
            connecting
                .entry(uri.clone())
                .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                .clone()
        };

        let tls_config = self.tls_config.clone();
        let uri_owned = uri.clone();
        let channel = connect_once
            .get_or_try_init(|| async move {
                parallel_endpoint_connect::connect(uri_owned, &tls_config).await
            })
            .await?
            .clone();

        {
            let mut connecting = self.connecting.lock().await;
            connecting.remove(uri);
        }

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
