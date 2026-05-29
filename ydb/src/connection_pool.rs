use crate::parallel_endpoint_connect::{self, ConnectTimeouts};
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
    connect_timeouts: ConnectTimeouts,
}

impl ConnectionPool {
    pub(crate) fn new() -> Self {
        Self::with_connect_timeouts(ConnectTimeouts::default())
    }

    pub(crate) fn with_connect_timeouts(timeouts: ConnectTimeouts) -> Self {
        Self {
            state: Arc::new(Mutex::new(ConnectionPoolState::new())),
            connecting: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            tls_config: None.into(),
            connect_timeouts: timeouts,
        }
    }

    pub(crate) fn load_certificate(mut self, path: String) -> Self {
        let pem = std::fs::read_to_string(path).unwrap();
        trace!("loaded cert: {}", pem);
        let ca = Certificate::from_pem(pem);
        let config = ClientTlsConfig::new().ca_certificate(ca);
        self.tls_config = Some(config).into();
        self
    }

    pub(crate) async fn connection(&self, uri: &Uri) -> YdbResult<Channel> {
        {
            let mut lock = self.state.lock().unwrap();
            if let Some(ci) = lock.connections.get_mut(uri) {
                ci.last_usage = Instant::now();
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

        let mut cleanup = ConnectingCleanup::new(
            Arc::clone(&self.connecting),
            uri.clone(),
            Arc::clone(&connect_once),
        );

        let tls_config = self.tls_config.clone();
        let uri_owned = uri.clone();
        let timeouts = self.connect_timeouts;
        let channel_result = connect_once
            .get_or_try_init(|| async move {
                parallel_endpoint_connect::connect(uri_owned, &tls_config, timeouts).await
            })
            .await
            .cloned();

        // Timestamp connection establishment, not the initial cache miss.
        let now = Instant::now();

        let channel = match channel_result {
            Ok(channel) => {
                let channel = {
                    let mut lock = self.state.lock().unwrap();
                    if let Some(ci) = lock.connections.get_mut(uri) {
                        ci.last_usage = now;
                        ci.channel.clone()
                    } else {
                        lock.connections.insert(
                            uri.clone(),
                            ConnectionInfo {
                                last_usage: now,
                                channel: channel.clone(),
                            },
                        );
                        channel
                    }
                };

                self.remove_connecting_if_same(uri, &connect_once).await;
                cleanup.disarm();

                channel
            }
            Err(err) => {
                self.remove_connecting_if_same(uri, &connect_once).await;
                cleanup.disarm();
                return Err(err);
            }
        };

        Ok(channel)
    }

    async fn remove_connecting_if_same(
        &self,
        uri: &Uri,
        connect_once: &Arc<tokio::sync::OnceCell<Channel>>,
    ) {
        remove_connecting_entry(
            Arc::clone(&self.connecting),
            uri.clone(),
            Arc::clone(connect_once),
        )
        .await;
    }
}

async fn remove_connecting_entry(
    connecting: Arc<tokio::sync::Mutex<HashMap<Uri, Arc<tokio::sync::OnceCell<Channel>>>>>,
    uri: Uri,
    connect_once: Arc<tokio::sync::OnceCell<Channel>>,
) {
    let mut map = connecting.lock().await;
    if map
        .get(&uri)
        .is_some_and(|entry| Arc::ptr_eq(entry, &connect_once))
    {
        map.remove(&uri);
    }
}

struct ConnectingCleanup {
    connecting: Arc<tokio::sync::Mutex<HashMap<Uri, Arc<tokio::sync::OnceCell<Channel>>>>>,
    uri: Uri,
    connect_once: Arc<tokio::sync::OnceCell<Channel>>,
    disarmed: bool,
}

impl ConnectingCleanup {
    fn new(
        connecting: Arc<tokio::sync::Mutex<HashMap<Uri, Arc<tokio::sync::OnceCell<Channel>>>>>,
        uri: Uri,
        connect_once: Arc<tokio::sync::OnceCell<Channel>>,
    ) -> Self {
        Self {
            connecting,
            uri,
            connect_once,
            disarmed: false,
        }
    }

    fn disarm(&mut self) {
        self.disarmed = true;
    }
}

impl Drop for ConnectingCleanup {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }

        let connecting = Arc::clone(&self.connecting);
        let uri = self.uri.clone();
        let connect_once = Arc::clone(&self.connect_once);

        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                remove_connecting_entry(connecting, uri, connect_once).await;
            });
        }
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
