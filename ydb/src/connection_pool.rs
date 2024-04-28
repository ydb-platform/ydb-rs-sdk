use crate::YdbResult;
use http::Uri;
use tracing::trace;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use http::uri::Scheme;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

#[derive(Clone)]
pub(crate) struct ConnectionPool {
    state: Arc<Mutex<ConnectionPoolState>>,
    certificate_path: Arc<Option<String>>,
}

impl ConnectionPool {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ConnectionPoolState::new())),
            certificate_path: None.into(),
        }
    }

    pub(crate) fn certificate_path(self, path: String) -> Self {
        Self {
            certificate_path: Some(path).into(),
            ..self
        }
    }

    pub(crate) async fn connection(&self, uri: &Uri) -> YdbResult<Channel> {
        let now = Instant::now();
        let mut lock = self.state.lock().unwrap();
        if let Some(ci) = lock.connections.get_mut(uri) {
            ci.last_usage = now;
            return Ok(ci.channel.clone());
        };

        // TODO: replace lazy connection to real, without global block
        let channel = connect_lazy(uri.clone(), self.certificate_path.as_ref())?;
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

fn connect_lazy(uri: Uri, certificate_path: &Option<String>) -> YdbResult<Channel> {
    let mut parts = uri.into_parts();
    if parts.scheme.as_ref().unwrap_or(&Scheme::HTTP).as_str() == "grpc" {
        parts.scheme = Some(Scheme::HTTP)
    } else if parts.scheme.as_ref().unwrap_or(&Scheme::HTTP).as_str() == "grpcs" {
        parts.scheme = Some(Scheme::HTTPS)
    }

    let uri = Uri::from_parts(parts)?;

    let tls = uri.scheme() == Some(&Scheme::HTTPS);
    trace!("scheme is {}", uri.scheme().unwrap());

    let mut endpoint = Endpoint::from(uri);
    if tls {
        endpoint = match certificate_path {
            Some(path) =>  {
                let pem = std::fs::read_to_string(path)?;
                trace!("cert: {}", pem);
                let ca = Certificate::from_pem(pem);
                let tls_config = ClientTlsConfig::new()
                    .ca_certificate(ca);

                endpoint.tls_config(tls_config)?
            }
            None => endpoint.tls_config(ClientTlsConfig::new())?,
        };
    };
    endpoint = endpoint.tcp_keepalive(Some(Duration::from_secs(15))); // tcp keepalive similar to default in golang lib

    Ok(endpoint.connect_lazy())
}
