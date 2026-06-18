use crate::{YdbError, YdbResult};
use http::uri::Scheme;
use http::Uri;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tracing::{instrument, trace};

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

    #[instrument(
        level = "trace",
        name = "ydb.ConnectionPool.LoadCertificate",
        skip_all,
        fields(
            ydb.pool.certificate = %path,
        ),
        err
    )]
    pub(crate) fn load_certificate(self, path: String) -> YdbResult<Self> {
        let pem = std::fs::read_to_string(path).map_err(|err| YdbError::custom(err.to_string()))?;
        trace!("loaded cert: {}", pem);
        let ca = Certificate::from_pem(pem);
        let config = ClientTlsConfig::new().ca_certificate(ca);
        Ok(Self {
            tls_config: Some(config).into(),
            ..self
        })
    }

    #[instrument(
        name = "ydb.ConnectionPool.GetConnection",
        skip_all,
        fields(
            network.peer.address = uri.host(),
            network.peer.port = uri.port_u16()
        ),
        err
    )]
    pub(crate) async fn connection(&self, uri: &Uri) -> YdbResult<Channel> {
        let now = Instant::now();
        let mut lock = self.state.lock().unwrap();
        if let Some(ci) = lock.connections.get_mut(uri) {
            ci.last_usage = now;
            return Ok(ci.channel.clone());
        };

        // TODO: replace lazy connection to real, without global block
        let channel = connect_lazy(uri.clone(), &self.tls_config)?;
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

pub fn connect_lazy(uri: Uri, tls_config: &Option<ClientTlsConfig>) -> YdbResult<Channel> {
    let uri = normalize_uri_scheme(uri)?;

    let tls = uri.scheme() == Some(&Scheme::HTTPS);
    trace!("scheme is {:?}", uri.scheme());

    let mut endpoint = Endpoint::from(uri.clone());

    if tls {
        let domain = uri.host().ok_or_else(|| {
            YdbError::Custom("URI must have a host for TLS connections".to_string())
        })?;

        endpoint = configure_tls_endpoint(endpoint, domain, tls_config)?;
    }

    endpoint = endpoint.http2_keep_alive_interval(Duration::from_secs(10));
    Ok(endpoint.connect_lazy())
}

pub(crate) fn normalize_uri_scheme(uri: Uri) -> YdbResult<Uri> {
    let mut parts = uri.into_parts();
    let scheme = parts.scheme.as_ref().unwrap_or(&Scheme::HTTP);

    match scheme.as_str() {
        "grpc" => parts.scheme = Some(Scheme::HTTP),
        "grpcs" => parts.scheme = Some(Scheme::HTTPS),
        _ => {}
    }

    Ok(Uri::from_parts(parts)?)
}

pub fn configure_tls_endpoint(
    endpoint: Endpoint,
    domain: &str,
    tls_config: &Option<ClientTlsConfig>,
) -> YdbResult<Endpoint> {
    let config = match tls_config {
        Some(config) => config.clone(),
        None => {
            // When no custom CA is provided, use system root certificates.
            ClientTlsConfig::new()
                .domain_name(domain.to_string())
                .with_native_roots()
        }
    };

    Ok(endpoint.tls_config(config)?)
}
