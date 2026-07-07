use crate::{YdbError, YdbResult};
use derivative::Derivative;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{future, FutureExt, StreamExt};
use http::uri::Scheme;
use http::Uri;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::iter;
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tracing::trace;

#[derive(Debug)]
pub(crate) struct ConnectionPool<C: ConnectionState> {
    connections: HashMap<Uri, C>,
    tls_config: Option<Arc<ClientTlsConfig>>,
}

impl<C: ConnectionState> ConnectionPool<C> {
    pub(crate) fn new() -> Self {
        Self {
            connections: HashMap::new(),
            tls_config: None,
        }
    }

    pub(crate) fn load_certificate(self, path: impl AsRef<Path>) -> Self {
        let pem = std::fs::read_to_string(path).unwrap();
        trace!("loaded cert: {}", pem);
        let ca = Certificate::from_pem(pem);
        let config = ClientTlsConfig::new().ca_certificate(ca);

        Self {
            tls_config: Some(config.into()),
            ..self
        }
    }

    pub(crate) async fn connection(&mut self, uri: &Uri) -> YdbResult<Channel> {
        let mut entry = self.connections.entry(uri.clone());

        let tls_config = self.tls_config.as_ref();

        let state = match entry {
            Entry::Occupied(ref mut entry) => entry.get_mut(),
            Entry::Vacant(entry) => entry.insert(C::init(uri.to_owned(), tls_config)?),
        };

        state.channel().await
    }
}

pub trait ConnectionState: Sized {
    fn init(uri: Uri, tls_config: Option<&Arc<ClientTlsConfig>>) -> YdbResult<Self>;
    async fn channel(&mut self) -> YdbResult<Channel>;
}

/// Connection state that is just a lazy channel.
#[derive(Debug)]
pub(crate) struct Simple {
    channel: Channel,
}

impl ConnectionState for Simple {
    fn init(uri: Uri, tls_config: Option<&Arc<ClientTlsConfig>>) -> YdbResult<Self> {
        let uri = normalize_uri_scheme(uri)?;
        let channel = endpoint(uri, tls_config.map(Arc::as_ref))?.connect_lazy();

        Ok(Self { channel })
    }

    async fn channel(&mut self) -> YdbResult<Channel> {
        Ok(self.channel.clone())
    }
}

/// Connection state that tries to
/// connect to all addresses for a given
/// URI and then does round-robin on
/// successful connections.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct RacyRoundRobin {
    uri: Uri,
    tls_config: Option<Arc<ClientTlsConfig>>,
    addrs: Vec<IpAddr>,
    #[derivative(Debug = "ignore")]
    connections: VecDeque<BoxFuture<'static, (YdbResult<Channel>, IpAddr)>>,
}

impl ConnectionState for RacyRoundRobin {
    fn init(uri: Uri, tls_config: Option<&Arc<ClientTlsConfig>>) -> YdbResult<Self> {
        // There are initially no connections,
        // so they will be reinitialized
        Ok(Self {
            uri: normalize_uri_scheme(uri)?,
            tls_config: tls_config.cloned(),
            addrs: vec![],
            connections: VecDeque::new(),
        })
    }

    async fn channel(&mut self) -> YdbResult<Channel> {
        let host = self
            .uri
            .host()
            .ok_or_else(|| YdbError::EndpointHasNoHost(self.uri.clone()))?;

        let addrs = tokio::net::lookup_host(&(host, 0))
            .await?
            .map(|addr| addr.ip())
            .collect::<Vec<_>>();

        if addrs.is_empty() {
            return Err(YdbError::from_str("domain somehow has zero addresses"));
        }

        if self.addrs != addrs || self.connections.is_empty() {
            self.addrs = addrs;
            self.connections.clear();
            self.reinit().await
        } else {
            self.connect_next().await
        }
    }
}

impl RacyRoundRobin {
    async fn reinit(&mut self) -> YdbResult<Channel> {
        let mut first_err = None;

        let mut channels = self
            .addrs
            .iter()
            .map(|&addr| self.try_connect(addr))
            .collect::<FuturesUnordered<_>>();

        let mut failed_addrs = Vec::new();

        // Wait for the first successful connection
        loop {
            let Some((first_result, addr)) = channels.next().await else {
                // All connections have failed.
                return Err(first_err.expect("at least one future must be failed with an error"));
            };

            match first_result {
                // Drop failed connections, ignore errors, but save the first one
                Err(err) => {
                    trace!("connection to {addr} has failed");
                    failed_addrs.push(addr);
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
                Ok(channel) => {
                    trace!("connection to {addr} has succeeded");
                    self.connections = VecDeque::from_iter(
                        channels
                            .into_iter()
                            .chain(failed_addrs.into_iter().map(|addr| self.try_connect(addr)))
                            .chain(iter::once(
                                future::ready((Ok(channel.clone()), addr)).boxed(),
                            )),
                    );
                    break Ok(channel);
                }
            }
        }
    }

    fn try_connect(&self, addr: IpAddr) -> BoxFuture<'static, (YdbResult<Channel>, IpAddr)> {
        let uri = self.uri.clone();
        let tls_config = self.tls_config.clone();

        async move {
            // Connect to URI with replaced origin
            // to specify address
            let mut uri_parts = uri.clone().into_parts();
            uri_parts.authority = Some(
                if let Some(port) = uri.port() {
                    format!("{addr}:{port}")
                } else {
                    addr.to_string()
                }
                .parse()?,
            );
            let resolved_uri = Uri::from_parts(uri_parts)?;

            endpoint(resolved_uri, tls_config.as_deref())?
                .origin(uri)
                .connect()
                .await
                .map_err(YdbError::from)
        }
        .map(move |res| (res, addr))
        .boxed()
    }

    async fn connect_next(&mut self) -> YdbResult<Channel> {
        // At least the initially chosen connection is alive,
        // so the loop will terminate
        loop {
            let mut next_channel = self
                .connections
                .pop_front()
                .expect("at least the initially chosen connection is alive");

            match futures_util::poll!(&mut next_channel) {
                // Connection is ready
                Poll::Ready((Ok(channel), addr)) => {
                    trace!("choosing connection to {addr}");
                    self.connections
                        .push_back(future::ready((Ok(channel.clone()), addr)).boxed());
                    return Ok(channel);
                }
                // Connection has failed
                Poll::Ready((Err(err), addr)) => {
                    trace!("failed to connect to {addr}: {err}");
                    self.connections.push_back(self.try_connect(addr));
                }
                // Still connecting
                Poll::Pending => self.connections.push_back(next_channel),
            }
        }
    }
}

pub fn endpoint(uri: Uri, tls_config: Option<&ClientTlsConfig>) -> YdbResult<Endpoint> {
    let need_tls = uri.scheme() == Some(&Scheme::HTTPS);
    trace!("scheme is {:?}", uri.scheme());

    let mut endpoint =
        Endpoint::from(uri.clone()).http2_keep_alive_interval(Duration::from_secs(10));

    if need_tls {
        let domain = uri
            .host()
            .ok_or_else(|| YdbError::EndpointHasNoHost(uri.clone()))?;

        endpoint = configure_tls_endpoint(endpoint, domain, tls_config.cloned())?;
    }

    Ok(endpoint)
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
    tls_config: Option<ClientTlsConfig>,
) -> YdbResult<Endpoint> {
    Ok(endpoint.tls_config(tls_config.unwrap_or_else(|| {
        ClientTlsConfig::new()
            .domain_name(domain.to_owned())
            .with_native_roots()
    }))?)
}
