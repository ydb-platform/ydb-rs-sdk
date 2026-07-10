use crate::{YdbError, YdbResult};
use derivative::Derivative;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{future, FutureExt, StreamExt};
use http::uri::Scheme;
use http::Uri;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::IpAddr;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::task::Poll;
use std::time::Duration;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tracing::trace;

#[derive(Debug)]
pub(crate) struct ConnectionPool<C: ConnectionState> {
    connections: RwLock<HashMap<Uri, Arc<C>>>,
    tls_config: Option<Arc<ClientTlsConfig>>,
}

impl<C: ConnectionState> ConnectionPool<C> {
    pub(crate) fn new() -> Self {
        Self {
            connections: HashMap::new().into(),
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

    pub(crate) async fn connection(&self, uri: &Uri) -> YdbResult<Channel> {
        let connection;

        {
            let read = self.connections.read().map_err(YdbError::from)?;
            connection = read.get(uri).cloned();
        }

        if let Some(connection) = connection {
            connection.channel().await
        } else {
            let (channel, connection) = C::init(uri.to_owned(), self.tls_config.as_ref()).await?;
            self.connections
                .write()?
                .insert(uri.clone(), Arc::new(connection));

            Ok(channel)
        }
    }
}

pub trait ConnectionState: Sized {
    async fn init(
        uri: Uri,
        tls_config: Option<&Arc<ClientTlsConfig>>,
    ) -> YdbResult<(Channel, Self)>;
    async fn channel(&self) -> YdbResult<Channel>;
}

/// Connection state that is just a lazy channel.
#[derive(Debug)]
pub(crate) struct Simple {
    channel: Channel,
}

impl ConnectionState for Simple {
    async fn init(
        uri: Uri,
        tls_config: Option<&Arc<ClientTlsConfig>>,
    ) -> YdbResult<(Channel, Self)> {
        let uri = normalize_uri_scheme(uri)?;
        let channel = endpoint(uri, None, tls_config.map(Arc::as_ref))?.connect_lazy();

        Ok((channel.clone(), Self { channel }))
    }

    async fn channel(&self) -> YdbResult<Channel> {
        Ok(self.channel.clone())
    }
}

/// Connection state that tries to
/// connect to all addresses for a given
/// URI and then does round-robin on
/// successful connections.
#[derive(Debug)]
pub(crate) struct RacyRoundRobin {
    uri: Uri,
    tls_config: Option<Arc<ClientTlsConfig>>,

    state: tokio::sync::Mutex<RacyRoundRobinState>,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct RacyRoundRobinState {
    addrs: HashSet<IpAddr>,

    #[derivative(Debug = "ignore")]
    connections: VecDeque<ConnectionFuture>,
    first_connection: Channel,
    first_connection_addr: IpAddr,
    #[derivative(Debug = "ignore")]
    polled_connections: VecDeque<ConnectionFuture>,
}

type ConnectionFuture = BoxFuture<'static, (YdbResult<Channel>, IpAddr)>;

impl ConnectionState for RacyRoundRobin {
    async fn init(
        uri: Uri,
        tls_config: Option<&Arc<ClientTlsConfig>>,
    ) -> YdbResult<(Channel, Self)> {
        let uri = normalize_uri_scheme(uri)?;
        let addrs = Self::resolve(&uri).await?;

        let (connections, first_connection, first_connection_addr) =
            Self::init(uri.clone(), tls_config, &addrs).await?;

        Ok((
            first_connection.clone(),
            Self {
                uri,
                tls_config: tls_config.cloned(),
                state: RacyRoundRobinState {
                    addrs,
                    connections,
                    first_connection,
                    first_connection_addr,
                    polled_connections: VecDeque::new(),
                }
                .into(),
            },
        ))
    }

    async fn channel(&self) -> YdbResult<Channel> {
        let addrs = Self::resolve(&self.uri).await?;

        let mut state = self.state.lock().await;

        if state.addrs != addrs {
            let (connections, first_connection, first_connection_addr) =
                Self::init(self.uri.clone(), self.tls_config.as_ref(), &addrs).await?;

            *state = RacyRoundRobinState {
                addrs,
                connections,
                first_connection: first_connection.clone(),
                first_connection_addr,
                polled_connections: VecDeque::new(),
            };

            Ok(first_connection)
        } else {
            Ok(future::poll_fn(|cx| Poll::Ready(self.connect_next(&mut state, cx))).await)
        }
    }
}

impl RacyRoundRobin {
    async fn resolve(uri: &Uri) -> YdbResult<HashSet<IpAddr>> {
        let host = uri
            .host()
            .ok_or_else(|| YdbError::EndpointHasNoHost(uri.clone()))?;

        let addrs = tokio::net::lookup_host(&(host, 0))
            .await?
            .map(|addr| addr.ip())
            .collect::<HashSet<_>>();

        Ok(addrs)
    }

    async fn init(
        uri: Uri,
        tls_config: Option<&Arc<ClientTlsConfig>>,
        addrs: &HashSet<IpAddr>,
    ) -> YdbResult<(VecDeque<ConnectionFuture>, Channel, IpAddr)> {
        let mut first_err = None;

        let mut connections = addrs
            .iter()
            .map(|&addr| Self::try_connect(uri.clone(), tls_config.cloned(), addr))
            .collect::<FuturesUnordered<_>>();

        let mut reconnections = VecDeque::new();

        loop {
            let Some((first_result, addr)) = connections.next().await else {
                return Err(first_err
                    .unwrap_or_else(|| YdbError::from_str("domain somehow has zero addresses")));
            };

            match first_result {
                // Remember failed connections, ignore errors, but save the first one
                Err(err) => {
                    trace!("connection to {addr} has failed");
                    reconnections.push_back(Self::try_connect(
                        uri.clone(),
                        tls_config.cloned(),
                        addr,
                    ));
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
                Ok(channel) => {
                    trace!("connection to {addr} has succeeded");
                    return Ok((
                        connections.into_iter().chain(reconnections).collect(),
                        channel,
                        addr,
                    ));
                }
            }
        }
    }

    fn try_connect(
        uri: Uri,
        tls_config: Option<Arc<ClientTlsConfig>>,
        addr: IpAddr,
    ) -> ConnectionFuture {
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

            endpoint(resolved_uri, Some(&uri), tls_config.as_deref())?
                .origin(uri)
                .connect()
                .await
                .map_err(YdbError::from)
        }
        .map(move |res| (res, addr))
        .boxed()
    }

    fn connect_next(
        &self,
        state: &mut RacyRoundRobinState,
        cx: &mut std::task::Context<'_>,
    ) -> Channel {
        while let Some(mut connection) = state.connections.pop_front() {
            match connection.poll_unpin(cx) {
                // Connection is ready
                Poll::Ready((Ok(channel), addr)) => {
                    trace!("choosing connection to {addr}");
                    state
                        .polled_connections
                        .push_back(future::ready((Ok(channel.clone()), addr)).boxed());
                    return channel;
                }
                // Connection has failed
                Poll::Ready((Err(err), addr)) => {
                    trace!("failed to connect to {addr}: {err}, trying next");
                    state.polled_connections.push_back(Self::try_connect(
                        self.uri.clone(),
                        self.tls_config.clone(),
                        addr,
                    ));
                }
                // Still connecting
                Poll::Pending => state.polled_connections.push_back(connection),
            }
        }

        state.connections = std::mem::take(&mut state.polled_connections);
        trace!(
            "choosing to connect to {}, round-robin cycle finished",
            state.first_connection_addr
        );

        state.first_connection.clone()
    }
}

pub fn endpoint(
    uri: Uri,
    original_uri: Option<&Uri>,
    tls_config: Option<&ClientTlsConfig>,
) -> YdbResult<Endpoint> {
    let need_tls = uri.scheme() == Some(&Scheme::HTTPS);
    trace!("scheme is {:?}", uri.scheme());

    let mut endpoint =
        Endpoint::from(uri.clone()).http2_keep_alive_interval(Duration::from_secs(10));

    if need_tls {
        let domain = original_uri
            .unwrap_or(&uri)
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
