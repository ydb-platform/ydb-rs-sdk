use crate::{GrpcOptions, YdbError, YdbResult};
use derivative::Derivative;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{future, FutureExt, StreamExt};
use http::uri::Scheme;
use http::Uri;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use std::task::Poll;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::trace;

#[derive(Debug)]
pub(crate) struct ConnectionPool<C: Connection> {
    connections: RwLock<HashMap<Uri, Arc<C>>>,
    opts: GrpcOptions,
}

impl<C: Connection> Default for ConnectionPool<C> {
    fn default() -> Self {
        Self {
            connections: Default::default(),
            opts: Default::default(),
        }
    }
}

impl<C: Connection> ConnectionPool<C> {
    pub(crate) fn new(opts: GrpcOptions) -> Self {
        Self {
            connections: HashMap::new().into(),
            opts,
        }
    }

    pub(crate) async fn connection(&self, uri: &Uri) -> YdbResult<Channel> {
        let connection = self.connections.read()?.get(uri).cloned();

        if let Some(connection) = connection {
            connection.channel().await
        } else {
            let (channel, connection) = C::init(uri.to_owned(), self.opts.clone()).await?;
            self.connections
                .write()?
                .insert(uri.clone(), Arc::new(connection));

            Ok(channel)
        }
    }
}

pub trait Connection: Sized {
    async fn init(uri: Uri, opts: GrpcOptions) -> YdbResult<(Channel, Self)>;
    async fn channel(&self) -> YdbResult<Channel>;
}

/// Connection state that is just a lazy channel.
#[derive(Debug)]
pub(crate) struct Simple {
    channel: Channel,
}

impl Connection for Simple {
    async fn init(uri: Uri, opts: GrpcOptions) -> YdbResult<(Channel, Self)> {
        let uri = normalize_uri_scheme(uri)?;
        let channel = endpoint(uri, None, &opts)?.connect_lazy();

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
    opts: GrpcOptions,

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

impl Connection for RacyRoundRobin {
    async fn init(uri: Uri, opts: GrpcOptions) -> YdbResult<(Channel, Self)> {
        let uri = normalize_uri_scheme(uri)?;
        let addrs = Self::resolve(&uri).await?;

        let (connections, first_connection, first_connection_addr) =
            Self::init(uri.clone(), &opts, &addrs).await?;

        Ok((
            first_connection.clone(),
            Self {
                uri,
                opts,
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
                Self::init(self.uri.clone(), &self.opts, &addrs).await?;

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
        opts: &GrpcOptions,
        addrs: &HashSet<IpAddr>,
    ) -> YdbResult<(VecDeque<ConnectionFuture>, Channel, IpAddr)> {
        let mut first_err = None;

        let mut connections = addrs
            .iter()
            .map(|&addr| Self::try_connect(uri.clone(), opts.clone(), addr))
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
                    reconnections.push_back(Self::try_connect(uri.clone(), opts.clone(), addr));
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

    fn try_connect(uri: Uri, opts: GrpcOptions, addr: IpAddr) -> ConnectionFuture {
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

            endpoint(resolved_uri, Some(&uri), &opts)?
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
                        self.opts.clone(),
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

pub fn endpoint(uri: Uri, original_uri: Option<&Uri>, opts: &GrpcOptions) -> YdbResult<Endpoint> {
    let need_tls = uri.scheme() == Some(&Scheme::HTTPS);
    trace!("scheme is {:?}", uri.scheme());

    let mut endpoint = Endpoint::from(uri.clone());

    if let Some(inverval) = opts.keepalive_interval {
        endpoint = endpoint.http2_keep_alive_interval(inverval);
    }

    if need_tls {
        let domain = original_uri
            .unwrap_or(&uri)
            .host()
            .ok_or_else(|| YdbError::EndpointHasNoHost(uri.clone()))?;

        endpoint = configure_tls_endpoint(
            endpoint,
            domain,
            opts.tls_config.clone().map(|tls_config| {
                Arc::try_unwrap(tls_config).unwrap_or_else(|arc| arc.as_ref().clone())
            }),
        )?;
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
