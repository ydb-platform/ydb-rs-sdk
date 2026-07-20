use crate::{GrpcOptions, YdbError, YdbResult};
use derivative::Derivative;
use futures_util::FutureExt;
use futures_util::stream::FuturesUnordered;
use http::Uri;
use http::uri::Scheme;
use itertools::Either;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::IpAddr;
use std::sync::Arc;
use std::task::Poll;
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::instrument;
use tracing::trace;

#[derive(Debug)]
pub(crate) struct ConnectionPool<ConnectionT: Connection> {
    connections: std::sync::Mutex<HashMap<Uri, Arc<OnceCell<ConnectionT>>>>,
    opts: GrpcOptions,
}

impl<ConnectionT: Connection> Default for ConnectionPool<ConnectionT> {
    fn default() -> Self {
        Self {
            connections: std::sync::Mutex::new(HashMap::new()),
            opts: GrpcOptions::default(),
        }
    }
}

impl<ConnectionT: Connection> ConnectionPool<ConnectionT> {
    pub(crate) fn new(opts: GrpcOptions) -> Self {
        Self {
            connections: HashMap::new().into(),
            opts,
        }
    }

    #[instrument(name = "ydb.ConnectionPool.GetConnection", skip_all, fields(network.peer.address = uri.host(), network.peer.port = uri.port_u16()), err)]
    pub(crate) async fn connection(&self, uri: &Uri) -> YdbResult<Channel> {
        let connection = self
            .connections
            .lock()?
            .entry(uri.to_owned())
            .or_default()
            .clone();

        connection
            .get_or_try_init(|| async { ConnectionT::init(uri.to_owned(), &self.opts).await })
            .await?
            .channel()
            .await
    }
}

pub(crate) trait Connection: Sized {
    async fn init(uri: Uri, opts: &GrpcOptions) -> YdbResult<Self>;
    async fn channel(&self) -> YdbResult<Channel>;
}

/// Connection state that is just a lazy channel.
#[derive(Debug)]
pub(crate) struct Simple {
    channel: Channel,
}

impl Connection for Simple {
    async fn init(uri: Uri, opts: &GrpcOptions) -> YdbResult<Self> {
        let uri = normalize_uri_scheme(uri)?;
        let channel = endpoint(uri, None, opts)?.connect_lazy();

        Ok(Self { channel })
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
    connections: VecDeque<ConnectionTask>,
    first_connection: ReadyConnection,
    #[derivative(Debug = "ignore")]
    tried_connections: VecDeque<ConnectionTask>,
}

type ConnectionTask = Either<PendingConnection, ReadyConnection>;
type ReadyConnection = (Channel, IpAddr);

struct PendingConnection {
    task: JoinHandle<YdbResult<Channel>>,
    addr: IpAddr,
}

impl Future for PendingConnection {
    type Output = (YdbResult<Channel>, IpAddr);

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        let result = futures_util::ready!(self.task.poll_unpin(cx))
            .map_err(YdbError::from)
            .unwrap_or_else(Err);

        Poll::Ready((result, self.addr))
    }
}

impl Connection for RacyRoundRobin {
    async fn init(uri: Uri, opts: &GrpcOptions) -> YdbResult<Self> {
        let uri = normalize_uri_scheme(uri)?;
        let addrs = Self::resolve(&uri).await?;

        let (connections, first_connection) =
            Self::init_connections(uri.clone(), opts, &addrs).await?;

        Ok(Self {
            uri,
            opts: opts.clone(),
            state: RacyRoundRobinState {
                addrs,
                connections,
                first_connection,
                tried_connections: VecDeque::new(),
            }
            .into(),
        })
    }

    async fn channel(&self) -> YdbResult<Channel> {
        let addrs = Self::resolve(&self.uri).await?;

        let mut state = self.state.lock().await;

        if state.addrs != addrs {
            let (connections, first_connection) =
                Self::init_connections(self.uri.clone(), &self.opts, &addrs).await?;
            let channel = first_connection.0.clone();

            *state = RacyRoundRobinState {
                addrs,
                connections,
                first_connection,
                tried_connections: VecDeque::new(),
            };

            Ok(channel)
        } else {
            Ok(self.connect_next(&mut state).await)
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

    async fn init_connections(
        uri: Uri,
        opts: &GrpcOptions,
        addrs: &HashSet<IpAddr>,
    ) -> YdbResult<(VecDeque<ConnectionTask>, ReadyConnection)> {
        let mut first_err = None;

        let mut connections = addrs
            .iter()
            .map(|&addr| Self::try_connect(uri.clone(), opts.clone(), addr))
            .collect::<FuturesUnordered<_>>();

        let mut reconnections = VecDeque::new();

        loop {
            let Some((first_result, addr)) = connections.next().await else {
                return Err(first_err.unwrap_or_else(|| {
                    YdbError::from_str(format!("domain '{}' has no IP addresses", uri))
                }));
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
                        connections
                            .into_iter()
                            .chain(reconnections)
                            .map(Either::Left)
                            .collect(),
                        (channel, addr),
                    ));
                }
            }
        }
    }

    fn try_connect(uri: Uri, opts: GrpcOptions, addr: IpAddr) -> PendingConnection {
        let task = tokio::spawn(async move {
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
        });

        PendingConnection { task, addr }
    }

    async fn connect_next(&self, state: &mut RacyRoundRobinState) -> Channel {
        while let Some(connection) = state.connections.pop_front() {
            let result = match connection {
                // Connection has been finished
                Either::Left(pending) if pending.task.is_finished() => pending.await,
                // Connecting is still pending
                Either::Left(_) => {
                    state.tried_connections.push_back(connection);
                    continue;
                }
                // Connection is ready
                Either::Right((channel, addr)) => (Ok(channel), addr),
            };

            match result {
                // Connection is ready
                (Ok(channel), addr) => {
                    trace!("choosing connection to {addr}");
                    state
                        .tried_connections
                        .push_back(Either::Right((channel.clone(), addr)));
                    return channel;
                }
                // Connection has failed
                (Err(err), addr) => {
                    trace!("failed to connect to {addr}: {err}, trying next");
                    state
                        .tried_connections
                        .push_back(Either::Left(Self::try_connect(
                            self.uri.clone(),
                            self.opts.clone(),
                            addr,
                        )));
                }
            }
        }

        state.connections = std::mem::take(&mut state.tried_connections);
        trace!(
            "choosing to connect to {}, round-robin cycle finished",
            state.first_connection.1
        );

        state.first_connection.0.clone()
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
