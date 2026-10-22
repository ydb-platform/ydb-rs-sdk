use std::collections::HashSet;
use std::future;
use std::ops::ControlFlow;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use futures_util::stream::{self, BoxStream};
use http::Uri;
use http::uri::Authority;
use itertools::Itertools;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tracing::{instrument, trace, warn};

use crate::errors::{NeedRetry, YdbResult};
use crate::grpc_connection_manager::DiscoveryConnectionManager;
use crate::grpc_wrapper::{
    raw_discovery_client::{EndpointInfo, GrpcDiscoveryClient},
    raw_services::Service,
};
use crate::retry_settings::RetryState;
use crate::waiter::Waiter;
use crate::{ExponentialBackoff, YdbError, closure};

/// Current discovery state
#[derive(Clone, Debug, PartialEq)]
pub struct DiscoveryState {
    pub(crate) timestamp: std::time::Instant,
    nodes: Vec<NodeInfo>,

    pessimized_nodes: HashSet<Uri>,
    original_nodes: Vec<NodeInfo>,
}

impl DiscoveryState {
    pub(crate) fn new(timestamp: std::time::Instant, nodes: Vec<NodeInfo>) -> Self {
        let mut state = DiscoveryState {
            timestamp,
            nodes: Vec::new(),
            pessimized_nodes: HashSet::new(),
            original_nodes: nodes,
        };
        state.build_services();
        state
    }

    fn build_services(&mut self) {
        self.nodes.clear();

        for origin_node in self.original_nodes.iter() {
            if !self.pessimized_nodes.contains(&origin_node.uri) {
                self.nodes.push(origin_node.clone())
            }
        }

        // if all nodes pessimized - use full nodes set
        if self.nodes.is_empty() {
            self.nodes.clone_from(&self.original_nodes)
        }
    }

    pub(crate) fn get_nodes(&self, _service: &Service) -> Option<&[NodeInfo]> {
        Some(&self.nodes)
    }

    pub(crate) fn get_all_nodes(&self) -> Option<&[NodeInfo]> {
        Some(&self.nodes)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.nodes.len() == 0
    }

    fn is_pessimized(&self, uri: &Uri) -> bool {
        self.pessimized_nodes.contains(uri)
    }

    // pessimize return true if state was changed
    pub(crate) fn pessimize(&mut self, uri: &Uri) -> bool {
        if self.is_pessimized(uri) {
            return false;
        };

        self.pessimized_nodes.insert(uri.clone());
        self.build_services();
        true
    }

    // TODO: uncomment if need in read code or remove test
    #[cfg(test)]
    pub(crate) fn with_node_info(mut self, _service: Service, node_info: NodeInfo) -> Self {
        if !self.nodes.contains(&node_info) {
            self.nodes.push(node_info);
        }
        self
    }
}

impl Default for DiscoveryState {
    fn default() -> Self {
        DiscoveryState::new(std::time::Instant::now(), Vec::default())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NodeInfo {
    pub(crate) uri: Uri,
    pub(crate) location: String,
}

impl NodeInfo {
    pub(crate) fn new(uri: Uri, location: String) -> Self {
        Self { uri, location }
    }
}

/// Discovery YDB endpoints
#[async_trait]
pub trait Discovery: Send + Sync + Waiter {
    /// Pessimizes an endpoint.
    ///
    /// Pessimizations are reset after rediscovery.
    fn pessimization(&self, uri: &Uri);

    /// Subscribes to discovery changes.
    fn subscribe(&self) -> BoxStream<'static, Arc<DiscoveryState>>;

    /// Tries to get the current discovery state.
    ///
    /// Return `None` if the discovery state is not initialized yet.
    ///
    /// Guaranteed to always return `Some(_)` from the moment
    /// `Self::wait` has been called successfully.
    fn try_state(&self) -> Option<Arc<DiscoveryState>>;

    /// Returns the current discovery state.
    async fn state(&self) -> Arc<DiscoveryState>;
}

/// Always discovery once static node
///
/// Not used in prod, but may be good for tests
pub struct StaticDiscovery {
    discovery_state: Arc<DiscoveryState>,
}

/// Stub discovery pointed to one endpoint for all services.
///
/// Example:
/// ```no_run
/// # use ydb::{ClientBuilder, StaticDiscovery, YdbResult};
///
/// # #[tokio::main]
/// # async fn main() -> YdbResult<()> {
/// let discovery = StaticDiscovery::new_from_str("grpc://localhost:2136")?;
/// let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?
///     .with_discovery(discovery)
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
impl StaticDiscovery {
    pub fn new_from_str<'a, T: Into<&'a str>>(endpoint: T) -> YdbResult<Self> {
        let endpoint = Uri::from_str(endpoint.into())?;
        let nodes = vec![NodeInfo::new(endpoint, String::new())];

        let state = DiscoveryState::new(std::time::Instant::now(), nodes);
        let state = Arc::new(state);
        Ok(StaticDiscovery {
            discovery_state: state,
        })
    }
}

#[async_trait]
impl Discovery for StaticDiscovery {
    fn pessimization(&self, _uri: &Uri) {
        // pass
    }

    fn subscribe(&self) -> BoxStream<'static, Arc<DiscoveryState>> {
        stream::empty().boxed()
    }

    fn try_state(&self) -> Option<Arc<DiscoveryState>> {
        Some(self.discovery_state.clone())
    }

    async fn state(&self) -> Arc<DiscoveryState> {
        self.discovery_state.clone()
    }
}

#[async_trait]
impl Waiter for StaticDiscovery {
    async fn wait(&self) -> YdbResult<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct TimerDiscovery {
    state: Arc<DiscoverySharedState>,
}

impl TimerDiscovery {
    pub(crate) fn new(
        connection_manager: DiscoveryConnectionManager,
        endpoint: &str,
        interval: Duration,
        token_waiter: impl Waiter + 'static,
    ) -> YdbResult<Self> {
        let state = Arc::new(DiscoverySharedState::new(connection_manager, endpoint)?);

        let state_weak = Arc::downgrade(&state);
        tokio::spawn(DiscoverySharedState::background_discovery(
            state_weak,
            interval,
            token_waiter,
        ));

        Ok(TimerDiscovery { state })
    }
}

#[async_trait]
impl Discovery for TimerDiscovery {
    fn pessimization(&self, uri: &Uri) {
        self.state.pessimization(uri);

        // check if need force discovery
        let Some(Ok(state)) = &*self.state.state_sender.borrow() else {
            return;
        };

        let pessimized_nodes_count = state
            .original_nodes
            .iter()
            .filter(|node| state.pessimized_nodes.contains(&node.uri))
            .count();
        if pessimized_nodes_count > 0
            && pessimized_nodes_count >= state.original_nodes.len() / 2
            && self.state.claim_forced_discovery()
        {
            let shared_state_for_discovery = Arc::downgrade(&self.state);
            tokio::spawn(async move {
                if let Some(state) = shared_state_for_discovery.upgrade() {
                    let _ = state.discovery_now().await;
                    state.complete_forced_discovery();
                }
            });
        }
    }

    fn subscribe(&self) -> BoxStream<'static, Arc<DiscoveryState>> {
        self.state.subscribe()
    }

    fn try_state(&self) -> Option<Arc<DiscoveryState>> {
        self.state.try_state()
    }

    async fn state(&self) -> Arc<DiscoveryState> {
        self.state.state().await
    }
}

#[async_trait::async_trait]
impl Waiter for TimerDiscovery {
    async fn wait(&self) -> YdbResult<()> {
        self.state.wait().await
    }
}

#[derive(Debug)]
struct DiscoverySharedState {
    connection_manager: DiscoveryConnectionManager,
    discovery_uri: Uri,

    discovery_lock: tokio::sync::Mutex<()>,
    forced_discovery_in_flight: AtomicBool,

    /// Watch sender for the discovery state changes.
    ///
    /// Initially contains `None`. Contains `Some(Err(err))` if
    /// the first discovery has failed with a non-retriable
    /// error and has not been successfully
    /// retried yet, where `err` is the last non-retriable error
    /// received from the first discovery retries.
    ///
    /// After the first discovery successfully finishes,
    /// the value is always `Some(Ok(state))`, where `state`
    /// is the last successfully received discovery state.
    ///
    /// The discovery will always be retried, regardless of whether
    /// the received error was retriable.
    state_sender: watch::Sender<Option<YdbResult<Arc<DiscoveryState>>>>,
}

impl DiscoverySharedState {
    fn new(connection_manager: DiscoveryConnectionManager, endpoint: &str) -> YdbResult<Self> {
        let (state_sender, _) = watch::channel(None);
        Ok(Self {
            connection_manager,
            discovery_uri: http::Uri::from_str(endpoint)?,
            state_sender,
            discovery_lock: tokio::sync::Mutex::new(()),
            forced_discovery_in_flight: AtomicBool::new(false),
        })
    }

    fn claim_forced_discovery(&self) -> bool {
        self.forced_discovery_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn complete_forced_discovery(&self) {
        self.forced_discovery_in_flight
            .store(false, Ordering::Release);
    }

    #[tracing::instrument(name = "ydb.Discovery.DiscoveryNow", skip_all, err)]
    async fn discovery_now(&self) -> YdbResult<()> {
        let lock = self.discovery_lock.lock().await;

        let discovery_result = self.discovery_now_impl().await.map(Arc::new);

        let result = discovery_result
            .as_ref()
            .map(|_| ())
            .map_err(YdbError::clone);

        self.state_sender
            .send_if_modified(move |state| match (&state, &discovery_result) {
                (_, Err(err)) if err.need_retry() != NeedRetry::False => false,
                (Some(_), Err(_)) => false,
                (None, _) | (Some(_), Ok(_)) => {
                    *state = Some(discovery_result);
                    true
                }
            });

        drop(lock);

        result
    }

    #[tracing::instrument(skip(self))]
    async fn discovery_now_impl(&self) -> YdbResult<DiscoveryState> {
        trace!("creating grpc client");
        let start = std::time::Instant::now();
        let mut discovery_client = self
            .connection_manager
            .get_auth_service_to_node(GrpcDiscoveryClient::new, &self.discovery_uri)
            .await?;

        let res = discovery_client
            .list_endpoints(self.connection_manager.database().to_owned())
            .await?;
        let new_endpoints = Self::list_endpoints_to_node_infos(res)?;

        Ok(DiscoveryState::new(start, new_endpoints))
    }

    #[tracing::instrument(skip(state, token_waiter))]
    async fn background_discovery(
        state: Weak<DiscoverySharedState>,
        interval: Duration,
        token_waiter: impl Waiter,
    ) {
        #[instrument(name = "ydb.Discovery.Timer", skip_all, fields(db.system.name = "ydb", discovery_uri = %state.discovery_uri), err)]
        async fn discovery_once(
            state: Arc<DiscoverySharedState>,
            attempt: usize,
        ) -> Result<(), YdbError> {
            trace!("discovery attempt {attempt}");
            let res = state.discovery_now().await;
            trace!("discovery result: {:?}", res);
            res
        }

        trace!("start background_discovery. Waiting for token renew...");
        // Wait for the token to be available before the first discovery
        // attempt. This ensures that the first `ListEndpoints` gRPC call
        // carries a valid `x-ydb-auth-ticket` header.
        token_waiter.wait().await.unwrap_or_else(|err| {
            warn!("token waiter returned error (ignored): {err}");
        });

        loop {
            let result = ExponentialBackoff::default()
                .retry_indefinitely(closure!([&state], async |retry: &RetryState| {
                    let Some(state) = state.upgrade() else {
                        // Break out of the worker loop
                        return Some(ControlFlow::Break(()));
                    };

                    discovery_once(state, retry.attempt)
                        .await
                        .ok()
                        .map(ControlFlow::Continue)
                }))
                .await;

            if result.is_break() {
                break;
            }

            tokio::time::sleep(interval).await;
        }
        trace!("stop background_discovery");
    }

    fn list_endpoints_to_node_infos(list: Vec<EndpointInfo>) -> YdbResult<Vec<NodeInfo>> {
        list.into_iter()
            .map(|item| match Self::endpoint_info_to_uri(&item) {
                Ok(uri) => YdbResult::<NodeInfo>::Ok(NodeInfo::new(uri, item.location.clone())),
                Err(err) => YdbResult::<NodeInfo>::Err(err),
            })
            .try_collect()
    }

    fn endpoint_info_to_uri(endpoint_info: &EndpointInfo) -> YdbResult<Uri> {
        let authority: Authority =
            Authority::from_str(format!("{}:{}", endpoint_info.fqdn, endpoint_info.port).as_str())?;

        Ok(Uri::builder()
            .scheme(if endpoint_info.ssl { "https" } else { "http" })
            .authority(authority)
            .path_and_query("/")
            .build()?)
    }
}

#[async_trait]
impl Discovery for DiscoverySharedState {
    fn pessimization(&self, uri: &Uri) {
        self.state_sender.send_if_modified(|current| {
            let Some(Ok(state)) = current.as_mut() else {
                // Node pessimization is reset after discovery,
                // so it makes no sense to pessimize a node before
                // the first discovery.
                return false;
            };

            if state.is_pessimized(uri) {
                return false;
            }

            Arc::make_mut(state).pessimize(uri)
        });
    }

    fn subscribe(&self) -> BoxStream<'static, Arc<DiscoveryState>> {
        WatchStream::new(self.state_sender.subscribe())
            .filter_map(|opt_res| future::ready(opt_res.and_then(|res| res.ok())))
            .boxed()
    }

    fn try_state(&self) -> Option<Arc<DiscoveryState>> {
        if let Some(Ok(state)) = &*self.state_sender.borrow() {
            Some(state.clone())
        } else {
            None
        }
    }

    async fn state(&self) -> Arc<DiscoveryState> {
        let mut receiver = self.state_sender.subscribe();

        loop {
            if let Some(Ok(state)) = &*receiver.borrow_and_update() {
                return state.clone();
            }

            receiver
                .changed()
                .await
                .expect("at least one sender is stored in `self` so it cannot be dropped");
        }
    }
}

#[async_trait::async_trait]
impl Waiter for DiscoverySharedState {
    async fn wait(&self) -> YdbResult<()> {
        let mut receiver = self.state_sender.subscribe();

        loop {
            match &*receiver.borrow_and_update() {
                Some(Err(err)) => return Err(err.clone()),
                Some(Ok(_)) => return Ok(()),
                None => (),
            }

            receiver
                .changed()
                .await
                .expect("`self.state_sender` is alive")
        }
    }
}

#[cfg(test)]
mod test {
    use http::Uri;

    use crate::GrpcOptions;
    use crate::client_common::{DBCredentials, TokenCache};
    use crate::discovery::{
        Discovery, DiscoverySharedState, DiscoveryState, NodeInfo, StaticDiscovery, TimerDiscovery,
    };
    use crate::errors::{YdbError, YdbResult};
    use crate::grpc_connection_manager::{DiscoveryConnectionManager, NoBalancer};
    use crate::grpc_wrapper::auth::AuthGrpcInterceptor;
    use crate::grpc_wrapper::raw_discovery_client::EndpointInfo;
    use crate::grpc_wrapper::raw_services::Service;
    use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
    use crate::test_helpers::test_client_builder;
    use crate::waiter::Waiter;
    use futures_util::StreamExt;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    fn discovery_shared_state() -> YdbResult<DiscoverySharedState> {
        const DATABASE: &str = "/local";
        const ENDPOINT: &str = "grpc://localhost:2136";

        let connection_manager = DiscoveryConnectionManager::new(
            NoBalancer,
            DATABASE.to_string(),
            MultiInterceptor::new(),
            GrpcOptions::default(),
        );

        DiscoverySharedState::new(connection_manager, ENDPOINT)
    }

    #[test]
    fn pessimization_completes_after_discovery() -> YdbResult<()> {
        let state = Arc::new(discovery_shared_state()?);
        let endpoint = Uri::from_static("http://localhost:2136");
        state
            .state_sender
            .send_replace(Some(Ok(Arc::new(DiscoveryState::new(
                Instant::now(),
                vec![NodeInfo::new(endpoint.clone(), String::new())],
            )))));

        let state_for_thread = state.clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            state_for_thread.pessimization(&endpoint);
            let _ = done_tx.send(());
        });

        assert!(
            done_rx.recv_timeout(Duration::from_secs(1)).is_ok(),
            "endpoint pessimization deadlocked"
        );

        Ok(())
    }

    #[test]
    fn forced_discovery_coalesces_requests_until_completion() -> YdbResult<()> {
        let state = discovery_shared_state()?;

        assert!(state.claim_forced_discovery());
        assert!(
            !state.claim_forced_discovery(),
            "a refresh already in flight must absorb another request"
        );
        state.complete_forced_discovery();
        assert!(state.claim_forced_discovery());

        Ok(())
    }

    fn node(uri: &'static str) -> NodeInfo {
        NodeInfo::new(Uri::from_static(uri), String::new())
    }

    fn state_with(nodes: Vec<NodeInfo>) -> DiscoveryState {
        DiscoveryState::new(Instant::now(), nodes)
    }

    fn endpoint(fqdn: &str, port: u32, ssl: bool) -> EndpointInfo {
        EndpointInfo {
            fqdn: fqdn.to_string(),
            port,
            ssl,
            location: "dc-1".to_string(),
        }
    }

    /// Publish a ready state so pessimization and `try_state` have something
    /// to act on - both are no-ops before the first discovery.
    fn publish(state: &DiscoverySharedState, nodes: Vec<NodeInfo>) {
        state
            .state_sender
            .send_replace(Some(Ok(Arc::new(state_with(nodes)))));
    }

    // ---- DiscoveryState -------------------------------------------------

    #[test]
    fn new_state_exposes_every_node() {
        let state = state_with(vec![node("http://a:2136"), node("http://b:2136")]);

        assert!(!state.is_empty());
        assert_eq!(state.get_nodes(&Service::Table).map(<[_]>::len), Some(2));
        assert_eq!(state.get_all_nodes().map(<[_]>::len), Some(2));
    }

    #[test]
    fn default_state_is_empty() {
        let state = DiscoveryState::default();

        assert!(state.is_empty());
        assert_eq!(state.get_all_nodes(), Some(&[][..]));
    }

    #[test]
    fn pessimize_hides_the_node_and_is_idempotent() {
        let bad = Uri::from_static("http://a:2136");
        let mut state = state_with(vec![node("http://a:2136"), node("http://b:2136")]);

        assert!(
            state.pessimize(&bad),
            "first pessimization must change state"
        );
        assert!(state.is_pessimized(&bad));
        let remaining = state.get_all_nodes().expect("nodes");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].uri, Uri::from_static("http://b:2136"));

        assert!(
            !state.pessimize(&bad),
            "pessimizing the same node twice must not change state"
        );
    }

    /// Hiding every node would leave the balancer with nothing to pick, so the
    /// full set is restored once all of them are pessimized.
    #[test]
    fn pessimizing_every_node_falls_back_to_the_full_set() {
        let mut state = state_with(vec![node("http://a:2136"), node("http://b:2136")]);

        state.pessimize(&Uri::from_static("http://a:2136"));
        state.pessimize(&Uri::from_static("http://b:2136"));

        assert_eq!(state.get_all_nodes().map(<[_]>::len), Some(2));
        assert!(!state.is_empty());
    }

    #[test]
    fn with_node_info_appends_each_node_once() {
        let extra = node("http://c:2136");

        let state = state_with(vec![node("http://a:2136")])
            .with_node_info(Service::Table, extra.clone())
            .with_node_info(Service::Table, extra);

        assert_eq!(state.get_all_nodes().map(<[_]>::len), Some(2));
    }

    // ---- endpoint conversion --------------------------------------------

    #[test]
    fn endpoints_convert_to_uris_by_ssl_flag() -> YdbResult<()> {
        let nodes = DiscoverySharedState::list_endpoints_to_node_infos(vec![
            endpoint("plain.example.com", 2136, false),
            endpoint("secure.example.com", 2135, true),
        ])?;

        assert_eq!(nodes.len(), 2);
        assert_eq!(
            nodes[0].uri,
            Uri::from_static("http://plain.example.com:2136/")
        );
        assert_eq!(
            nodes[1].uri,
            Uri::from_static("https://secure.example.com:2135/")
        );
        assert_eq!(nodes[0].location, "dc-1");

        Ok(())
    }

    #[test]
    fn malformed_endpoint_is_rejected() {
        let err = DiscoverySharedState::list_endpoints_to_node_infos(vec![endpoint(
            "not a hostname",
            2136,
            false,
        )])
        .expect_err("an invalid authority must be reported");

        assert!(!err.to_string().is_empty());
    }

    // ---- StaticDiscovery -------------------------------------------------

    #[tokio::test]
    async fn static_discovery_always_returns_its_endpoint() -> YdbResult<()> {
        let discovery = StaticDiscovery::new_from_str("grpc://localhost:2136")?;

        discovery.wait().await?;
        let state = discovery.state().await;
        assert_eq!(state.get_all_nodes().map(<[_]>::len), Some(1));

        // pessimization is a no-op and the subscription is empty by design
        discovery.pessimization(&Uri::from_static("grpc://localhost:2136"));
        assert_eq!(
            discovery
                .try_state()
                .map(|s| s.get_all_nodes().map(<[_]>::len)),
            Some(Some(1))
        );
        assert!(discovery.subscribe().next().await.is_none());

        Ok(())
    }

    #[test]
    fn static_discovery_rejects_a_malformed_endpoint() {
        assert!(StaticDiscovery::new_from_str("not a uri").is_err());
    }

    // ---- DiscoverySharedState state plumbing ------------------------------

    #[tokio::test]
    async fn state_is_unavailable_until_the_first_discovery() -> YdbResult<()> {
        let state = discovery_shared_state()?;

        assert!(state.try_state().is_none());
        // pessimization before the first discovery is deliberately ignored
        state.pessimization(&Uri::from_static("http://a:2136"));
        assert!(state.try_state().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn state_resolves_once_discovery_publishes() -> YdbResult<()> {
        let state = Arc::new(discovery_shared_state()?);

        let waiter = state.clone();
        let pending = tokio::spawn(async move { waiter.state().await });

        publish(&state, vec![node("http://a:2136")]);

        let resolved = tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .expect("state() must resolve once a state is published")
            .expect("task should not panic");
        assert_eq!(resolved.get_all_nodes().map(<[_]>::len), Some(1));

        Ok(())
    }

    #[tokio::test]
    async fn wait_reports_the_first_discovery_error() -> YdbResult<()> {
        let state = discovery_shared_state()?;
        state
            .state_sender
            .send_replace(Some(Err(YdbError::custom("discovery failed"))));

        let err = state.wait().await.expect_err("a stored error must surface");
        assert!(err.to_string().contains("discovery failed"));
        assert!(state.try_state().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn wait_returns_once_a_state_exists() -> YdbResult<()> {
        let state = discovery_shared_state()?;
        publish(&state, vec![node("http://a:2136")]);

        state.wait().await?;

        Ok(())
    }

    #[tokio::test]
    async fn subscribe_yields_successful_states_only() -> YdbResult<()> {
        let state = discovery_shared_state()?;
        let mut updates = state.subscribe();

        state
            .state_sender
            .send_replace(Some(Err(YdbError::custom("transient"))));
        publish(&state, vec![node("http://a:2136")]);

        let update = tokio::time::timeout(Duration::from_secs(1), updates.next())
            .await
            .expect("an update must arrive")
            .expect("stream must not end while the sender is alive");
        assert_eq!(update.get_all_nodes().map(<[_]>::len), Some(1));

        Ok(())
    }

    #[tokio::test]
    async fn pessimization_updates_the_published_state() -> YdbResult<()> {
        let state = discovery_shared_state()?;
        publish(&state, vec![node("http://a:2136"), node("http://b:2136")]);

        state.pessimization(&Uri::from_static("http://a:2136"));

        let current = state.try_state().expect("state was published");
        assert_eq!(current.get_all_nodes().map(<[_]>::len), Some(1));

        // repeating it leaves the state untouched
        state.pessimization(&Uri::from_static("http://a:2136"));
        assert_eq!(
            state.try_state().map(|s| s.get_all_nodes().map(<[_]>::len)),
            Some(Some(1))
        );

        Ok(())
    }

    // ---- forced rediscovery ----------------------------------------------

    #[tokio::test]
    async fn discovery_now_reports_an_unreachable_endpoint() -> YdbResult<()> {
        const CLOSED_PORT: &str = "grpc://127.0.0.1:1";

        let connection_manager = DiscoveryConnectionManager::new(
            NoBalancer,
            "/local".to_string(),
            MultiInterceptor::new(),
            GrpcOptions::default(),
        );
        let state = DiscoverySharedState::new(connection_manager, CLOSED_PORT)?;

        // Loopback refuses immediately, so this needs no server and no network.
        state
            .discovery_now()
            .await
            .expect_err("an unreachable endpoint must not yield a state");
        assert!(state.try_state().is_none());

        Ok(())
    }

    fn timer_discovery_to_closed_port() -> YdbResult<TimerDiscovery> {
        let connection_manager = DiscoveryConnectionManager::new(
            NoBalancer,
            "/local".to_string(),
            MultiInterceptor::new(),
            GrpcOptions::default(),
        );

        TimerDiscovery::new(
            connection_manager,
            "grpc://127.0.0.1:1",
            // long enough that the periodic timer never fires during a test
            Duration::from_secs(3600),
            StaticDiscovery::new_from_str("grpc://127.0.0.1:1")?,
        )
    }

    /// Wait until the spawned refresh has released the in-flight flag.
    async fn await_forced_discovery(state: &DiscoverySharedState) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while state.forced_discovery_in_flight.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the spawned refresh must release the in-flight flag");
    }

    #[tokio::test]
    async fn timer_discovery_ignores_pessimization_before_the_first_discovery() -> YdbResult<()> {
        let discovery = timer_discovery_to_closed_port()?;

        discovery.pessimization(&Uri::from_static("http://a:2136"));

        assert!(discovery.try_state().is_none());
        assert!(
            !discovery
                .state
                .forced_discovery_in_flight
                .load(Ordering::Acquire),
            "no refresh should be scheduled before the first discovery"
        );

        Ok(())
    }

    /// A pessimization storm must schedule one refresh, not one per failure.
    #[tokio::test]
    async fn timer_discovery_pessimization_spawns_one_refresh() -> YdbResult<()> {
        let discovery = timer_discovery_to_closed_port()?;
        publish(
            &discovery.state,
            vec![node("http://a:2136"), node("http://b:2136")],
        );

        // Half the nodes pessimized is the threshold that triggers a refresh.
        // The refresh targets a closed port, so it fails fast and releases the
        // flag; that release is what proves the spawned task actually ran.
        discovery.pessimization(&Uri::from_static("http://a:2136"));
        await_forced_discovery(&discovery.state).await;

        assert_eq!(
            discovery
                .try_state()
                .map(|s| s.get_all_nodes().map(<[_]>::len)),
            Some(Some(1))
        );

        // A second storm can schedule a fresh refresh once the first finished.
        discovery.pessimization(&Uri::from_static("http://b:2136"));
        await_forced_discovery(&discovery.state).await;

        discovery.wait().await?;
        assert_eq!(
            discovery.state().await.get_all_nodes().map(<[_]>::len),
            Some(2)
        );

        let mut updates = discovery.subscribe();
        assert!(updates.next().await.is_some());

        Ok(())
    }

    /// The background loop must stop once the shared state is gone rather than
    /// retrying against a dropped driver forever.
    #[tokio::test]
    async fn background_discovery_stops_when_the_state_is_dropped() -> YdbResult<()> {
        let state = Arc::new(discovery_shared_state()?);
        let weak = Arc::downgrade(&state);
        drop(state);

        tokio::time::timeout(
            Duration::from_secs(5),
            DiscoverySharedState::background_discovery(
                weak,
                Duration::from_secs(3600),
                StaticDiscovery::new_from_str("grpc://127.0.0.1:1")?,
            ),
        )
        .await
        .expect("background discovery must exit once the state is dropped");

        Ok(())
    }

    #[tokio::test]
    #[ignore] // need YDB access
    async fn test_background_discovery() -> YdbResult<()> {
        let cred = DBCredentials {
            database: test_client_builder().database.clone(),
            token_cache: TokenCache::new(test_client_builder().credentials.clone()),
        };

        let interceptor =
            MultiInterceptor::new().with_interceptor(AuthGrpcInterceptor::new(cred.clone())?);

        let connection_manager = DiscoveryConnectionManager::new(
            NoBalancer,
            cred.database,
            interceptor,
            GrpcOptions::default(),
        );

        let discovery_shared =
            DiscoverySharedState::new(connection_manager, test_client_builder().endpoint.as_str())?;

        let state = Arc::new(discovery_shared);
        let mut rx = state.state_sender.subscribe();
        // skip initial value
        rx.borrow_and_update();

        let state_weak = Arc::downgrade(&state);

        tokio::spawn(DiscoverySharedState::background_discovery(
            state_weak,
            Duration::from_millis(50),
            cred.token_cache.clone(),
        ));

        // wait two updates
        for _ in 0..2 {
            rx.changed().await.unwrap();
            assert!(
                !rx.borrow()
                    .as_ref()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .nodes
                    .is_empty()
            );
        }

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_wrong_db_name() {
        tokio::time::timeout(Duration::from_secs(5), test_client_builder().build())
            .await
            .unwrap()
            .unwrap();

        let bad_client_builder = test_client_builder().with_database("/some-amogus-db");

        assert!(
            tokio::time::timeout(Duration::from_secs(5), bad_client_builder.build())
                .await
                .unwrap()
                .is_err()
        );
    }
}
