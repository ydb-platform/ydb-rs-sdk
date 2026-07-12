use std::collections::HashSet;
use std::future;
use std::str::FromStr;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use derivative::Derivative;
use futures_util::StreamExt;
use futures_util::stream::{self, BoxStream};
use http::Uri;
use http::uri::Authority;
use itertools::Itertools;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tracing::trace;

use crate::YdbError;
use crate::errors::{NeedRetry, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::{
    raw_discovery_client::{EndpointInfo, GrpcDiscoveryClient},
    raw_services::Service,
};
use crate::retry::{IndefiniteRetrier, Retry, RetryParams};
use crate::waiter::Waiter;

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

    // pessimize return true if state was changed
    pub(crate) fn pessimize(&mut self, uri: &Uri) -> bool {
        if self.pessimized_nodes.contains(uri) {
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
/// # fn main()->YdbResult<()>{
/// let discovery = StaticDiscovery::new_from_str("grpc://localhost:2136")?;
/// let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?.with_discovery(discovery).client()?;
/// # return Ok(());
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
    #[allow(dead_code)]
    pub(crate) fn new(
        connection_manager: GrpcConnectionManager,
        endpoint: &str,
        interval: Duration,
    ) -> YdbResult<Self> {
        let state = Arc::new(DiscoverySharedState::new(connection_manager, endpoint)?);

        let state_weak = Arc::downgrade(&state);
        tokio::spawn(DiscoverySharedState::background_discovery(
            state_weak, interval,
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
        if pessimized_nodes_count > 0 && pessimized_nodes_count >= state.original_nodes.len() / 2 {
            let shared_state_for_discovery = Arc::downgrade(&self.state);
            tokio::spawn(async move {
                if let Some(state) = shared_state_for_discovery.upgrade() {
                    let _ = state.discovery_now().await;
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

#[derive(Derivative)]
#[derivative(Debug)]
struct DiscoverySharedState {
    #[derivative(Debug = "ignore")]
    connection_manager: GrpcConnectionManager,
    discovery_uri: Uri,

    discovery_lock: tokio::sync::Mutex<()>,

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
    fn new(connection_manager: GrpcConnectionManager, endpoint: &str) -> YdbResult<Self> {
        let (state_sender, _) = watch::channel(None);

        Ok(Self {
            connection_manager,
            discovery_uri: http::Uri::from_str(endpoint)?,
            state_sender,
            discovery_lock: tokio::sync::Mutex::new(()),
        })
    }

    #[tracing::instrument(skip(self))]
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

    #[tracing::instrument(skip(state))]
    async fn background_discovery(state: Weak<DiscoverySharedState>, interval: Duration) {
        'worker: loop {
            let mut attempt = 0;
            let retrier = IndefiniteRetrier;
            let discovery_start = Instant::now();

            'attempt: loop {
                let Some(state) = state.upgrade() else {
                    break 'worker;
                };

                trace!("discovery attempt {attempt}");
                let res = state.discovery_now().await;
                attempt += 1;

                trace!("discovery result: {:?}", res);

                if res.is_ok() {
                    break 'attempt;
                }

                let decision = retrier.retry_decision(RetryParams {
                    attempt,
                    time_from_start: discovery_start.elapsed(),
                });

                if !decision.wait().await {
                    break 'attempt;
                }
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
            .path_and_query("")
            .build()?)
    }
}

#[async_trait]
impl Discovery for DiscoverySharedState {
    fn pessimization(&self, uri: &Uri) {
        let Some(Ok(state)) = &*self.state_sender.borrow() else {
            // Node pessimization is reset after discovery,
            // so it makes no sense to add pessimize node before
            // the first discovery.
            return;
        };

        // TODO: suppress force copy every time
        let mut state = state.as_ref().clone();

        if !state.pessimize(uri) {
            return;
        }

        self.state_sender.send_replace(Some(Ok(Arc::new(state))));
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
    use crate::client_common::{DBCredentials, TokenCache};
    use crate::discovery::{Discovery, DiscoverySharedState, DiscoveryState, NodeInfo};
    use crate::errors::YdbResult;
    use crate::grpc_connection_manager::GrpcConnectionManager;
    use crate::grpc_wrapper::auth::AuthGrpcInterceptor;
    use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
    use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
    use crate::test_helpers::test_client_builder;
    use http::Uri;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn discovery_shared_state() -> YdbResult<DiscoverySharedState> {
        const DATABASE: &str = "/local";
        const ENDPOINT: &str = "grpc://localhost:2136";

        let uri = Uri::from_str(ENDPOINT)?;
        let load_balancer =
            SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(uri)));
        let connection_manager = GrpcConnectionManager::new(
            load_balancer,
            DATABASE.to_string(),
            MultiInterceptor::new(),
            None,
            crate::grpc_wrapper::grpc_limits::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES,
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

    #[tokio::test]
    #[ignore] // need YDB access
    async fn test_background_discovery() -> YdbResult<()> {
        let cred = DBCredentials {
            database: test_client_builder().database.clone(),
            token_cache: tokio::task::spawn_blocking(|| {
                TokenCache::new(test_client_builder().credentials.clone())
            })
            .await??,
        };

        let uri = Uri::from_str(test_client_builder().endpoint.as_str())?;
        let load_balancer =
            SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(uri)));

        let interceptor =
            MultiInterceptor::new().with_interceptor(AuthGrpcInterceptor::new(cred.clone())?);

        let connection_manager = GrpcConnectionManager::new(
            load_balancer,
            cred.database,
            interceptor,
            None,
            crate::grpc_wrapper::grpc_limits::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES,
        );

        let discovery_shared =
            DiscoverySharedState::new(connection_manager, test_client_builder().endpoint.as_str())?;

        let state = Arc::new(discovery_shared);
        let mut rx = state.state_sender.subscribe();
        // skip initial value
        rx.borrow_and_update();

        let state_weak = Arc::downgrade(&state);
        tokio::spawn(async {
            DiscoverySharedState::background_discovery(state_weak, Duration::from_millis(50)).await;
        });

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
        let good_client = test_client_builder().client().unwrap();

        tokio::time::timeout(Duration::from_secs(5), good_client.wait())
            .await
            .unwrap()
            .unwrap();

        let bad_client = test_client_builder()
            .with_database("/some-amogus-db")
            .client()
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), bad_client.wait())
            .await
            .unwrap()
            .unwrap_err();
    }
}
