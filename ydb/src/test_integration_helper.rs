use crate::Executor;
use crate::client::Client;
use crate::errors::{YdbError, YdbResult};
use crate::session_pool::SessionPoolSettings;
use crate::test_helpers::test_custom_ca_client_builder;
use crate::test_helpers::{test_client_builder, test_with_password_builder};
use async_once::AsyncOnce;
use lazy_static::lazy_static;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::trace;

/// Executor that runs tasks immediately on the caller thread.
pub(crate) struct InplaceExecutor;

impl Executor for InplaceExecutor {
    fn available_parallelism(&self) -> NonZeroUsize {
        const { NonZeroUsize::new(1).unwrap() }
    }

    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        task();
    }
}

lazy_static! {
    static ref TEST_CLIENT: AsyncOnce<Arc<Client>> = AsyncOnce::new(async {
        trace!("create client");
        connect(Arc::new(InplaceExecutor)).await.unwrap()
    });
}

pub(crate) async fn create_client_with_executor(
    executor: Arc<dyn Executor>,
) -> YdbResult<Arc<Client>> {
    trace!("get client");
    // https://github.com/ydb-platform/ydb-rs-sdk/issues/92
    // return Ok(TEST_CLIENT.get().await.clone());
    connect(executor).await
}

#[tracing::instrument]
pub(crate) async fn create_client() -> YdbResult<Arc<Client>> {
    create_client_with_executor(Arc::new(InplaceExecutor)).await
}

#[tracing::instrument]
pub(crate) async fn create_client_with_session_pool(
    settings: SessionPoolSettings,
) -> YdbResult<Arc<Client>> {
    let client = test_client_builder()
        .with_executor(Arc::new(InplaceExecutor))
        .client()?;
    client.wait().await?;
    Ok(Arc::new(client.with_session_pool(settings).await?))
}

async fn connect(executor: Arc<dyn Executor>) -> YdbResult<Arc<Client>> {
    let client = test_client_builder()
        .with_executor(executor)
        .client()
        .unwrap();

    trace!("start wait");
    client.wait().await.unwrap();
    Ok(Arc::new(client))
}

#[tracing::instrument]
pub(crate) async fn create_password_client() -> YdbResult<Arc<Client>> {
    let client = test_with_password_builder().client().unwrap();
    trace!("start wait");
    client.wait().await.unwrap();
    Ok(Arc::new(client))
}

#[tracing::instrument]
pub(crate) async fn create_custom_ca_client() -> YdbResult<Arc<Client>> {
    let client = test_custom_ca_client_builder().client().unwrap();
    trace!("start wait");
    client.wait().await.unwrap();
    Ok(Arc::new(client))
}

pub(crate) struct TcpForwardProxy {
    listen_addr: SocketAddr,
    allow_tx: watch::Sender<bool>,
    accept_loop_handle: JoinHandle<()>,
}

impl TcpForwardProxy {
    pub(crate) async fn start(connection_string: &str) -> YdbResult<Self> {
        let target = ydb_connection_string_to_socket_addr(connection_string).await?;
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| YdbError::custom(format!("TcpForwardProxy bind: {e}")))?;
        let listen_addr = listener
            .local_addr()
            .map_err(|e| YdbError::custom(format!("TcpForwardProxy local_addr: {e}")))?;

        let (allow_tx, allow_rx) = watch::channel(true);
        let accept_loop = tokio::spawn(Self::accept_loop(listener, target, allow_rx));

        Ok(Self {
            listen_addr,
            allow_tx,
            accept_loop_handle: accept_loop,
        })
    }

    pub(crate) fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    pub(crate) fn set_allow_forward(&self, allow: bool) {
        let _ = self.allow_tx.send(allow);
    }

    async fn accept_loop(
        listener: TcpListener,
        target: SocketAddr,
        allow_rx: watch::Receiver<bool>,
    ) {
        loop {
            let Ok((inbound, _)) = listener.accept().await else {
                break;
            };

            if !*allow_rx.borrow() {
                continue;
            }

            tokio::spawn(Self::forward(inbound, target, allow_rx.clone()));
        }
    }

    async fn forward(
        mut inbound: TcpStream,
        target: SocketAddr,
        mut allow_rx: watch::Receiver<bool>,
    ) {
        let Ok(mut outbound) = TcpStream::connect(target).await else {
            return;
        };

        tokio::select! {
            _ = Self::wait_for_forwarding_disabled(&mut allow_rx) => {}
            _ = copy_bidirectional(&mut inbound, &mut outbound) => {}
        }
    }

    /// Waits until forwarding is disabled (or the watch::Sender is dropped).
    async fn wait_for_forwarding_disabled(allow_rx: &mut watch::Receiver<bool>) {
        loop {
            let forwarding_allowed = *allow_rx.borrow();
            if !forwarding_allowed {
                return;
            }
            if allow_rx.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Drop for TcpForwardProxy {
    fn drop(&mut self) {
        self.accept_loop_handle.abort();
    }
}

async fn ydb_connection_string_to_socket_addr(connection_string: &str) -> YdbResult<SocketAddr> {
    let url = url::Url::parse(connection_string).map_err(|err| {
        YdbError::custom(format!(
            "tcp_forward_proxy: invalid connection string: {err}"
        ))
    })?;
    let host = url
        .host_str()
        .ok_or_else(|| YdbError::custom("tcp_forward_proxy: connection string has no host"))?
        .to_string();
    let port = url
        .port()
        .ok_or_else(|| YdbError::custom("tcp_forward_proxy: connection string has no port"))?;

    // For some reason, it doesn't work without this.
    if host.eq_ignore_ascii_case("localhost") {
        return Ok(SocketAddr::from(([127, 0, 0, 1], port)));
    }

    let mut addrs = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|err| YdbError::custom(format!("tcp_forward_proxy: lookup_host failed: {err}")))?;

    addrs
        .next()
        .ok_or_else(|| YdbError::custom("tcp_forward_proxy: host resolved to no addresses"))
}
