use crate::client::Client;
use crate::client::TimeoutSettings;
use crate::errors::{YdbError, YdbResult};
use crate::test_helpers::test_custom_ca_client_builder;
use crate::test_helpers::{test_client_builder, test_with_password_builder};
use async_once::AsyncOnce;
use lazy_static::lazy_static;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::trace;

lazy_static! {
    static ref TEST_CLIENT: AsyncOnce<Arc<Client>> = AsyncOnce::new(async {
        trace!("create client");
        connect().await.unwrap()
    });
}

#[tracing::instrument]
pub(crate) async fn create_client() -> YdbResult<Arc<Client>> {
    trace!("get client");
    // https://github.com/ydb-platform/ydb-rs-sdk/issues/92
    // return Ok(TEST_CLIENT.get().await.clone());
    connect().await
}

async fn connect() -> YdbResult<Arc<Client>> {
    let client = test_client_builder()
        .client()
        .unwrap()
        .with_timeouts(TimeoutSettings {
            operation_timeout: std::time::Duration::from_secs(60),
        });

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
    let client = test_custom_ca_client_builder()
        .client()
        .unwrap()
        .with_timeouts(TimeoutSettings {
            operation_timeout: std::time::Duration::from_secs(60),
        });
    trace!("start wait");
    client.wait().await.unwrap();
    Ok(Arc::new(client))
}

pub(crate) struct TcpForwardProxy {
    listen_addr: SocketAddr,
    allow_forward: watch::Sender<bool>,
    accept_loop: JoinHandle<()>,
}

impl TcpForwardProxy {
    /// `connection_string` is the same format as `YDB_CONNECTION_STRING` / `test_helpers::CONNECTION_STRING`.
    pub(crate) async fn start(connection_string: &str) -> YdbResult<Self> {
        let target = ydb_grpc_socket_addr(connection_string).await?;
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| YdbError::custom(format!("TcpForwardingProxy bind: {e}")))?;
        let listen_addr = listener
            .local_addr()
            .map_err(|e| YdbError::custom(format!("TcpForwardingProxy local_addr: {e}")))?;
        let (allow_forward, allow_rx) = watch::channel(true);
        let accept_loop = tokio::spawn(accept_loop(listener, target, allow_rx));
        Ok(Self {
            listen_addr,
            allow_forward,
            accept_loop,
        })
    }
    pub(crate) fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }
    pub(crate) fn set_forwarding(&self, enabled: bool) {
        let _ = self.allow_forward.send(enabled);
    }
}

impl Drop for TcpForwardProxy {
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}
async fn accept_loop(listener: TcpListener, target: SocketAddr, allow_rx: watch::Receiver<bool>) {
    loop {
        let (inbound, _) = match listener.accept().await {
            Ok(x) => x,
            Err(_) => break,
        };
        if !*allow_rx.borrow() {
            drop(inbound);
            continue;
        }
        let allow_rx_conn = allow_rx.clone();
        tokio::spawn(forward_one(inbound, target, allow_rx_conn));
    }
}
async fn forward_one(
    mut inbound: TcpStream,
    target: SocketAddr,
    mut allow_rx: watch::Receiver<bool>,
) {
    let mut outbound = match TcpStream::connect(target).await {
        Ok(s) => s,
        Err(_) => return,
    };
    tokio::select! {
        _ = wait_forwarding_disabled(&mut allow_rx) => {}
        _ = copy_bidirectional(&mut inbound, &mut outbound) => {}
    }
}
async fn wait_forwarding_disabled(allow_rx: &mut watch::Receiver<bool>) {
    loop {
        if !*allow_rx.borrow() {
            return;
        }
        if allow_rx.changed().await.is_err() {
            return;
        }
    }
}

async fn ydb_grpc_socket_addr(connection_string: &str) -> YdbResult<SocketAddr> {
    let u = url::Url::parse(connection_string)
        .map_err(|e| YdbError::custom(format!("invalid connection string: {e}")))?;
    let host = u
        .host_str()
        .ok_or_else(|| YdbError::custom("connection string: missing host"))?
        .to_string();
    let port = u
        .port()
        .ok_or_else(|| YdbError::custom("connection string: missing port"))?;
    if host.eq_ignore_ascii_case("localhost") {
        return Ok(SocketAddr::from(([127, 0, 0, 1], port)));
    }
    let mut addrs = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|e| YdbError::custom(format!("lookup_host: {e}")))?;
    addrs
        .next()
        .ok_or_else(|| YdbError::custom("connection string: no resolved addresses"))
}
