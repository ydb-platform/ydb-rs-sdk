use crate::connection_pool::ConnectionPool;
use crate::parallel_endpoint_connect::{
    connect_resolved, normalize_uri_scheme, parallel_connect, ConnectTimeouts,
};
use crate::YdbResult;
use http::Uri;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, Server, ServerTlsConfig};
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::server::health_reporter;

fn normalized_test_uri(s: &str) -> Uri {
    normalize_uri_scheme(Uri::try_from(s).expect("valid uri")).expect("normalize uri")
}

async fn spawn_mock_grpc_server(
    listener: TcpListener,
) -> (SocketAddr, CancellationToken, JoinHandle<()>) {
    spawn_mock_grpc_server_with_tls(listener, None).await
}

async fn spawn_mock_grpc_server_with_tls(
    listener: TcpListener,
    tls_config: Option<ServerTlsConfig>,
) -> (SocketAddr, CancellationToken, JoinHandle<()>) {
    let addr = listener
        .local_addr()
        .expect("failed to read bound listener address");
    let shutdown = CancellationToken::new();
    let shutdown_on_cancel = shutdown.child_token();
    let incoming = TcpListenerStream::new(listener);

    let (_reporter, health_service) = health_reporter();

    let handle = tokio::spawn(async move {
        let server = if let Some(tls_config) = tls_config {
            Server::builder()
                .tls_config(tls_config)
                .expect("valid server TLS config")
                .add_service(health_service)
        } else {
            Server::builder().add_service(health_service)
        };

        server
            .serve_with_incoming_shutdown(incoming, shutdown_on_cancel.cancelled())
            .await
            .expect("mock gRPC server failed");
    });

    wait_until_server_accepts(addr).await;
    (addr, shutdown, handle)
}

async fn wait_until_server_accepts(addr: SocketAddr) {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("mock gRPC server did not start on {addr}");
}

async fn health_check(channel: tonic::transport::Channel) -> YdbResult<()> {
    let mut client = HealthClient::new(channel);
    let response = client
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .map_err(|e| crate::YdbError::Custom(format!("health check failed: {e}")))?;

    assert_eq!(
        response.into_inner().status,
        tonic_health::pb::health_check_response::ServingStatus::Serving as i32
    );

    Ok(())
}

async fn bind_local_listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind ephemeral port")
}

fn test_connect_timeouts() -> ConnectTimeouts {
    ConnectTimeouts {
        per_endpoint: Duration::from_millis(500),
        parallel_overall: Duration::from_millis(750),
    }
}

fn unreachable_local_addr(last_octet: u8) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, last_octet)), 1)
}

#[tokio::test]
async fn parallel_connect_skips_unreachable_ip() -> YdbResult<()> {
    let listener = bind_local_listener().await;
    let (live_addr, shutdown, server_handle) = spawn_mock_grpc_server(listener).await;

    let dead_addr = unreachable_local_addr(2);
    let dead_addr = SocketAddr::new(dead_addr.ip(), live_addr.port());
    let original_uri = normalized_test_uri(&format!("grpc://localhost:{}/", live_addr.port()));

    let channel = parallel_connect(
        vec![dead_addr, live_addr],
        original_uri,
        &None,
        test_connect_timeouts(),
    )
    .await?;
    health_check(channel).await?;

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(1), server_handle)
        .await
        .expect("server shutdown timed out");

    Ok(())
}

#[tokio::test]
async fn parallel_connect_fails_when_all_ips_unreachable() {
    let original_uri = normalized_test_uri("grpc://ydb.test.local/");
    let result = parallel_connect(
        vec![unreachable_local_addr(2), unreachable_local_addr(3)],
        original_uri,
        &None,
        test_connect_timeouts(),
    )
    .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn connect_resolved_uses_parallel_dial() -> YdbResult<()> {
    let listener = bind_local_listener().await;
    let (live_addr, shutdown, server_handle) = spawn_mock_grpc_server(listener).await;

    let dead_addr = SocketAddr::new(unreachable_local_addr(2).ip(), live_addr.port());
    let uri = normalized_test_uri(&format!("grpc://localhost:{}/", live_addr.port()));

    let channel = connect_resolved(
        uri,
        vec![dead_addr, live_addr],
        &None,
        test_connect_timeouts(),
    )
    .await?;
    health_check(channel).await?;

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(1), server_handle)
        .await
        .expect("server shutdown timed out");

    Ok(())
}

#[tokio::test]
async fn parallel_connect_over_tls_dials_by_ip_with_sni() -> YdbResult<()> {
    let hostname = "tls-parallel.test";
    let certified = rcgen::generate_simple_self_signed(vec![hostname.into()])
        .expect("failed to generate self-signed certificate");
    let cert_pem = certified.cert.pem();
    let key_pem = certified.key_pair.serialize_pem();
    let identity = Identity::from_pem(cert_pem.as_bytes(), key_pem.as_bytes());
    let server_tls = ServerTlsConfig::new().identity(identity);

    let listener = bind_local_listener().await;
    let (live_addr, shutdown, server_handle) =
        spawn_mock_grpc_server_with_tls(listener, Some(server_tls)).await;

    let dead_addr = SocketAddr::new(unreachable_local_addr(2).ip(), live_addr.port());
    let original_uri = normalized_test_uri(&format!("grpcs://{hostname}:{}/", live_addr.port()));
    let client_tls =
        Some(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(cert_pem.as_bytes())));

    let channel = parallel_connect(
        vec![dead_addr, live_addr],
        original_uri,
        &client_tls,
        test_connect_timeouts(),
    )
    .await?;
    health_check(channel).await?;

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(1), server_handle)
        .await
        .expect("server shutdown timed out");

    Ok(())
}

#[tokio::test]
async fn connection_pool_reaches_mock_server() -> YdbResult<()> {
    let listener = bind_local_listener().await;
    let port = listener
        .local_addr()
        .expect("failed to read local addr")
        .port();

    let (_live_addr, shutdown, server_handle) = spawn_mock_grpc_server(listener).await;

    let pool = ConnectionPool::with_connect_timeouts(test_connect_timeouts());
    let uri = Uri::try_from(format!("grpc://127.0.0.1:{port}/")).expect("valid uri");
    let channel = pool.connection(&uri).await?;
    health_check(channel).await?;

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(1), server_handle)
        .await
        .expect("server shutdown timed out");

    Ok(())
}

#[tokio::test]
async fn connection_pool_resolves_hostname_with_parallel_dial() -> YdbResult<()> {
    let listener = bind_local_listener().await;
    let port = listener
        .local_addr()
        .expect("failed to read local addr")
        .port();

    let resolved: Vec<SocketAddr> = tokio::net::lookup_host(("localhost", port))
        .await
        .expect("failed to resolve localhost")
        .collect();
    if resolved.len() < 2 {
        return Ok(());
    }

    let (_live_addr, shutdown, server_handle) = spawn_mock_grpc_server(listener).await;

    let pool = ConnectionPool::with_connect_timeouts(test_connect_timeouts());
    let uri = Uri::try_from(format!("grpc://localhost:{port}/")).expect("valid uri");
    let channel = pool.connection(&uri).await?;
    health_check(channel).await?;

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(1), server_handle)
        .await
        .expect("server shutdown timed out");

    Ok(())
}
