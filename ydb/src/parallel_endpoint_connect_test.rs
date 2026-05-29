use crate::connection_pool::ConnectionPool;
use crate::parallel_endpoint_connect::{connect, parallel_connect, ConnectTimeouts};
use crate::YdbResult;
use http::Uri;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::server::health_reporter;

async fn spawn_mock_grpc_server(addr: SocketAddr) -> (CancellationToken, JoinHandle<()>) {
    let shutdown = CancellationToken::new();
    let shutdown_on_cancel = shutdown.child_token();

    let (_reporter, health_service) = health_reporter();

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(health_service)
            .serve_with_shutdown(addr, shutdown_on_cancel.cancelled())
            .await
            .expect("mock gRPC server failed");
    });

    wait_until_server_accepts(addr).await;
    (shutdown, handle)
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

async fn reserve_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind ephemeral port");
    listener.local_addr().expect("failed to read local addr")
}

fn test_connect_timeouts() -> ConnectTimeouts {
    ConnectTimeouts {
        per_endpoint: Duration::from_millis(500),
        parallel_overall: Duration::from_millis(750),
    }
}

#[tokio::test]
async fn parallel_connect_skips_unreachable_ip() -> YdbResult<()> {
    let live_addr = reserve_local_addr().await;
    let (shutdown, server_handle) = spawn_mock_grpc_server(live_addr).await;

    let dead_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), live_addr.port());
    let original_uri = Uri::from_static("grpc://ydb.test.local:2135/");

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
    let closed_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind ephemeral port");
    let closed_addr = closed_listener
        .local_addr()
        .expect("failed to read local addr");
    drop(closed_listener);

    let another_closed_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind ephemeral port");
    let another_closed_addr = another_closed_listener
        .local_addr()
        .expect("failed to read local addr");
    drop(another_closed_listener);

    let original_uri = Uri::from_static("grpc://ydb.test.local:2135/");
    let result = parallel_connect(
        vec![closed_addr, another_closed_addr],
        original_uri,
        &None,
        test_connect_timeouts(),
    )
    .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn connect_uses_parallel_dial_for_localhost_with_multiple_records() -> YdbResult<()> {
    let live_addr = reserve_local_addr().await;
    let port = live_addr.port();

    let resolved_addrs: Vec<SocketAddr> = tokio::net::lookup_host(("localhost", port))
        .await
        .map_err(|e| crate::YdbError::Custom(format!("failed to resolve localhost: {e}")))?
        .collect();

    if resolved_addrs.len() < 2 {
        eprintln!("skip: localhost resolves to a single address on this host");
        return Ok(());
    }

    let (shutdown, server_handle) = spawn_mock_grpc_server(live_addr).await;

    let uri = Uri::try_from(format!("grpc://localhost:{port}/")).expect("valid uri");
    let channel = connect(uri, &None, test_connect_timeouts()).await?;
    health_check(channel).await?;

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(1), server_handle)
        .await
        .expect("server shutdown timed out");

    Ok(())
}

#[tokio::test]
async fn connection_pool_parallel_dial_through_localhost() -> YdbResult<()> {
    let live_addr = reserve_local_addr().await;
    let port = live_addr.port();

    let resolved_addrs: Vec<SocketAddr> = tokio::net::lookup_host(("localhost", port))
        .await
        .map_err(|e| crate::YdbError::Custom(format!("failed to resolve localhost: {e}")))?
        .collect();

    if resolved_addrs.len() < 2 {
        eprintln!("skip: localhost resolves to a single address on this host");
        return Ok(());
    }

    let (shutdown, server_handle) = spawn_mock_grpc_server(live_addr).await;

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
