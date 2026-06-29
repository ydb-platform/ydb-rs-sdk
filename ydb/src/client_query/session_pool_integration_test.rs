use std::time::{Duration, Instant};

use crate::client::TimeoutSettings;
use crate::session_pool::SessionPoolSettings;
use crate::test_helpers::test_client_builder;
use crate::test_integration_helper::create_client_with_session_pool;
use crate::Client;
use std::sync::Arc;

async fn wait_for_idle_sessions(client: &Client, expected_idle: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while client.session_pool_stats().idle < expected_idle {
        assert!(
            Instant::now() < deadline,
            "timeout waiting for session return to pool"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
#[ignore] // need YDB access
async fn query_client_reuses_driver_session_pool() {
    let client = create_client_with_session_pool(SessionPoolSettings::new().with_limit(2))
        .await
        .expect("client");

    let mut query_client = client.query_client();
    let stats = client.session_pool_stats();
    assert_eq!(stats.limit, 2);
    assert_eq!(stats.in_use, 0);
    assert_eq!(stats.idle, 0);

    let _row = query_client
        .query_row("SELECT 1 AS value")
        .await
        .expect("query");

    wait_for_idle_sessions(&client, 1).await;
    let stats = client.session_pool_stats();
    assert_eq!(stats.in_use, 0);
    assert_eq!(stats.idle, 1);
}

#[tokio::test]
#[ignore] // need YDB access
async fn query_client_returns_session_to_driver_pool_after_stream_drop() {
    let client = create_client_with_session_pool(SessionPoolSettings::new().with_limit(2))
        .await
        .expect("client");

    let mut query_client = client.query_client();
    {
        let _row = query_client
            .query_row("SELECT 1 AS value")
            .await
            .expect("query");
    }

    wait_for_idle_sessions(&client, 1).await;
    let stats = client.session_pool_stats();
    assert_eq!(stats.in_use, 0);
    assert_eq!(stats.idle, 1);
}

#[tokio::test]
#[ignore] // need YDB access
async fn table_and_query_clients_share_driver_session_pool() {
    let client = create_client_with_session_pool(SessionPoolSettings::new().with_limit(2))
        .await
        .expect("client");

    let mut query_client = client.query_client();
    let table_client = client.table_client();

    let _row = query_client
        .query_row("SELECT 1 AS value")
        .await
        .expect("query");

    wait_for_idle_sessions(&client, 1).await;
    let stats = client.session_pool_stats();
    assert_eq!(stats.in_use, 0);
    assert_eq!(stats.idle, 1);

    let _table_session = table_client.create_session().await.expect("table session");
    let stats = client.session_pool_stats();
    assert_eq!(stats.in_use, 1);
    assert_eq!(stats.idle, 0);
}

#[tokio::test]
#[ignore] // need YDB access
async fn driver_session_pool_stats_reflect_active_and_idle() {
    let client = create_client_with_session_pool(SessionPoolSettings::new().with_limit(2))
        .await
        .expect("client");

    let mut query_client = client.query_client();
    let _row = query_client
        .query_row("SELECT 1 AS value")
        .await
        .expect("query");

    wait_for_idle_sessions(&client, 1).await;
    let stats = client.session_pool_stats();
    assert_eq!(stats.limit, 2);
    assert_eq!(stats.in_use, 0);
    assert_eq!(stats.idle, 1);
}

async fn create_client_with_short_pool_acquire_timeout(
    settings: SessionPoolSettings,
) -> Arc<Client> {
    let client = test_client_builder()
        .with_executor(Arc::new(crate::test_integration_helper::InplaceExecutor))
        .client()
        .expect("client builder")
        .with_timeouts(TimeoutSettings {
            operation_timeout: Duration::from_millis(300),
            ..TimeoutSettings::default()
        });
    client.wait().await.expect("discovery");
    Arc::new(
        client
            .with_session_pool(settings)
            .await
            .expect("session pool"),
    )
}

#[tokio::test]
#[ignore] // need YDB access
async fn driver_session_pool_acquire_times_out_when_exhausted() {
    let client = create_client_with_short_pool_acquire_timeout(
        SessionPoolSettings::new().with_limit(1),
    )
    .await;

    let _table_session = client
        .table_client()
        .create_session()
        .await
        .expect("hold pool slot");
    assert_eq!(client.session_pool_stats().in_use, 1);

    let err = client
        .query_client()
        .query_row("SELECT 1 AS value")
        .await
        .expect_err("second acquire should time out");
    let msg = err.to_string();
    assert!(
        msg.contains("acquire session from pool timed out"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
#[ignore] // need YDB access
async fn query_and_table_clients_share_pool_under_parallel_load() {
    let client = create_client_with_session_pool(SessionPoolSettings::new().with_limit(2))
        .await
        .expect("client");

    let mut handles = Vec::with_capacity(6);
    for i in 0..6 {
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            if i % 2 == 0 {
                client
                    .query_client()
                    .query_row("SELECT 1 AS value")
                    .await
                    .map(|_| ())
            } else {
                client
                    .table_client()
                    .create_session()
                    .await
                    .map(|_| ())
            }
        }));
    }
    for handle in handles {
        handle.await.expect("join").expect("workload");
    }

    wait_for_idle_sessions(&client, 2).await;
    let stats = client.session_pool_stats();
    assert_eq!(stats.limit, 2);
    assert_eq!(stats.in_use, 0);
    assert!(stats.idle <= 2);
}
