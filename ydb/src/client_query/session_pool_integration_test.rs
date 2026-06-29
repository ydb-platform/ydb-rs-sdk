use std::time::{Duration, Instant};

use crate::session_pool::SessionPoolSettings;
use crate::test_integration_helper::create_client_with_session_pool;
use crate::Client;

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
