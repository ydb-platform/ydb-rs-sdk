use std::time::Duration;

use crate::session_pool::QuerySessionPoolSettings;
use crate::test_integration_helper::create_client_with_session_pool;

#[tokio::test]
#[ignore] // need YDB access
async fn query_client_reuses_driver_session_pool() {
    let client = create_client_with_session_pool(QuerySessionPoolSettings::new().with_limit(2))
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

    tokio::time::sleep(Duration::from_millis(50)).await;
    let stats = client.session_pool_stats();
    assert_eq!(stats.in_use, 0);
    assert_eq!(stats.idle, 1);
}

#[tokio::test]
#[ignore] // need YDB access
async fn query_client_returns_session_to_driver_pool_after_stream_drop() {
    let client = create_client_with_session_pool(QuerySessionPoolSettings::new().with_limit(2))
        .await
        .expect("client");

    let mut query_client = client.query_client();
    {
        let _row = query_client
            .query_row("SELECT 1 AS value")
            .await
            .expect("query");
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    let stats = client.session_pool_stats();
    assert_eq!(stats.in_use, 0);
    assert_eq!(stats.idle, 1);
}

#[tokio::test]
#[ignore] // need YDB access
async fn table_and_query_clients_share_driver_session_pool() {
    let client = create_client_with_session_pool(QuerySessionPoolSettings::new().with_limit(2))
        .await
        .expect("client");

    let mut query_client = client.query_client();
    let table_client = client.table_client();

    let _row = query_client
        .query_row("SELECT 1 AS value")
        .await
        .expect("query");

    tokio::time::sleep(Duration::from_millis(50)).await;
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
    let client = create_client_with_session_pool(QuerySessionPoolSettings::new().with_limit(2))
        .await
        .expect("client");

    let mut query_client = client.query_client();
    let _row = query_client
        .query_row("SELECT 1 AS value")
        .await
        .expect("query");

    tokio::time::sleep(Duration::from_millis(50)).await;
    let stats = client.session_pool_stats();
    assert_eq!(stats.limit, 2);
    assert_eq!(stats.in_use, 0);
    assert_eq!(stats.idle, 1);
}
