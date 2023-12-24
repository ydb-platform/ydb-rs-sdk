use tokio::task::JoinHandle;
use tracing_test::traced_test;

use crate::{
    client_coordination::list_types::{
        ConsistencyMode, NodeConfigBuilder, RateLimiterCountersMode,
    },
    test_integration_helper::create_client,
    AcquireCount, AcquireOptionsBuilder, CoordinationSession, DescribeOptionsBuilder,
    SemaphoreLimit, SessionOptionsBuilder, YdbResult,
};

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn create_delete_node_test() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let node_name = "test_node".to_string();
    let node_path = format!("{}/{}", database_path, node_name);

    let mut coordination_client = client.coordination_client();

    let _ = coordination_client.drop_node(node_path.clone()).await; // ignore error

    coordination_client
        .create_node(node_path.clone(), NodeConfigBuilder::default().build()?)
        .await?;

    let node_desc = coordination_client.describe_node(node_path.clone()).await?;
    assert_eq!(node_desc.config.self_check_period_millis, 0);
    assert_eq!(node_desc.config.session_grace_period_millis, 0);
    assert!(node_desc.config.read_consistency_mode.is_none());
    assert!(node_desc.config.attach_consistency_mode.is_none());
    assert!(node_desc.config.rate_limiter_counters_mode.is_none());

    coordination_client
        .alter_node(
            node_path.clone(),
            NodeConfigBuilder::default()
                .self_check_period_millis(1)
                .session_grace_period_millis(2)
                .read_consistency_mode(Some(ConsistencyMode::Strict))
                .rate_limiter_counters_mode(Some(RateLimiterCountersMode::Aggregated))
                .build()?,
        )
        .await?;

    let node_desc = coordination_client.describe_node(node_path.clone()).await?;
    assert_eq!(node_desc.config.self_check_period_millis, 1);
    assert_eq!(node_desc.config.session_grace_period_millis, 2);
    assert!(matches!(
        node_desc.config.read_consistency_mode,
        Some(ConsistencyMode::Strict)
    ));
    assert!(node_desc.config.attach_consistency_mode.is_none());
    assert!(matches!(
        node_desc.config.rate_limiter_counters_mode,
        Some(RateLimiterCountersMode::Aggregated)
    ));

    coordination_client.drop_node(node_path.clone()).await?;

    Ok(())
}

async fn mutex_work(i: u8, session: CoordinationSession, ephemeral: bool) {
    let lease = session
        .acquire_semaphore(
            "my-resource".to_string(),
            if ephemeral {
                AcquireCount::Exclusive
            } else {
                AcquireCount::Single
            },
            AcquireOptionsBuilder::default()
                .data(vec![i])
                .ephemeral(ephemeral)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

    let lease_alive = lease.alive();
    tokio::select! {
        _ = lease_alive.cancelled() => {
            unreachable!("lease should live");
        },
        result = session.describe_semaphore(
            "my-resource".to_string(),
            DescribeOptionsBuilder::default()
                .with_owners(true)
                .with_waiters(true)
                .build()
                .unwrap()
        ) => {
            let description = result.unwrap();
            assert_eq!(description.ephemeral, ephemeral);
            assert_eq!(description.owners.len(), 1);
            assert_eq!(description.owners[0].data, vec![i]);
            assert_eq!(description.owners[0].session_id, session.id());
        },
    }
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn mutex_test() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let node_name = "test_mutex".to_string();
    let node_path = format!("{}/{}", database_path, node_name);

    let mut coordination_client = client.coordination_client();

    let _ = coordination_client.drop_node(node_path.clone()).await;

    coordination_client
        .create_node(node_path.clone(), NodeConfigBuilder::default().build()?)
        .await?;

    let session = coordination_client
        .create_session(node_path.clone(), SessionOptionsBuilder::default().build()?)
        .await?;

    session
        .create_semaphore("my-resource".to_string(), SemaphoreLimit::Mutex, None)
        .await?;

    let mut handles: Vec<JoinHandle<()>> = vec![];
    for i in 0u8..10 {
        let mut client = client.coordination_client();
        let node_path = node_path.clone();
        handles.push(tokio::spawn(async move {
            let session = client
                .create_session(node_path, SessionOptionsBuilder::default().build().unwrap())
                .await
                .unwrap();

            let session_alive_token = session.alive();
            tokio::select! {
                _ = session_alive_token.cancelled() => {
                    unreachable!("session should live");
                },
                _ = mutex_work(i, session, false) => {},
            }
        }));
    }

    for result in futures_util::future::join_all(handles).await {
        result.unwrap();
    }

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn ephemeral_mutex_test() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let node_name = "test_ephemeral_mutex".to_string();
    let node_path = format!("{}/{}", database_path, node_name);

    let mut coordination_client = client.coordination_client();

    let _ = coordination_client.drop_node(node_path.clone()).await;

    coordination_client
        .create_node(node_path.clone(), NodeConfigBuilder::default().build()?)
        .await?;

    let mut handles: Vec<JoinHandle<()>> = vec![];
    for i in 0u8..10 {
        let mut client = client.coordination_client();
        let node_path = node_path.clone();
        handles.push(tokio::spawn(async move {
            let session = client
                .create_session(node_path, SessionOptionsBuilder::default().build().unwrap())
                .await
                .unwrap();

            let session_alive_token = session.alive();
            tokio::select! {
                _ = session_alive_token.cancelled() => {
                    unreachable!("session should live");
                },
                _ = mutex_work(i, session, true) => {},
            }
        }));
    }

    for result in futures_util::future::join_all(handles).await {
        result.unwrap();
    }

    Ok(())
}
