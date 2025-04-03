use std::time::Duration;

use tokio::task::JoinHandle;
use tracing_test::traced_test;

use crate::{
    client_coordination::list_types::{
        ConsistencyMode, NodeConfigBuilder, RateLimiterCountersMode,
    },
    test_integration_helper::create_client,
    AcquireOptionsBuilder, CoordinationSession, SessionOptionsBuilder, YdbResult,
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
        .acquire_semaphore_with_params(
            "my-resource",
            if ephemeral { u64::MAX } else { 1 },
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
        result = session.describe_semaphore("my-resource") => {
            let description = result.unwrap();
            assert_ne!(description.data, Vec::<u8>::from_iter([i]));
            assert_eq!(description.ephemeral, ephemeral);
            assert_eq!(description.owners.len(), 1);
            assert_eq!(description.owners[0].data, vec![i]);
            assert_eq!(description.owners[0].session_id, session.id());
        },
    }
    tokio::select! {
        _ = lease_alive.cancelled() => {
            unreachable!("lease should live");
        },
        _ = session.update_semaphore("my-resource", vec![i]) => {
        },
    }
    tokio::select! {
        _ = lease_alive.cancelled() => {
            unreachable!("lease should live");
        },
        result = session.describe_semaphore("my-resource") => {
            let description = result.unwrap();
            assert_eq!(description.data, Vec::<u8>::from_iter([i]));
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

    session.create_semaphore("my-resource", 1, vec![]).await?;

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
        result?;
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
        result?;
    }

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn describe_semaphore_test() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let node_name = "test_describe_semaphore".to_string();
    let node_path = format!("{}/{}", database_path, node_name);

    let mut coordination_client = client.coordination_client();

    let _ = coordination_client.drop_node(node_path.clone()).await;

    coordination_client
        .create_node(node_path.clone(), NodeConfigBuilder::default().build()?)
        .await?;

    let sessions: &mut Vec<CoordinationSession> = Box::leak(Box::default());
    for _ in 0..3 {
        sessions.push(
            coordination_client
                .create_session(node_path.clone(), SessionOptionsBuilder::default().build()?)
                .await?,
        );
    }

    let semaphore_name = "my-resource";
    sessions[0]
        .create_semaphore(semaphore_name, 2, vec![])
        .await?;

    let description = sessions[0].describe_semaphore(semaphore_name).await?;
    assert_eq!(description.data, Vec::<u8>::new());
    assert!(!description.ephemeral);
    assert_eq!(description.owners.len(), 0);
    assert_eq!(description.waiters.len(), 0);

    let lease_1 = sessions[0].acquire_semaphore(semaphore_name, 1).await?;
    let description = sessions[0].describe_semaphore(semaphore_name).await?;
    assert!(!description.ephemeral);
    assert_eq!(description.owners.len(), 1);
    assert_eq!(description.owners[0].data, Vec::<u8>::new());
    assert_eq!(description.owners[0].count, 1);
    assert_eq!(description.owners[0].session_id, sessions[0].id());
    assert_eq!(description.owners[0].timeout_millis, 0);
    assert_eq!(description.waiters.len(), 0);

    let lease_2 = sessions[1]
        .acquire_semaphore_with_params(
            semaphore_name,
            1,
            AcquireOptionsBuilder::default()
                .data(vec![2, 2, 2, 2, 2])
                .build()?,
        )
        .await?;
    let description = sessions[0].describe_semaphore(semaphore_name).await?;
    assert!(!description.ephemeral);
    assert_eq!(description.owners.len(), 2);
    assert_eq!(description.waiters.len(), 0);
    description.owners.iter().for_each(|owner| {
        assert_eq!(owner.count, 1);
        assert_eq!(owner.timeout_millis, 0);
        if owner.session_id == sessions[0].id() {
            assert_eq!(owner.data, Vec::<u8>::new());
        } else if owner.session_id == sessions[1].id() {
            assert_eq!(owner.data, vec![2, 2, 2, 2, 2]);
        } else {
            unreachable!("unknown owner");
        }
    });

    let lease_3_handle = tokio::spawn(
        sessions[2].acquire_semaphore_with_params(
            semaphore_name,
            1,
            AcquireOptionsBuilder::default()
                .data(vec![3, 3, 3, 3, 3])
                .build()?,
        ),
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    let description = sessions[0].describe_semaphore(semaphore_name).await?;
    assert!(!description.ephemeral);
    assert_eq!(description.owners.len(), 2);
    description.owners.iter().for_each(|owner| {
        assert_eq!(owner.count, 1);
        assert_eq!(owner.timeout_millis, 0);
        if owner.session_id == sessions[0].id() {
            assert_eq!(owner.data, Vec::<u8>::new());
        } else if owner.session_id == sessions[1].id() {
            assert_eq!(owner.data, vec![2, 2, 2, 2, 2]);
        } else {
            unreachable!("unknown owner");
        }
    });
    assert_eq!(description.waiters.len(), 1);
    assert_eq!(description.waiters[0].data, vec![3, 3, 3, 3, 3]);
    assert_eq!(description.waiters[0].count, 1);
    assert_eq!(description.waiters[0].session_id, sessions[2].id());
    assert_eq!(
        description.waiters[0].timeout_millis,
        Duration::from_secs(20).as_millis() as u64,
    );

    lease_2.release();
    let lease_3 = lease_3_handle.await??;
    let description = sessions[0].describe_semaphore(semaphore_name).await?;
    assert!(!description.ephemeral);
    assert_eq!(description.owners.len(), 2);
    assert_eq!(description.waiters.len(), 0);
    description.owners.iter().for_each(|owner| {
        assert_eq!(owner.count, 1);
        assert_eq!(owner.timeout_millis, 0);
        if owner.session_id == sessions[0].id() {
            assert_eq!(owner.data, Vec::<u8>::new());
        } else if owner.session_id == sessions[2].id() {
            assert_eq!(owner.data, vec![3, 3, 3, 3, 3]);
        } else {
            unreachable!("unknown owner");
        }
    });

    // ensure for lease 1 and 3 doesn't drop before test ends
    lease_1.release();
    lease_3.release();
    Ok(())
}
