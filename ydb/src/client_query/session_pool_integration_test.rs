//! Session pool timeouts and one-shot session routing (issues #333 / #332).

use super::QuerySessionPoolSettings;
use crate::errors::YdbResult;
use crate::test_integration_helper::create_client;
use crate::QueryClient;
use std::time::Duration;
use tracing_test::traced_test;

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_one_shot_without_pool_uses_implicit_sessions() -> YdbResult<()> {
    let client = create_client().await?;
    let qc = client.query_client();

    assert!(qc.session_pool_stats().is_none());
    assert!(qc.implicit_session_pool_stats().is_none());

    let mut qc = qc.clone_with_idempotent_operations(true);
    let mut row = qc.query_row("SELECT 1 AS one").await?;
    let one: i64 = row.remove_field_by_name("one")?.try_into()?;
    assert_eq!(one, 1);
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_one_shot_with_implicit_session_pool() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client
        .query_client()
        .clone_with_idempotent_operations(true)
        .with_implicit_session_pool(
            QuerySessionPoolSettings::new()
                .with_limit(2)
                .with_warm_up(1),
        );

    assert!(qc.session_pool_stats().is_none());
    let stats = qc
        .implicit_session_pool_stats()
        .expect("implicit session pool configured");
    assert_eq!(stats.limit, 2);
    assert!(stats.idle >= 1);

    let mut row = qc.query_row("SELECT 2 AS two").await?;
    let two: i64 = row.remove_field_by_name("two")?.try_into()?;
    assert_eq!(two, 2);
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_one_shot_with_explicit_session_pool() -> YdbResult<()> {
    let client = create_client().await?;
    let mut qc = client
        .query_client()
        .clone_with_idempotent_operations(true)
        .with_session_pool(
            QuerySessionPoolSettings::new()
                .with_limit(2)
                .with_warm_up(1),
        )
        .await?;

    assert!(qc.implicit_session_pool_stats().is_none());
    let stats = qc
        .session_pool_stats()
        .expect("explicit session pool configured");
    assert_eq!(stats.limit, 2);

    let mut row = qc.query_row("SELECT 3 AS three").await?;
    let three: i64 = row.remove_field_by_name("three")?.try_into()?;
    assert_eq!(three, 3);
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_call_timeout_preempts_pool_session_acquire() {
    let client = create_client().await.unwrap();
    let mut qc = client
        .query_client()
        .clone_with_idempotent_operations(true)
        .with_session_pool(
            QuerySessionPoolSettings::new()
                .with_limit(1)
                .with_warm_up(0)
                .with_session_create_timeout(Duration::from_secs(30)),
        )
        .await
        .unwrap();

    // Hold the only pool slot until this stream is dropped.
    let mut qc_holder = qc.clone();
    let _holding = qc_holder.query("SELECT 1 AS one").await.unwrap();

    let err = qc
        .query_row("SELECT 2 AS two")
        .timeout(Duration::from_millis(100))
        .await
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("operation timed out"),
        "expected per-call deadline while waiting for pool slot, got: {msg}"
    );
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_retry_transaction_call_timeout_preempts_pool_acquire() {
    let client = create_client().await.unwrap();
    let qc = client
        .query_client()
        .clone_with_idempotent_operations(true)
        .with_session_pool(
            QuerySessionPoolSettings::new()
                .with_limit(1)
                .with_warm_up(0)
                .with_session_create_timeout(Duration::from_secs(30)),
        )
        .await
        .unwrap();

    let mut qc_holder = qc.clone();
    let _holding = qc_holder.query("SELECT 1 AS one").await.unwrap();

    let err = qc
        .retry_transaction(async |tx| {
            tx.query_row("SELECT 2 AS two")
                .timeout(Duration::from_millis(100))
                .await?;
            Ok(())
        })
        .await
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("operation timed out"),
        "expected per-call deadline in retry_transaction while pool is saturated, got: {err}"
    );
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_retry_transaction_uses_pool_session_rpc_timeouts() -> YdbResult<()> {
    let client = create_client().await?;
    let qc = client
        .query_client()
        .clone_with_idempotent_operations(true)
        .with_implicit_session_pool(
            QuerySessionPoolSettings::new()
                .with_session_create_timeout(Duration::from_secs(30))
                .with_session_delete_timeout(Duration::from_secs(30)),
        );

    let value: i64 = qc
        .retry_transaction(async |tx| {
            let mut row = tx.query_row("SELECT 7 AS v").await?;
            Ok(row.remove_field_by_name("v")?.try_into()?)
        })
        .await?;

    assert_eq!(value, 7);
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_session_pool_respects_custom_create_timeout() {
    let client = create_client().await.unwrap();
    let mut qc = client
        .query_client()
        .clone_with_idempotent_operations(true)
        .with_session_pool(
            QuerySessionPoolSettings::new()
                .with_limit(1)
                .with_warm_up(0)
                .with_session_create_timeout(Duration::from_millis(1)),
        )
        .await
        .unwrap();

    match qc.query_row("SELECT 1 AS one").await {
        Ok(_) => {
            // Local YDB may finish CreateSession+Attach faster than 1ms; skip strict assert.
        }
        Err(err) => {
            let msg = err.to_string();
            assert!(
                msg.contains("create query session timed out after 1ms")
                    || msg.contains("attach query session timed out after 1ms"),
                "expected pool create timeout from settings, got: {msg}"
            );
        }
    }
}

const SESSION_POOL_REUSE_PARALLELISM: usize = 100;

async fn wait_explicit_pool_idle(qc: &QueryClient, expected_idle: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let stats = qc
            .session_pool_stats()
            .expect("explicit session pool configured");
        if stats.idle >= expected_idle && stats.create_in_progress == 0 {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "session pool did not return {expected_idle} idle sessions, last stats: {stats:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn run_parallel_query_rows(qc: &QueryClient, count: usize) -> YdbResult<()> {
    let mut handles = Vec::with_capacity(count);
    for _ in 0..count {
        let mut qc = qc.clone();
        handles.push(tokio::spawn(async move {
            let mut row = qc.query_row("SELECT 1 AS one").await?;
            let one: i64 = row.remove_field_by_name("one")?.try_into()?;
            YdbResult::Ok(one)
        }));
    }
    for handle in handles {
        let one = handle.await??;
        assert_eq!(one, 1);
    }
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn query_session_pool_reuses_sessions_under_parallel_load() -> YdbResult<()> {
    let client = create_client().await?;
    let qc = client
        .query_client()
        .clone_with_idempotent_operations(true)
        .with_session_pool(
            QuerySessionPoolSettings::new()
                .with_limit(SESSION_POOL_REUSE_PARALLELISM)
                .with_warm_up(0),
        )
        .await?;

    let initial = qc
        .session_pool_stats()
        .expect("explicit session pool configured");
    assert_eq!(initial.sessions_created, 0);
    assert_eq!(initial.idle, 0);

    run_parallel_query_rows(&qc, SESSION_POOL_REUSE_PARALLELISM).await?;
    wait_explicit_pool_idle(&qc, SESSION_POOL_REUSE_PARALLELISM).await;

    let after_first_wave = qc.session_pool_stats().expect("explicit session pool configured");
    assert_eq!(
        after_first_wave.sessions_created,
        SESSION_POOL_REUSE_PARALLELISM as u64,
        "first wave should create one session per concurrent query, stats: {after_first_wave:?}"
    );
    assert!(
        after_first_wave.idle >= SESSION_POOL_REUSE_PARALLELISM,
        "sessions should return to idle stack, stats: {after_first_wave:?}"
    );

    run_parallel_query_rows(&qc, SESSION_POOL_REUSE_PARALLELISM).await?;
    wait_explicit_pool_idle(&qc, SESSION_POOL_REUSE_PARALLELISM).await;

    let after_second_wave = qc.session_pool_stats().expect("explicit session pool configured");
    assert_eq!(
        after_second_wave.sessions_created,
        SESSION_POOL_REUSE_PARALLELISM as u64,
        "second wave must reuse idle sessions without CreateSession, stats: {after_second_wave:?}"
    );
    assert!(
        after_second_wave.idle >= SESSION_POOL_REUSE_PARALLELISM,
        "sessions should return to idle stack after second wave, stats: {after_second_wave:?}"
    );

    Ok(())
}
