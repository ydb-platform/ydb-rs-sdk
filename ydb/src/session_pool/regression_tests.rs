//! Regression tests for session-pool corner cases found during PR #501 / native-table SLO work.

use super::pool::{SessionPool, SessionPoolSettings};

#[tokio::test]
async fn warm_up_partial_keeps_successful_sessions() {
    let pool = SessionPool::new_explicit_bench_with_create_failures(
        SessionPoolSettings::new().with_limit(10),
        2,
    );
    pool.warm_up_for_tests(5)
        .await
        .expect("partial warm-up should succeed");
    let stats = pool.stats();
    assert_eq!(stats.idle, 3, "3 of 5 warm-up tasks should succeed");
    assert_eq!(stats.sessions_created, 3);
}

#[tokio::test]
async fn warm_up_fails_when_every_create_fails() {
    let pool = SessionPool::new_explicit_bench_with_create_failures(
        SessionPoolSettings::new().with_limit(10),
        3,
    );
    let err = pool
        .warm_up_for_tests(3)
        .await
        .expect_err("all warm-up tasks failed");
    assert!(
        err.to_string()
            .contains("bench injected create session failure"),
        "unexpected error: {err}"
    );
    assert_eq!(pool.stats().idle, 0);
}

#[tokio::test]
async fn acquire_reuses_idle_session() {
    let pool =
        SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(2).with_warm_up(1));
    let first = pool.acquire_explicit().await.expect("first acquire");
    let session_id = first.session_id().to_string();
    first.return_to_pool();

    let second = pool.acquire_explicit().await.expect("second acquire");
    assert_eq!(second.session_id(), session_id);
    second.return_to_pool();
}

#[tokio::test]
async fn dropped_lease_is_not_reused() {
    let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
    let first = pool.acquire_explicit().await.expect("first acquire");
    let first_id = first.session_id().to_string();

    drop(first);

    let second = pool.acquire_explicit().await.expect("second acquire");
    assert_ne!(second.session_id(), first_id);
    second.return_to_pool();
}

#[tokio::test]
async fn acquire_skips_invalidated_idle_session() {
    let pool =
        SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(2).with_warm_up(0));
    let created_before = pool.stats().sessions_created;

    let mut lease = pool.acquire_explicit().await.expect("first acquire");
    let first_id = lease.session_id().to_string();
    lease.bench_invalidate_session();
    lease.return_to_pool();

    let second = pool.acquire_explicit().await.expect("second acquire");
    assert_ne!(
        second.session_id(),
        first_id,
        "invalidated session must not be leased again"
    );
    assert!(
        pool.stats().sessions_created > created_before,
        "pool should create a replacement session"
    );
    second.return_to_pool();
}

#[tokio::test]
async fn item_usage_limit_closes_session_on_return() {
    let pool = SessionPool::new_explicit_bench(
        SessionPoolSettings::new()
            .with_limit(2)
            .with_item_usage_limit(1),
    );
    let created_before = pool.stats().sessions_created;

    let lease = pool.acquire_explicit().await.expect("first acquire");
    let first_id = lease.session_id().to_string();
    lease.return_to_pool();

    assert_eq!(pool.stats().idle, 0, "session must be closed after one use");

    let lease = pool.acquire_explicit().await.expect("second acquire");
    assert_ne!(lease.session_id(), first_id);
    assert!(
        pool.stats().sessions_created > created_before,
        "pool should create a replacement session"
    );
    lease.return_to_pool();
}

#[tokio::test]
async fn warm_up_overflow_respects_pool_limit() {
    let pool =
        SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(2).with_warm_up(5));
    pool.warm_up_for_tests(5)
        .await
        .expect("warm-up should succeed");
    let stats = pool.stats();
    assert_eq!(stats.idle, 2, "idle stack must not exceed pool limit");
    assert_eq!(stats.sessions_created, 5);
}
