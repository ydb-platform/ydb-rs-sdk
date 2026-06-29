//! Regression tests for session-pool corner cases found during PR #501 / native-table SLO work.

use super::query_pool::{QuerySessionPool, QuerySessionPoolSettings};
use crate::errors::YdbError;

#[tokio::test]
async fn warm_up_partial_keeps_successful_sessions() {
    let pool = QuerySessionPool::new_explicit_bench_with_create_failures(
        QuerySessionPoolSettings::new().with_limit(10),
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
    let pool = QuerySessionPool::new_explicit_bench_with_create_failures(
        QuerySessionPoolSettings::new().with_limit(10),
        3,
    );
    let err = pool.warm_up_for_tests(3).await.expect_err("all warm-up tasks failed");
    assert!(
        err.to_string().contains("bench injected create session failure"),
        "unexpected error: {err}"
    );
    assert_eq!(pool.stats().idle, 0);
}

#[tokio::test]
async fn acquire_reuses_idle_session() {
    let pool = QuerySessionPool::new_explicit_bench(
        QuerySessionPoolSettings::new()
            .with_limit(2)
            .with_warm_up(1),
    );
    let first = pool.acquire_explicit().await.expect("first acquire");
    let session_id = first.session_id().to_string();
    first.return_to_pool().await;

    let second = pool.acquire_explicit().await.expect("second acquire");
    assert_eq!(second.session_id(), session_id);
    second.return_to_pool().await;
}

#[tokio::test]
async fn acquire_skips_invalidated_idle_session() {
    let pool = QuerySessionPool::new_explicit_bench(
        QuerySessionPoolSettings::new()
            .with_limit(2)
            .with_warm_up(0),
    );
    let created_before = pool.stats().sessions_created;

    let mut lease = pool.acquire_explicit().await.expect("first acquire");
    let first_id = lease.session_id().to_string();
    lease.bench_invalidate_session();
    lease.return_to_pool().await;

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
    second.return_to_pool().await;
}

#[tokio::test]
async fn bad_session_marks_table_session_non_poolable() {
    use crate::client::TimeoutSettings;
    use crate::grpc_connection_manager::GrpcConnectionManager;
    use crate::grpc_wrapper::grpc_limits::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES;
    use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
    use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
    use crate::session_pool::SessionPool;
    use http::Uri;
    use ydb_grpc::ydb_proto::status_ids::StatusCode;

    let pool = SessionPool::from_shared(
        QuerySessionPool::new_explicit_bench(
            QuerySessionPoolSettings::new()
                .with_limit(2)
                .with_warm_up(1),
        ),
        GrpcConnectionManager::new(
            SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(
                Uri::from_static("http://127.0.0.1/bench"),
            ))),
            "bench".to_string(),
            MultiInterceptor::new(),
            None,
            DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES,
        ),
        TimeoutSettings::default(),
    );

    let mut session = pool.session().await.expect("lease table session");
    assert!(session.can_pooled);
    session.handle_error(&YdbError::YdbStatusError(crate::errors::YdbStatusError {
        message: "bad".into(),
        operation_status: StatusCode::BadSession as i32,
        issues: vec![],
    }));
    assert!(!session.can_pooled);
}
