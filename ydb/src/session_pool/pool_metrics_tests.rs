//! Tests for the synchronous session pool metrics (issue #267, section 5).
//!
//! Gauges are re-emitted from pool counters at every mutation site. The property
//! test drives a randomized acquire/return/drop sequence and checks the recorded
//! values against a ledger derived from the operations themselves — an oracle
//! independent of the pool's internal accounting (gauges are emitted from
//! `SessionPool::stats()`, so comparing them against `stats()` alone would be
//! circular). The `stats()` comparison is kept as a secondary invariant: it catches
//! mutation sites that forgot to re-emit the gauges at all.

use std::time::Duration;

use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;

use crate::client_metrics::test_recorder::TestRecorder;
use crate::session_pool::{SessionPool, SessionPoolSettings};
use std::sync::Arc;

fn bench_pool(settings: SessionPoolSettings) -> (SessionPool, Arc<TestRecorder>) {
    let recorder = Arc::new(TestRecorder::new());
    let pool = SessionPool::new_explicit_bench_with_metrics(settings, recorder.clone());
    (pool, recorder)
}

fn gauge(recorder: &TestRecorder, name: &str, state: &str) -> f64 {
    recorder.gauge_value_with_label(name, "state", state)
}

/// Expected metric state computed from the test's own operations, independent of
/// the pool's internal counters.
#[derive(Default)]
struct Ledger {
    idle: f64,
    active: f64,
    closed_pool_full: u64,
    closed_bad: u64,
}

/// Assert the recorded metrics match the operation ledger.
fn assert_matches_ledger(recorder: &TestRecorder, ledger: &Ledger, limit: usize) {
    assert_eq!(
        gauge(recorder, "ydb_session_pool_sessions", "idle"),
        ledger.idle,
        "idle gauge must match the operation ledger"
    );
    assert_eq!(
        gauge(recorder, "ydb_session_pool_sessions", "active"),
        ledger.active,
        "active gauge must match the operation ledger"
    );
    assert_eq!(
        gauge(recorder, "ydb_session_pool_sessions", "creating"),
        0.0,
        "creating gauge must be zero between operations"
    );
    assert_eq!(
        recorder.gauge_value("ydb_session_pool_pending_requests"),
        0.0,
        "pending gauge must be zero: the ledger allows no concurrent waiters"
    );
    assert_eq!(
        recorder.gauge_value("ydb_session_pool_size_limit"),
        limit as f64,
        "size limit gauge must match the configured limit"
    );
    assert_eq!(
        recorder.counter_value_with_label(
            "ydb_session_pool_sessions_closed_total",
            "reason",
            "pool_full"
        ),
        ledger.closed_pool_full,
        "pool_full closes must match the operation ledger"
    );
    assert_eq!(
        recorder.counter_value_with_label(
            "ydb_session_pool_sessions_closed_total",
            "reason",
            "bad_session"
        ),
        ledger.closed_bad,
        "bad_session closes must match the operation ledger"
    );
    assert!(
        recorder.counter_value("ydb_session_pool_sessions_created_total")
            >= recorder.counter_value("ydb_session_pool_sessions_closed_total"),
        "closed sessions must never exceed created sessions"
    );
}

/// Assert the recorded gauges exactly match the pool's own stats snapshot.
fn assert_gauges_match_stats(pool: &SessionPool, recorder: &TestRecorder) {
    let stats = pool.stats();
    assert_eq!(
        gauge(recorder, "ydb_session_pool_sessions", "idle"),
        stats.idle as f64,
        "idle gauge must match stats().idle"
    );
    assert_eq!(
        gauge(recorder, "ydb_session_pool_sessions", "active"),
        stats.in_use as f64,
        "active gauge must match stats().in_use"
    );
    assert_eq!(
        gauge(recorder, "ydb_session_pool_sessions", "creating"),
        stats.create_in_progress as f64,
        "creating gauge must match stats().create_in_progress"
    );
    assert_eq!(
        recorder.gauge_value("ydb_session_pool_size_limit"),
        stats.limit as f64,
        "size limit gauge must match the configured limit"
    );
}

#[test]
fn pool_creation_emits_limit_gauge() {
    let (pool, recorder) = bench_pool(SessionPoolSettings::new().with_limit(7));
    assert_eq!(recorder.gauge_value("ydb_session_pool_size_limit"), 7.0);
    assert_gauges_match_stats(&pool, &recorder);
}

#[tokio::test]
async fn acquire_return_and_drop_keep_gauges_in_sync() {
    let limit = 4;
    let (pool, recorder) = bench_pool(SessionPoolSettings::new().with_limit(limit));

    let mut held = Vec::new();
    let mut ledger = Ledger::default();
    let mut acquire_calls = 0u64;
    let mut rng = StdRng::seed_from_u64(42);
    for _step in 0..200 {
        match rng.gen_range(0..3) {
            0 | 1 if held.len() < limit => {
                let lease = pool
                    .acquire_explicit()
                    .await
                    .expect("bench pool acquire must succeed");
                held.push(lease);
                ledger.active += 1.0;
                // Reuse moves a session from idle instead of creating one.
                if ledger.idle > 0.0 {
                    ledger.idle -= 1.0;
                }
                acquire_calls += 1;
            }
            _ if !held.is_empty() => {
                let index = rng.gen_range(0..held.len());
                let lease = held.swap_remove(index);
                match rng.gen_range(0..2) {
                    // Normal return: idle if there is room, closed as overflow otherwise.
                    0 => {
                        lease.return_to_pool();
                        ledger.active -= 1.0;
                        if ledger.idle < limit as f64 {
                            ledger.idle += 1.0;
                        } else {
                            ledger.closed_pool_full += 1;
                        }
                    }
                    // Abnormal drop: session is discarded and counted as bad.
                    _ => {
                        drop(lease);
                        ledger.active -= 1.0;
                        ledger.closed_bad += 1;
                    }
                }
            }
            _ => {}
        }
        // The ledger is the independent oracle; the stats() comparison additionally
        // catches mutation sites that forgot to re-emit the gauges.
        assert_matches_ledger(&recorder, &ledger, limit);
        assert_gauges_match_stats(&pool, &recorder);
    }

    // Drain all held leases so the pool is quiescent, then verify once more.
    for lease in held {
        lease.return_to_pool();
        ledger.active -= 1.0;
        if ledger.idle < limit as f64 {
            ledger.idle += 1.0;
        } else {
            ledger.closed_pool_full += 1;
        }
    }
    assert_matches_ledger(&recorder, &ledger, limit);
    assert_gauges_match_stats(&pool, &recorder);
    assert_eq!(
        recorder.gauge_value("ydb_session_pool_pending_requests"),
        0.0,
        "no acquirer may stay pending after the sequence"
    );

    // Every acquire attempt in the sequence was successful.
    assert_eq!(
        recorder.counter_value_with_label("ydb_session_pool_acquire_total", "result", "ok"),
        acquire_calls,
        "acquire counter must cover all attempts"
    );

    drop(pool);
    assert_eq!(recorder.gauge_value("ydb_session_pool_sessions"), 0.0);
    assert_eq!(recorder.gauge_value("ydb_session_pool_size_limit"), 0.0);
    assert_eq!(
        recorder.counter_value("ydb_session_pool_sessions_created_total"),
        recorder.counter_value("ydb_session_pool_sessions_closed_total"),
        "pool teardown must leave the independent created/closed ledger balanced"
    );
}

#[tokio::test]
async fn acquire_timeout_counts_timeout_and_clears_pending() {
    let (pool, recorder) = bench_pool(
        SessionPoolSettings::new()
            .with_limit(1)
            .with_acquire_timeout(Duration::from_millis(50)),
    );

    let lease = pool
        .acquire_explicit()
        .await
        .expect("first acquire on empty pool must succeed");
    assert_eq!(
        recorder.gauge_value("ydb_session_pool_pending_requests"),
        0.0,
        "immediate acquire must not stay pending"
    );
    assert!(
        recorder
            .gauge_observations("ydb_session_pool_pending_requests")
            .iter()
            .all(|value| *value == 0.0),
        "a fast-path acquire must never increment pending"
    );

    let result = pool.acquire_explicit().await;
    assert!(result.is_err(), "acquire at capacity must time out");

    assert_eq!(
        recorder.counter_value_with_label("ydb_session_pool_acquire_total", "result", "timeout"),
        1,
        "the timed-out acquire must be counted as timeout"
    );
    assert_eq!(
        recorder.gauge_value("ydb_session_pool_pending_requests"),
        0.0,
        "pending gauge must return to zero after the timeout"
    );

    let _ = lease;
}

#[tokio::test]
async fn pending_gauge_tracks_waiters_while_blocked() {
    let (pool, recorder) = bench_pool(
        SessionPoolSettings::new()
            .with_limit(1)
            .with_acquire_timeout(Duration::from_secs(10)),
    );

    let lease = pool
        .acquire_explicit()
        .await
        .expect("first acquire on empty pool must succeed");

    let waiter_pool = pool.clone();
    let waiter = tokio::spawn(async move { waiter_pool.acquire_explicit().await });

    // Let the waiter reach the semaphore wait.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        recorder.gauge_value("ydb_session_pool_pending_requests"),
        1.0,
        "a blocked acquirer must be visible in the pending gauge"
    );

    // Returning the first lease unblocks the waiter.
    lease.return_to_pool();
    let waited = waiter
        .await
        .expect("waiter task must not panic")
        .expect("waiter must acquire after release");
    assert_eq!(
        recorder.gauge_value("ydb_session_pool_pending_requests"),
        0.0,
        "pending gauge must return to zero after unblocking"
    );
    waited.return_to_pool();
    assert_gauges_match_stats(&pool, &recorder);
}

#[tokio::test]
async fn cancelled_acquire_records_cancelled_and_clears_pending() {
    let (pool, recorder) = bench_pool(
        SessionPoolSettings::new()
            .with_limit(1)
            .with_acquire_timeout(Duration::from_secs(10)),
    );
    let _lease = pool.acquire_explicit().await.expect("first acquire");
    let waiter = tokio::spawn({
        let pool = pool.clone();
        async move { pool.acquire_explicit().await }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        recorder.gauge_value("ydb_session_pool_pending_requests"),
        1.0
    );
    waiter.abort();
    let error = waiter.await.err().expect("waiter task must be cancelled");
    assert!(error.is_cancelled());

    assert_eq!(
        recorder.gauge_value("ydb_session_pool_pending_requests"),
        0.0
    );
    assert_eq!(
        recorder.counter_value_with_label("ydb_session_pool_acquire_total", "result", "cancelled"),
        1,
        "dropping the acquire future must record one cancelled outcome"
    );
    assert_eq!(
        recorder.counter_value("ydb_session_pool_acquire_total"),
        2,
        "successful and cancelled acquires must each be recorded exactly once"
    );
}

#[tokio::test]
async fn create_failure_releases_permit_before_gauge_update() {
    let recorder = Arc::new(TestRecorder::new());
    let pool = SessionPool::new_explicit_bench_with_metrics_and_create_failures(
        SessionPoolSettings::new().with_limit(1),
        recorder.clone(),
        1,
    );

    let result = pool.acquire_explicit().await;
    assert!(result.is_err(), "injected create failure must surface");
    assert_eq!(gauge(&recorder, "ydb_session_pool_sessions", "active"), 0.0);
    assert_eq!(
        gauge(&recorder, "ydb_session_pool_sessions", "creating"),
        0.0
    );
    assert_eq!(
        recorder.counter_value_with_label("ydb_session_pool_acquire_total", "result", "error"),
        1
    );
}

#[tokio::test]
async fn session_creates_are_counted_and_measured() {
    let (pool, recorder) = bench_pool(SessionPoolSettings::new().with_limit(2));

    let first = pool.acquire_explicit().await.expect("create first session");
    let second = pool
        .acquire_explicit()
        .await
        .expect("create second session");

    assert_eq!(
        recorder.counter_value("ydb_session_pool_sessions_created_total"),
        2,
        "each created session must be counted"
    );
    let observations = recorder.histogram_observations("ydb_session_pool_session_create_seconds");
    assert_eq!(
        observations.len(),
        2,
        "each successful create must record a duration"
    );

    drop(first);
    drop(second);
}

#[tokio::test]
async fn close_reasons_are_recorded() {
    // usage_limit: session closed on return after reaching the use limit.
    let (pool, recorder) = bench_pool(
        SessionPoolSettings::new()
            .with_limit(1)
            .with_item_usage_limit(1),
    );
    let lease = pool.acquire_explicit().await.expect("acquire");
    lease.return_to_pool();
    assert_eq!(
        recorder.counter_value_with_label(
            "ydb_session_pool_sessions_closed_total",
            "reason",
            "usage_limit"
        ),
        1,
        "session closed at the usage limit must be counted"
    );

    // max_age: session lifetime expiry is distinct from the use-count limit.
    let (pool, recorder) = bench_pool(
        SessionPoolSettings::new()
            .with_limit(1)
            .with_item_usage_ttl(Duration::from_millis(30)),
    );
    let lease = pool.acquire_explicit().await.expect("acquire");
    tokio::time::sleep(Duration::from_millis(60)).await;
    lease.return_to_pool();
    assert_eq!(
        recorder.counter_value_with_label(
            "ydb_session_pool_sessions_closed_total",
            "reason",
            "max_age"
        ),
        1,
        "session closed at its lifetime limit must be counted as max_age"
    );
    assert_eq!(
        recorder.counter_value_with_label(
            "ydb_session_pool_sessions_closed_total",
            "reason",
            "usage_limit"
        ),
        0
    );

    // bad_session: lease dropped without returning.
    let (pool, recorder) = bench_pool(SessionPoolSettings::new().with_limit(1));
    let lease = pool.acquire_explicit().await.expect("acquire");
    drop(lease);
    assert_eq!(
        recorder.counter_value_with_label(
            "ydb_session_pool_sessions_closed_total",
            "reason",
            "bad_session"
        ),
        1,
        "abnormally dropped lease must count as bad session"
    );
    assert_gauges_match_stats(&pool, &recorder);

    // idle_ttl: idle session closed on the next acquire attempt.
    let (pool, recorder) = bench_pool(
        SessionPoolSettings::new()
            .with_limit(1)
            .with_idle_ttl(Duration::from_millis(30)),
    );
    let lease = pool.acquire_explicit().await.expect("acquire");
    lease.return_to_pool();
    tokio::time::sleep(Duration::from_millis(60)).await;
    let _replacement = pool.acquire_explicit().await.expect("replacement session");
    assert_eq!(
        recorder.counter_value_with_label(
            "ydb_session_pool_sessions_closed_total",
            "reason",
            "idle_ttl"
        ),
        1,
        "expired idle session must be counted as idle_ttl close"
    );

    // shutdown: idle sessions drained for a node.
    let (pool, recorder) = bench_pool(SessionPoolSettings::new().with_limit(2));
    let lease = pool.acquire_explicit().await.expect("acquire");
    lease.return_to_pool();
    pool.observer_for_metrics_tests()
        .node_shutdown(&http::Uri::from_static("http://127.0.0.1/bench"));
    assert_eq!(
        recorder.counter_value_with_label(
            "ydb_session_pool_sessions_closed_total",
            "reason",
            "shutdown"
        ),
        1,
        "drained idle sessions must be counted as shutdown close"
    );
    assert_gauges_match_stats(&pool, &recorder);
}

#[tokio::test]
async fn abnormal_drop_preserves_keepalive_failure_reason() {
    let (pool, recorder) = bench_pool(SessionPoolSettings::new().with_limit(1));
    let mut lease = pool.acquire_explicit().await.expect("acquire");
    lease.invalidate_keepalive();
    drop(lease);

    assert_eq!(
        recorder.counter_value_with_label(
            "ydb_session_pool_sessions_closed_total",
            "reason",
            "keepalive_failed"
        ),
        1
    );
    assert_eq!(
        recorder.counter_value_with_label(
            "ydb_session_pool_sessions_closed_total",
            "reason",
            "bad_session"
        ),
        0
    );
    assert_gauges_match_stats(&pool, &recorder);
}

#[tokio::test]
async fn session_use_duration_is_recorded_on_return() {
    let (pool, recorder) = bench_pool(SessionPoolSettings::new().with_limit(1));

    let lease = pool.acquire_explicit().await.expect("acquire");
    tokio::time::sleep(Duration::from_millis(5)).await;
    lease.return_to_pool();

    let observations = recorder.histogram_observations("ydb_session_pool_session_use_seconds");
    assert_eq!(observations.len(), 1, "exactly one use duration on return");
    assert!(
        observations[0] >= 0.004,
        "use duration must cover the lease time, got {}",
        observations[0]
    );
}

#[test]
fn attach_stream_close_reasons_are_counted_via_observer() {
    use crate::client_metrics::SessionPoolAttachStreamCloseReason;

    let (pool, recorder) = bench_pool(SessionPoolSettings::new().with_limit(1));
    for reason in [
        SessionPoolAttachStreamCloseReason::SessionShutdown,
        SessionPoolAttachStreamCloseReason::NodeShutdown,
        SessionPoolAttachStreamCloseReason::StreamClosed,
        SessionPoolAttachStreamCloseReason::Error,
    ] {
        pool.observer_for_metrics_tests()
            .attach_stream_closed(reason);
    }

    for reason in SessionPoolAttachStreamCloseReason::LABELS {
        assert_eq!(
            recorder.counter_value_with_label(
                "ydb_session_pool_attach_stream_closed_total",
                "reason",
                reason
            ),
            1,
            "AttachSession close reason {reason} must be recorded"
        );
    }
}

#[tokio::test]
async fn warm_up_overflow_closes_sessions_as_pool_full() {
    let (pool, recorder) = bench_pool(SessionPoolSettings::new().with_limit(2));
    pool.warm_up_for_tests(5)
        .await
        .expect("warm-up must succeed");

    assert_eq!(gauge(&recorder, "ydb_session_pool_sessions", "idle"), 2.0);
    assert_eq!(
        recorder.counter_value_with_label(
            "ydb_session_pool_sessions_closed_total",
            "reason",
            "pool_full"
        ),
        3
    );
    assert_eq!(
        recorder.counter_value_with_label(
            "ydb_session_pool_sessions_closed_total",
            "reason",
            "usage_limit"
        ),
        0
    );
}

#[tokio::test]
async fn two_pools_contribute_deltas_and_drop_independently() {
    let recorder = Arc::new(TestRecorder::new());
    let first = SessionPool::new_explicit_bench_with_metrics(
        SessionPoolSettings::new().with_limit(2),
        recorder.clone(),
    );
    let second = SessionPool::new_explicit_bench_with_metrics(
        SessionPoolSettings::new().with_limit(3),
        recorder.clone(),
    );
    assert_eq!(recorder.gauge_value("ydb_session_pool_size_limit"), 5.0);

    let first_lease = first.acquire_explicit().await.expect("first pool acquire");
    let second_lease = second
        .acquire_explicit()
        .await
        .expect("second pool acquire");
    assert_eq!(gauge(&recorder, "ydb_session_pool_sessions", "active"), 2.0);
    first_lease.return_to_pool();
    second_lease.return_to_pool();
    assert_eq!(gauge(&recorder, "ydb_session_pool_sessions", "idle"), 2.0);

    drop(first);
    assert_eq!(recorder.gauge_value("ydb_session_pool_size_limit"), 3.0);
    assert_eq!(gauge(&recorder, "ydb_session_pool_sessions", "idle"), 1.0);
    assert_eq!(
        recorder.counter_value_with_label(
            "ydb_session_pool_sessions_closed_total",
            "reason",
            "shutdown"
        ),
        1
    );

    drop(second);
    assert_eq!(recorder.gauge_value("ydb_session_pool_sessions"), 0.0);
    assert_eq!(recorder.gauge_value("ydb_session_pool_size_limit"), 0.0);
    assert_eq!(
        recorder.gauge_value("ydb_session_pool_pending_requests"),
        0.0
    );
}

#[tokio::test]
async fn parallel_mutations_keep_additive_gauges_consistent() {
    let (pool, recorder) = bench_pool(SessionPoolSettings::new().with_limit(4));
    let mut tasks = Vec::new();
    for _ in 0..8 {
        tasks.push(tokio::spawn({
            let pool = pool.clone();
            async move {
                for _ in 0..25 {
                    let lease = pool.acquire_explicit().await.expect("parallel acquire");
                    tokio::task::yield_now().await;
                    lease.return_to_pool();
                }
            }
        }));
    }
    for task in tasks {
        task.await.expect("parallel worker must finish");
    }

    assert_gauges_match_stats(&pool, &recorder);
    assert_eq!(gauge(&recorder, "ydb_session_pool_sessions", "active"), 0.0);
    assert_eq!(gauge(&recorder, "ydb_session_pool_sessions", "idle"), 4.0);
    assert!(
        recorder
            .gauge_observations("ydb_session_pool_sessions")
            .iter()
            .all(|value| *value >= 0.0),
        "ordered per-pool deltas must never produce negative gauges"
    );
}

#[test]
fn default_recorder_smoke_for_pool_constructors() {
    let (pool, recorder) = bench_pool(SessionPoolSettings::new().with_limit(1));
    assert_gauges_match_stats(&pool, &recorder);
}
