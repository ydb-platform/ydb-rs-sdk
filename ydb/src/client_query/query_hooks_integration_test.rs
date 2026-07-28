use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use tracing_test::traced_test;

use super::{
    Transaction,
    hooks::{QueryTxCommitStatus, QueryTxHook},
};
use crate::test_integration_helper::create_client;
use crate::{
    closure,
    errors::{YdbError, YdbResult, YdbResultWithCustomerErr},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
struct HookStats {
    before_commit: AtomicUsize,
    committed: AtomicUsize,
    aborted: AtomicUsize,
    fail_before_commit: AtomicBool,
}

impl HookStats {
    fn hook(self: &Arc<Self>) -> StatsHook {
        StatsHook {
            stats: Arc::clone(self),
        }
    }

    fn fail_before_commit(&self) {
        self.fail_before_commit.store(true, Ordering::SeqCst);
    }

    fn assert_counts(&self, before_commit: usize, committed: usize, aborted: usize) {
        assert_eq!(
            self.before_commit.load(Ordering::SeqCst),
            before_commit,
            "before_commit count"
        );
        assert_eq!(
            self.committed.load(Ordering::SeqCst),
            committed,
            "committed count"
        );
        assert_eq!(
            self.aborted.load(Ordering::SeqCst),
            aborted,
            "aborted count"
        );
    }
}

struct StatsHook {
    stats: Arc<HookStats>,
}

#[async_trait::async_trait]
impl QueryTxHook for StatsHook {
    async fn before_commit(&mut self) -> YdbResult<()> {
        self.stats.before_commit.fetch_add(1, Ordering::SeqCst);
        if self.stats.fail_before_commit.load(Ordering::SeqCst) {
            return Err(YdbError::custom("before_commit hook failed"));
        }
        Ok(())
    }

    fn after_commit(&mut self, status: QueryTxCommitStatus) {
        match status {
            QueryTxCommitStatus::Committed => {
                self.stats.committed.fetch_add(1, Ordering::SeqCst);
            }
            QueryTxCommitStatus::Aborted => {
                self.stats.aborted.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

fn register_stats_hook(tx: &mut Transaction, stats: &Arc<HookStats>) {
    tx.register_hook(stats.hook());
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn commit_implicit_calls_before_commit_once() -> YdbResult<()> {
    let client = create_client().await?;
    let stats = Arc::new(HookStats::default());

    client
        .query_client()
        .retry_tx(closure!([&stats], async |tx: &mut Transaction| {
            register_stats_hook(tx, stats);
            tx.exec("SELECT 1").await?;
            Ok(())
        }))
        .timeout(TEST_TIMEOUT)
        .await?;

    stats.assert_counts(1, 1, 0);
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn rollback_explicit_skips_before_commit() -> YdbResult<()> {
    let client = create_client().await?;
    let stats = Arc::new(HookStats::default());

    client
        .query_client()
        .retry_tx(closure!([&stats], async |tx: &mut Transaction| {
            register_stats_hook(tx, stats);
            tx.exec("SELECT 1").await?;
            tx.rollback().await?;
            Ok(())
        }))
        .timeout(TEST_TIMEOUT)
        .await?;

    stats.assert_counts(0, 0, 1);
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn rollback_on_callback_error_skips_before_commit() -> YdbResult<()> {
    let client = create_client().await?;
    let stats = Arc::new(HookStats::default());

    let result: YdbResultWithCustomerErr<()> = client
        .query_client()
        .retry_tx(closure!([&stats], async |tx: &mut Transaction| {
            register_stats_hook(tx, stats);
            tx.exec("SELECT 1").await?;
            Err(YdbError::custom("callback failed").into())
        }))
        .timeout(TEST_TIMEOUT)
        .await;

    assert!(result.is_err(), "callback error must be returned");
    stats.assert_counts(0, 0, 1);
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn commit_with_commit_calls_before_commit_once() -> YdbResult<()> {
    let client = create_client().await?;
    let stats = Arc::new(HookStats::default());

    client
        .query_client()
        .retry_tx(closure!([&stats], async |tx: &mut Transaction| {
            register_stats_hook(tx, stats);
            tx.exec("SELECT 1").with_commit(true).await?;
            Ok(())
        }))
        .timeout(TEST_TIMEOUT)
        .await?;

    stats.assert_counts(1, 1, 0);
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn finished_tx_rejects_second_with_commit_without_before_commit() -> YdbResult<()> {
    let client = create_client().await?;
    let stats = Arc::new(HookStats::default());

    client
        .query_client()
        .retry_tx(closure!([&stats], async |tx: &mut Transaction| {
            register_stats_hook(tx, stats);
            tx.exec("SELECT 1").with_commit(true).await?;

            assert!(tx.exec("SELECT 1").with_commit(true).await.is_err());

            Ok(())
        }))
        .timeout(TEST_TIMEOUT)
        .await?;

    stats.assert_counts(1, 1, 0);
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn before_commit_error_aborts_transaction() -> YdbResult<()> {
    let client = create_client().await?;
    let stats = Arc::new(HookStats::default());
    stats.fail_before_commit();

    let result: YdbResultWithCustomerErr<()> = client
        .query_client()
        .retry_tx(closure!([&stats], async |tx: &mut Transaction| {
            register_stats_hook(tx, stats);
            tx.exec("SELECT 1").await?;
            Ok(())
        }))
        .timeout(TEST_TIMEOUT)
        .await;

    let err = result.expect_err("before_commit error must abort retry_tx");
    assert!(
        err.to_string().contains("before_commit hook failed"),
        "unexpected before_commit error: {err}"
    );
    stats.assert_counts(1, 0, 1);
    Ok(())
}
