//! Query Service public facade (<https://github.com/ydb-platform/ydb-rs-sdk/issues/207>).
//!
//! Requires Rust 1.85+ (`AsyncFnMut` in [`QueryClient::retry_transaction`]).
//!
//! This module is introduced incrementally: the [`QueryExecutor`] trait currently
//! exposes only streaming [`QueryExecutor::query`]. See the pull request description
//! for the planned follow-up PRs.

mod builders;
mod exec;
mod internal;
mod session_pool;
mod stream_facade;

#[cfg(test)]
mod integration_test;

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use tokio::time::sleep;

use crate::client::TimeoutSettings;
use crate::discovery::Discovery;
use crate::errors::{YdbError, YdbOrCustomerError, YdbResult, YdbResultWithCustomerErr};
use crate::grpc_connection_manager::GrpcConnectionManager;

use builders::impl_query_methods;
use exec::{
    check_retry_transaction_error, retry_wait, transaction_commit, transaction_exec_context,
    transaction_rollback, ClientExecContext, TransactionExecContext, DEFAULT_QUERY_RETRY_BUDGET,
};
use internal::{ExecCoreRef, HasCore};
use session_pool::QuerySessionPool;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum QueryTxMode {
    #[default]
    SerializableReadWrite,
    SnapshotReadOnly,
    StaleReadOnly,
    /// Online read-only mode with stale-replica reads disabled (`allow_inconsistent_reads: false`).
    OnlineReadOnly,
}

#[derive(Clone, Debug, Default)]
pub struct QueryTransactionOptions {
    mode: QueryTxMode,
}

impl QueryTransactionOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_mode(mut self, mode: QueryTxMode) -> Self {
        self.mode = mode;
        self
    }

    pub(crate) fn mode(&self) -> QueryTxMode {
        self.mode
    }
}

pub struct QueryClient {
    ctx: ClientExecContext,
    tx_options: QueryTransactionOptions,
}

impl Clone for QueryClient {
    fn clone(&self) -> Self {
        Self {
            ctx: self.ctx.clone(),
            tx_options: self.tx_options.clone(),
        }
    }
}

impl QueryClient {
    impl_query_methods!();

    pub(crate) fn new(
        connection_manager: GrpcConnectionManager,
        timeouts: TimeoutSettings,
        discovery: Arc<Box<dyn Discovery>>,
    ) -> Self {
        Self {
            ctx: ClientExecContext {
                connection_manager,
                timeouts,
                discovery,
                idempotent_operation: false,
                retry_budget: DEFAULT_QUERY_RETRY_BUDGET,
                implicit_session_pool: None,
            },
            tx_options: QueryTransactionOptions::default(),
        }
    }

    /// Configure an implicit session pool (empty `session_id`, no AttachSession).
    /// Limits concurrency and enables warm-up for one-shot streaming queries.
    pub fn with_implicit_session_pool(self, settings: QuerySessionPoolSettings) -> Self {
        let pool = QuerySessionPool::new_implicit(
            self.ctx.connection_manager.clone(),
            self.ctx.timeouts,
            self.ctx.discovery.clone(),
            settings,
        );
        Self {
            ctx: ClientExecContext {
                implicit_session_pool: Some(pool),
                ..self.ctx
            },
            tx_options: self.tx_options,
        }
    }

    pub fn clone_with_idempotent_operations(&self, idempotent: bool) -> Self {
        Self {
            ctx: ClientExecContext {
                idempotent_operation: idempotent,
                ..self.ctx.clone()
            },
            tx_options: self.tx_options.clone(),
        }
    }

    pub fn clone_with_transaction_options(&self, opts: QueryTransactionOptions) -> Self {
        Self {
            tx_options: opts,
            ..self.clone()
        }
    }

    /// Total wall-clock budget for automatic retries on idempotent operations
    /// (aligned with [`crate::TableClient::clone_with_retry_timeout`]).
    pub fn clone_with_retry_timeout(&self, timeout: Duration) -> Self {
        Self {
            ctx: ClientExecContext {
                retry_budget: timeout,
                ..self.ctx.clone()
            },
            tx_options: self.tx_options.clone(),
        }
    }

    pub fn clone_with_no_retry(&self) -> Self {
        Self {
            ctx: ClientExecContext {
                retry_budget: Duration::ZERO,
                ..self.ctx.clone()
            },
            tx_options: self.tx_options.clone(),
        }
    }

    pub async fn retry_transaction<T>(
        &self,
        mut callback: impl AsyncFnMut(&mut QueryTransaction) -> YdbResultWithCustomerErr<T>,
    ) -> YdbResultWithCustomerErr<T> {
        let retry_budget = self.ctx.retry_budget;
        let start = Instant::now();
        let mut attempt = 0;

        loop {
            attempt += 1;
            let mut tx = QueryTransaction::new(
                self.ctx.connection_manager.clone(),
                self.ctx.timeouts,
                self.ctx.discovery.clone(),
                self.tx_options.clone(),
            );

            let callback_result = AssertUnwindSafe(callback(&mut tx)).catch_unwind().await;

            let err = match callback_result {
                Ok(Ok(value)) => {
                    if tx.state == TxState::RolledBack {
                        return Ok(value);
                    }
                    return match tx.commit().await {
                        Ok(()) => Ok(value),
                        // Commit outcome is ambiguous on transport errors; never retry.
                        Err(e) => Err(YdbOrCustomerError::YDB(e)),
                    };
                }
                Ok(Err(err)) => {
                    tx.rollback_quiet().await;
                    err
                }
                Err(panic_payload) => {
                    tx.rollback_quiet().await;
                    YdbOrCustomerError::YDB(YdbError::Custom(format!(
                        "query transaction callback panicked: {}",
                        panic_message(panic_payload)
                    )))
                }
            };

            if !check_retry_transaction_error(&err) {
                return Err(err);
            }
            match retry_wait(attempt, start.elapsed(), retry_budget) {
                Some(wait) if wait > Duration::ZERO => sleep(wait).await,
                Some(_) => {}
                None => return Err(err),
            }
        }
    }
}

impl HasCore for QueryClient {
    fn core_mut(&mut self) -> ExecCoreRef<'_> {
        ExecCoreRef::Client(&mut self.ctx)
    }
}

impl QueryExecutor for QueryClient {}

#[derive(Debug, PartialEq, Eq)]
enum TxState {
    Active,
    Committed,
    RolledBack,
}

pub struct QueryTransaction {
    ctx: TransactionExecContext,
    state: TxState,
}

impl QueryTransaction {
    impl_query_methods!();

    fn new(
        connection_manager: GrpcConnectionManager,
        timeouts: TimeoutSettings,
        discovery: Arc<Box<dyn Discovery>>,
        options: QueryTransactionOptions,
    ) -> Self {
        Self {
            ctx: transaction_exec_context(
                connection_manager,
                timeouts,
                discovery,
                options,
            ),
            state: TxState::Active,
        }
    }

    pub fn mode(&self) -> QueryTxMode {
        self.ctx.tx_mode
    }

    pub async fn rollback(&mut self) -> YdbResult<()> {
        if self.state != TxState::Active {
            return Err(YdbError::Custom("transaction already finished".to_string()));
        }
        transaction_rollback(&mut self.ctx).await?;
        self.state = TxState::RolledBack;
        Ok(())
    }

    async fn commit(&mut self) -> YdbResult<()> {
        transaction_commit(&mut self.ctx).await?;
        self.state = TxState::Committed;
        Ok(())
    }

    async fn rollback_quiet(&mut self) {
        if self.state == TxState::Active {
            let _ = transaction_rollback(&mut self.ctx).await;
            self.state = TxState::RolledBack;
        }
    }
}

impl HasCore for QueryTransaction {
    fn core_mut(&mut self) -> ExecCoreRef<'_> {
        ExecCoreRef::Transaction(&mut self.ctx)
    }
}

impl QueryExecutor for QueryTransaction {}

pub use builders::{CallBuilder, QueryExecutor, QueryStreamBuilder, Streamed};
pub use session_pool::QuerySessionPoolSettings;
pub use stream_facade::{QueryStats, QueryStream};

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(msg) => *msg,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(msg) => (*msg).to_string(),
            Err(_) => "unknown panic payload".to_string(),
        },
    }
}
