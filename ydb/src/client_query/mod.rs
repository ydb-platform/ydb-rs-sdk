//! Query Service public facade (<https://github.com/ydb-platform/ydb-rs-sdk/issues/207>).
//!
//! Requires Rust 1.85+ (`AsyncFnMut` in [`QueryClient::retry_transaction`]).

mod builders;
mod exec;
mod internal;
mod script;
mod stream_facade;

#[cfg(test)]
mod integration_test;

#[cfg(test)]
mod session_pool_integration_test;

#[cfg(test)]
mod session_pool_bench;

#[cfg(test)]
mod tx_modes_integration_test;

#[cfg(test)]
mod concurrent_result_sets_test;

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use tokio::time::sleep;

use crate::client::TimeoutSettings;
use crate::errors::{YdbError, YdbOrCustomerError, YdbResult, YdbResultWithCustomerErr};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::result::Row;

use crate::session_pool::QuerySessionPool;
use builders::{impl_client_query_methods, impl_transaction_query_methods};
use exec::{
    check_retry_transaction_error, retry_wait, transaction_commit, transaction_ensure_begin,
    transaction_exec_context, transaction_rollback, ClientExecContext, TransactionExecContext,
    DEFAULT_QUERY_RETRY_BUDGET,
};

/// Row-to-struct mapping (the sqlx `FromRow` analogue).
pub trait FromYdbRow: Sized {
    fn from_row(row: Row) -> YdbResult<Self>;
}

impl FromYdbRow for Row {
    fn from_row(row: Row) -> YdbResult<Self> {
        Ok(row)
    }
}

/// Query Service transaction isolation mode.
///
/// | Mode | One-shot [`QueryClient`] | Interactive [`QueryTransaction`] |
/// |------|--------------------------|----------------------------------|
/// | [`Implicit`](Self::Implicit) | yes (default) | no |
/// | [`SerializableReadWrite`](Self::SerializableReadWrite) | yes | yes (default) |
/// | [`SnapshotReadOnly`](Self::SnapshotReadOnly) | yes | yes |
/// | [`SnapshotReadWrite`](Self::SnapshotReadWrite) | yes | yes |
/// | [`StaleReadOnly`](Self::StaleReadOnly) | yes | no |
/// | [`OnlineReadOnly`](Self::OnlineReadOnly) | yes | no |
/// | [`OnlineReadOnlyInconsistent`](Self::OnlineReadOnlyInconsistent) | yes | no |
///
/// Default for one-shot calls is [`Implicit`](Self::Implicit) (`tx_control: None`): the server
/// picks isolation from the SQL kind (DDL — non-transactional, `SELECT` — snapshot read-only,
/// DML — serializable read-write). Override per call with [`CallBuilder::with_tx_mode`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum QueryTxMode {
    /// Server-side inference (ImplicitTx / NoTx). One-shot only.
    #[default]
    Implicit,
    SerializableReadWrite,
    SnapshotReadOnly,
    SnapshotReadWrite,
    StaleReadOnly,
    /// Online read-only with `allow_inconsistent_reads: false` (consistent reads).
    OnlineReadOnly,
    /// Online read-only with `allow_inconsistent_reads: true` (inconsistent reads).
    OnlineReadOnlyInconsistent,
}

impl QueryTxMode {
    pub(crate) fn supported_in_interactive(self) -> bool {
        matches!(
            self,
            Self::SerializableReadWrite | Self::SnapshotReadOnly | Self::SnapshotReadWrite
        )
    }
}

#[derive(Clone, Debug)]
pub struct QueryTransactionOptions {
    mode: QueryTxMode,
    /// Call `BeginTransaction` RPC before the first `ExecuteQuery` instead of lazy `BeginTx`.
    begin: bool,
}

impl Default for QueryTransactionOptions {
    fn default() -> Self {
        Self {
            mode: QueryTxMode::SerializableReadWrite,
            begin: false,
        }
    }
}

impl QueryTransactionOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_mode(mut self, mode: QueryTxMode) -> Self {
        self.mode = mode;
        self
    }

    /// Explicit transaction start: the first operation in [`QueryTransaction`] calls
    /// `BeginTransaction` RPC and obtains `tx_id` before any `ExecuteQuery`.
    ///
    /// Default (lazy tx): the first `ExecuteQuery` carries `BeginTx` in `tx_control` without a
    /// separate RPC — see [`QueryTransaction::begin`] for the same behavior inside the callback.
    pub fn with_begin(mut self) -> Self {
        self.begin = true;
        self
    }

    pub(crate) fn mode(&self) -> QueryTxMode {
        self.mode
    }

    pub(crate) fn begin(&self) -> bool {
        self.begin
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
    impl_client_query_methods!();

    pub(crate) fn new(
        connection_manager: GrpcConnectionManager,
        timeouts: TimeoutSettings,
        session_pool: QuerySessionPool,
    ) -> Self {
        Self {
            ctx: ClientExecContext {
                connection_manager,
                timeouts,
                idempotent_operation: false,
                retry_budget: DEFAULT_QUERY_RETRY_BUDGET,
                session_pool,
            },
            tx_options: QueryTransactionOptions::default(),
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

    /// Start a long-running script operation. Poll completion via
    /// [`crate::OperationClient::get_operation`], then read rows with
    /// [`Self::fetch_script_results`].
    pub fn execute_script(&self, text: impl Into<String>) -> script::ExecuteScriptBuilder<'_> {
        script::ExecuteScriptBuilder::new(&self.ctx, text.into())
    }

    /// Fetch a page of script results for a completed operation.
    pub fn fetch_script_results(
        &self,
        operation_id: impl Into<String>,
    ) -> script::FetchScriptResultsBuilder<'_> {
        script::FetchScriptResultsBuilder::new(&self.ctx, operation_id.into())
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
                self.ctx.session_pool.clone(),
                self.tx_options.clone(),
            );

            let callback_result = AssertUnwindSafe(callback(&mut tx)).catch_unwind().await;

            let err = match callback_result {
                Ok(Ok(value)) => {
                    if tx.state == TxState::RolledBack {
                        return Ok(value);
                    }
                    if tx.ctx.finished {
                        tx.state = TxState::Committed;
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

impl QueryExecutor for QueryClient {
    type Scope = builders::ClientOneShot;

    fn exec(&mut self, text: impl Into<String>) -> ExecBuilder<'_, Self::Scope> {
        QueryClient::exec(self, text)
    }

    fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_, Self::Scope> {
        QueryClient::query(self, text)
    }

    fn query_result_set(&mut self, text: impl Into<String>) -> ResultSetBuilder<'_, Self::Scope> {
        QueryClient::query_result_set(self, text)
    }

    fn query_row(&mut self, text: impl Into<String>) -> QueryRowBuilder<'_, Row, Self::Scope> {
        QueryClient::query_row(self, text)
    }
}

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
    impl_transaction_query_methods!();

    fn new(
        connection_manager: GrpcConnectionManager,
        timeouts: TimeoutSettings,
        session_pool: QuerySessionPool,
        options: QueryTransactionOptions,
    ) -> Self {
        Self {
            ctx: transaction_exec_context(connection_manager, timeouts, session_pool, options),
            state: TxState::Active,
        }
    }

    pub fn mode(&self) -> QueryTxMode {
        self.ctx.tx_mode
    }

    /// Explicitly open the transaction via `BeginTransaction` RPC.
    ///
    /// By default (lazy tx) the transaction materializes on the first query. Call this when you
    /// need `tx_id` before any YQL, or configure [`QueryTransactionOptions::with_begin`]
    /// on the client so the first operation does this automatically.
    pub async fn begin(&mut self) -> YdbResult<()> {
        if self.state != TxState::Active {
            return Err(YdbError::Custom("transaction already finished".to_string()));
        }
        transaction_ensure_begin(&mut self.ctx, false).await
    }

    pub async fn rollback(&mut self) -> YdbResult<()> {
        if self.ctx.finished || self.state == TxState::RolledBack {
            return Ok(());
        }
        transaction_rollback(&mut self.ctx).await?;
        self.state = TxState::RolledBack;
        Ok(())
    }

    async fn commit(&mut self) -> YdbResult<()> {
        if self.ctx.finished {
            self.state = TxState::Committed;
            return Ok(());
        }
        transaction_commit(&mut self.ctx).await?;
        self.state = TxState::Committed;
        Ok(())
    }

    async fn rollback_quiet(&mut self) {
        if self.state == TxState::Active && !self.ctx.finished {
            let _ = transaction_rollback(&mut self.ctx).await;
            self.state = TxState::RolledBack;
        }
    }

    #[cfg(test)]
    pub(crate) fn tx_id_for_test(&self) -> Option<&str> {
        self.ctx.tx_id.as_deref()
    }
}

impl QueryExecutor for QueryTransaction {
    type Scope = builders::Interactive;

    fn exec(&mut self, text: impl Into<String>) -> ExecBuilder<'_, Self::Scope> {
        QueryTransaction::exec(self, text)
    }

    fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_, Self::Scope> {
        QueryTransaction::query(self, text)
    }

    fn query_result_set(&mut self, text: impl Into<String>) -> ResultSetBuilder<'_, Self::Scope> {
        QueryTransaction::query_result_set(self, text)
    }

    fn query_row(&mut self, text: impl Into<String>) -> QueryRowBuilder<'_, Row, Self::Scope> {
        QueryTransaction::query_row(self, text)
    }
}

pub use crate::session_pool::QuerySessionPoolSettings;
pub use builders::{
    CallBuilder, ClientOneShot, ExecBuilder, ExecCall, Interactive, OneResultSet, OneRow,
    OptionalRow, OptionalRowBuilder, QueryExecutor, QueryRowBuilder, QueryStreamBuilder,
    ResultSetBuilder, Streamed,
};
pub use script::{ExecuteScriptBuilder, FetchScriptResultsBuilder};
pub use script::{ExecuteScriptOperation, FetchScriptResult};
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

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;
    use crate::grpc_wrapper::raw_table_service::value::{RawColumn, RawResultSet, RawValue};
    use crate::result::ResultSet;

    use builders::{exactly_one_set, take_single_row};

    fn int64_set(values: Vec<i64>) -> ResultSet {
        RawResultSet {
            columns: vec![RawColumn {
                name: "id".to_string(),
                column_type: RawType::Int64,
            }],
            rows: values
                .into_iter()
                .map(|v| vec![RawValue::Int64(v)])
                .collect(),
            truncated: false,
        }
        .try_into()
        .expect("valid result set")
    }

    #[test]
    fn exactly_one_set_and_take_single_row() {
        assert!(exactly_one_set(vec![]).is_err());
        assert!(exactly_one_set(vec![int64_set(vec![1])]).is_ok());
        assert!(exactly_one_set(vec![int64_set(vec![1]), int64_set(vec![2])]).is_err());

        assert!(take_single_row(int64_set(vec![]))
            .expect("empty rows")
            .is_none());
        assert!(take_single_row(int64_set(vec![1, 2])).is_err());
    }
}
