//! Query Service public facade (<https://github.com/ydb-platform/ydb-rs-sdk/issues/207>).
//!
//! Requires Rust 1.85+ (`AsyncFnMut` in [`QueryClient::retry_tx`]).

mod builders;
mod exec;
pub(crate) mod hooks;
mod internal;
mod retry_tx;
mod script;
mod stream_facade;

#[cfg(test)]
mod integration_test;

#[cfg(test)]
mod query_hooks_integration_test;

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
use http::Uri;
use tracing::instrument;

use crate::client_query::exec::TxState;
use crate::errors::{YdbError, YdbOrCustomerError, YdbResult, YdbResultWithCustomerErr};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::result::Row;

use crate::session_pool::SessionPool;
use builders::{impl_client_query_methods, impl_transaction_query_methods};
use exec::{
    ClientExecContext, TransactionExecContext, spawn_query_tx_rollback_on_drop, transaction_commit,
    transaction_ensure_begin, transaction_exec_context, transaction_identity, transaction_rollback,
};
use hooks::{QueryTxCommitStatus, QueryTxHook};

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
/// | Mode | One-shot [`QueryClient`] | Interactive [`Transaction`] |
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
pub enum TxMode {
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

impl TxMode {
    pub(crate) fn supported_in_interactive(self) -> bool {
        matches!(
            self,
            Self::SerializableReadWrite | Self::SnapshotReadOnly | Self::SnapshotReadWrite
        )
    }
}

#[derive(Clone, Debug)]
pub struct TransactionOptions {
    mode: TxMode,
    /// Call `BeginTransaction` RPC before the first `ExecuteQuery` instead of lazy `BeginTx`.
    begin: bool,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self {
            mode: TxMode::SerializableReadWrite,
            begin: false,
        }
    }
}

impl TransactionOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_mode(mut self, mode: TxMode) -> Self {
        self.mode = mode;
        self
    }

    /// Explicit transaction start: the first operation in [`Transaction`] calls
    /// `BeginTransaction` RPC and obtains `tx_id` before any `ExecuteQuery`.
    ///
    /// Default (lazy tx): the first `ExecuteQuery` carries `BeginTx` in `tx_control` without a
    /// separate RPC — see [`Transaction::begin`] for the same behavior inside the callback.
    pub fn with_begin(mut self) -> Self {
        self.begin = true;
        self
    }

    pub(crate) fn mode(&self) -> TxMode {
        self.mode
    }

    pub(crate) fn begin(&self) -> bool {
        self.begin
    }
}

pub struct QueryClient {
    ctx: ClientExecContext,
}

impl Clone for QueryClient {
    fn clone(&self) -> Self {
        Self {
            ctx: self.ctx.clone(),
        }
    }
}

impl QueryClient {
    impl_client_query_methods!();

    pub(crate) fn new(
        connection_manager: GrpcConnectionManager,
        session_pool: SessionPool,
        retry_control: std::sync::Arc<crate::retry_budget::RetryControl>,
    ) -> Self {
        Self {
            ctx: ClientExecContext {
                connection_manager,
                session_pool,
                retry_control,
            },
        }
    }

    /// Run a callback inside a retried interactive transaction.
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # use ydb::{AccessTokenCredentials, ClientBuilder, TxMode, YdbResultWithCustomerErr};
    /// #
    /// # #[tokio::main]
    /// # async fn main() -> YdbResultWithCustomerErr<()> {
    /// # let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?
    /// #     .with_credentials(AccessTokenCredentials::from("token"))
    /// #     .client()?;
    /// client.query_client()
    ///     .retry_tx(async |_tx| Ok(()))
    ///     .isolation(TxMode::SerializableReadWrite)
    ///     .timeout(Duration::from_secs(30))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn retry_tx<F, T>(&self, callback: F) -> RetryTxBuilder<'_, F, T>
    where
        F: AsyncFnMut(&mut Transaction) -> YdbResultWithCustomerErr<T>,
    {
        RetryTxBuilder::new(self, callback)
    }

    #[instrument(
        name = "ydb.Try",
        skip_all,
        fields(
            ydb.retry.attempt = attempt,
            ydb.retry.backoff_ms = tracing::field::Empty,
            db.system.name = "ydb",
        ),
        err,
    )]
    async fn try_attempt_body<F, T>(
        &self,
        callback: &mut F,
        mut tx: Transaction,
        attempt: usize,
        start: Instant,
        wall_timeout: Option<Duration>,
        idempotent: bool,
    ) -> YdbResultWithCustomerErr<Option<T>>
    where
        F: AsyncFnMut(&mut Transaction) -> YdbResultWithCustomerErr<T>,
    {
        use crate::retry_budget::{RetryPauseError, pause_before_retry};
        use exec::check_retry_tx_error;

        #[instrument(name = "ydb.Try.Attempt", skip_all, fields(db.system.name = "ydb"))]
        async fn try_attempt<F, T>(
            callback: &mut F,
            tx: &mut Transaction,
        ) -> Result<Result<T, YdbOrCustomerError>, Box<dyn Any + Send>>
        where
            F: AsyncFnMut(&mut Transaction) -> YdbResultWithCustomerErr<T>,
        {
            AssertUnwindSafe(callback(tx)).catch_unwind().await
        }

        let callback_result = try_attempt(callback, &mut tx).await;

        let err = match callback_result {
            Ok(Ok(value)) => match resolve_post_callback_action(&tx.ctx.state) {
                PostCallbackAction::Return(status) => {
                    tx.notify_hooks(status);
                    return Ok(Some(value));
                }
                PostCallbackAction::Commit => {
                    return match tx.commit().await {
                        Ok(()) => {
                            tx.notify_hooks(QueryTxCommitStatus::Committed);
                            Ok(Some(value))
                        }
                        // Commit outcome is ambiguous on transport errors; never retry.
                        Err(e) => {
                            tx.notify_hooks(QueryTxCommitStatus::Aborted);
                            Err(YdbOrCustomerError::YDB(e))
                        }
                    };
                }
                PostCallbackAction::Retry(err) => {
                    tx.notify_hooks(QueryTxCommitStatus::Aborted);
                    err
                }
                PostCallbackAction::Fail(err) => {
                    tx.notify_hooks(QueryTxCommitStatus::Aborted);
                    return Err(err);
                }
            },
            Ok(Err(err)) => {
                tx.rollback_quiet().await;
                tx.notify_hooks(QueryTxCommitStatus::Aborted);
                err
            }
            Err(panic_payload) => {
                tx.rollback_quiet().await;
                tx.notify_hooks(QueryTxCommitStatus::Aborted);
                YdbOrCustomerError::YDB(YdbError::Custom(format!(
                    "query transaction callback panicked: {}",
                    panic_message(panic_payload)
                )))
            }
        };

        if !check_retry_tx_error(&err, idempotent) {
            return Err(err);
        }
        match pause_before_retry(&self.ctx.retry_control, attempt, start, wall_timeout).await {
            Ok(()) => {}
            Err(RetryPauseError::Timeout) | Err(RetryPauseError::Budget(_)) => {
                return Err(err);
            }
        }
        Ok(None)
    }

    #[instrument(name = "ydb.RunWithRetry", skip_all, fields(db.system.name = "ydb", ydb.Query.idempotent = idempotent), err)]
    pub(crate) async fn run_retry_tx<F, T>(
        &self,
        mut callback: F,
        options: TransactionOptions,
        wall_timeout: Option<Duration>,
        idempotent: bool,
    ) -> YdbResultWithCustomerErr<T>
    where
        F: AsyncFnMut(&mut Transaction) -> YdbResultWithCustomerErr<T>,
    {
        let start = Instant::now();
        let absolute_deadline = wall_timeout.map(|d| start + d);
        let mut attempt = 0;

        loop {
            self.ctx.retry_control.metrics().record_attempt();
            attempt += 1;

            let tx = Transaction::new(
                self.ctx.connection_manager.clone(),
                self.ctx.session_pool.clone(),
                options.clone(),
                absolute_deadline,
            );

            let result = self
                .try_attempt_body(&mut callback, tx, attempt, start, wall_timeout, idempotent)
                .await?;

            if let Some(value) = result {
                return Ok(value);
            }
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
}

enum PostCallbackAction {
    Return(QueryTxCommitStatus),
    Commit,
    Retry(YdbOrCustomerError),
    Fail(YdbOrCustomerError),
}

fn resolve_post_callback_action(state: &TxState) -> PostCallbackAction {
    match state {
        TxState::RolledBack => PostCallbackAction::Return(QueryTxCommitStatus::Aborted),
        TxState::Committed => PostCallbackAction::Return(QueryTxCommitStatus::Committed),
        TxState::Invalidated(err) => {
            PostCallbackAction::Retry(YdbOrCustomerError::YDB(err.clone()))
        }
        TxState::Ambiguous(err) => PostCallbackAction::Fail(YdbOrCustomerError::YDB(err.clone())),
        TxState::Active => PostCallbackAction::Commit,
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

pub struct Transaction {
    ctx: TransactionExecContext,
}

impl Transaction {
    impl_transaction_query_methods!();

    fn new(
        connection_manager: GrpcConnectionManager,
        session_pool: SessionPool,
        options: TransactionOptions,
        retry_deadline: Option<Instant>,
    ) -> Self {
        Self {
            ctx: transaction_exec_context(
                connection_manager,
                session_pool,
                options,
                retry_deadline,
            ),
        }
    }

    pub fn mode(&self) -> TxMode {
        self.ctx.tx_mode
    }

    pub(crate) fn register_hook(&mut self, hook: impl QueryTxHook) {
        self.ctx.hooks.push(Box::new(hook));
    }

    /// Explicitly open the transaction via `BeginTransaction` RPC.
    ///
    /// By default (lazy tx) the transaction materializes on the first query. Call this when you
    /// need `tx_id` before any YQL, or configure [`TransactionOptions::with_begin`]
    /// on the client so the first operation does this automatically.
    pub async fn begin(&mut self) -> YdbResult<()> {
        if !self.ctx.state.is_active() {
            return Err(YdbError::Custom("transaction already finished".to_string()));
        }
        transaction_ensure_begin(&mut self.ctx, false).await
    }

    /// Session and transaction ids for topic offset updates inside a transaction.
    pub(crate) async fn identity(&mut self) -> YdbResult<(String, String)> {
        if !self.ctx.state.is_active() {
            return Err(YdbError::Custom("transaction already finished".to_string()));
        }
        transaction_identity(&mut self.ctx).await
    }

    pub async fn rollback(&mut self) -> YdbResult<()> {
        if !self.ctx.state.is_active() {
            return Ok(());
        }
        transaction_rollback(&mut self.ctx).await
    }

    async fn commit(&mut self) -> YdbResult<()> {
        transaction_commit(&mut self.ctx).await
    }

    async fn rollback_quiet(&mut self) {
        if self.ctx.state.is_active() {
            let _ = transaction_rollback(&mut self.ctx).await;
        }
    }

    fn notify_hooks(&mut self, status: QueryTxCommitStatus) {
        for hook in &mut self.ctx.hooks {
            hook.after_commit(status);
        }
    }

    pub(crate) async fn tx_identity(&mut self) -> YdbResult<QueryTxIdentity> {
        transaction_ensure_begin(&mut self.ctx, false).await?;

        let transaction_id = self
            .ctx
            .tx_id
            .as_ref()
            .ok_or(YdbError::custom("no transaction id"))?
            .to_string();

        let session_id = self
            .ctx
            .pooled_lease
            .as_ref()
            .ok_or(YdbError::custom("no session id"))?
            .session_id()
            .to_string();

        Ok(QueryTxIdentity {
            transaction_id,
            session_id,
        })
    }

    pub(crate) async fn uri(&mut self) -> YdbResult<Option<&Uri>> {
        transaction_ensure_begin(&mut self.ctx, false).await?;
        Ok(self.ctx.query_node.as_ref())
    }

    #[cfg(test)]
    pub(crate) fn tx_id_for_test(&self) -> Option<&str> {
        self.ctx.tx_id.as_deref()
    }
}

pub(crate) struct QueryTxIdentity {
    pub(crate) transaction_id: String,
    pub(crate) session_id: String,
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.ctx.state.is_active() {
            return;
        }
        self.ctx.state = TxState::RolledBack;
        self.notify_hooks(QueryTxCommitStatus::Aborted);
        spawn_query_tx_rollback_on_drop(&mut self.ctx);
    }
}

impl QueryExecutor for Transaction {
    type Scope = builders::Interactive;

    fn exec(&mut self, text: impl Into<String>) -> ExecBuilder<'_, Self::Scope> {
        Transaction::exec(self, text)
    }

    fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_, Self::Scope> {
        Transaction::query(self, text)
    }

    fn query_result_set(&mut self, text: impl Into<String>) -> ResultSetBuilder<'_, Self::Scope> {
        Transaction::query_result_set(self, text)
    }

    fn query_row(&mut self, text: impl Into<String>) -> QueryRowBuilder<'_, Row, Self::Scope> {
        Transaction::query_row(self, text)
    }
}

pub use builders::{
    CallBuilder, ClientOneShot, ExecBuilder, ExecCall, Interactive, OneResultSet, OneRow,
    OptionalRow, OptionalRowBuilder, QueryExecutor, QueryRowBuilder, QueryStreamBuilder,
    ResultSetBuilder, Streamed,
};
pub use retry_tx::RetryTxBuilder;
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

        assert!(
            take_single_row(int64_set(vec![]))
                .expect("empty rows")
                .is_none()
        );
        assert!(take_single_row(int64_set(vec![1, 2])).is_err());
    }

    #[test]
    fn invalidated_state_fails_instead_of_committing() {
        let state = TxState::Invalidated(YdbError::Custom("server aborted".into()));
        assert!(matches!(
            resolve_post_callback_action(&state),
            PostCallbackAction::Retry(_)
        ));
    }

    #[test]
    fn ambiguous_state_fails_instead_of_committing() {
        let state = TxState::Ambiguous(YdbError::Custom("rollback rpc failed".into()));
        assert!(matches!(
            resolve_post_callback_action(&state),
            PostCallbackAction::Fail(_)
        ));
    }

    #[test]
    fn committed_and_rolled_back_states_are_done_not_failed() {
        assert!(matches!(
            resolve_post_callback_action(&TxState::Committed),
            PostCallbackAction::Return(QueryTxCommitStatus::Committed)
        ));
        assert!(matches!(
            resolve_post_callback_action(&TxState::RolledBack),
            PostCallbackAction::Return(QueryTxCommitStatus::Aborted)
        ));
    }

    #[test]
    fn active_state_needs_a_real_commit() {
        assert!(matches!(
            resolve_post_callback_action(&TxState::Active),
            PostCallbackAction::Commit
        ));
    }
}
