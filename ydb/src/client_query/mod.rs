//! Query Service public facade (<https://github.com/ydb-platform/ydb-rs-sdk/issues/207>).
//!
//! Requires Rust 1.85+ (`AsyncFnMut` in [`QueryClient::retry_tx`]).

mod builders;
mod exec;
mod explain_query;
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

use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use http::Uri;
use tracing::instrument;

use crate::client_query::exec::TxState;
use crate::closure;
use crate::errors::{
    Idempotency, YdbError, YdbOrCustomerError, YdbResult, YdbResultWithCustomerErr,
};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::result::Row;

use crate::retry_settings::{RetrySettings, RetryState};
use crate::session_pool::SessionPool;
use builders::{impl_client_query_methods, impl_transaction_query_methods};
use exec::{
    ClientExecContext, TransactionExecContext, schedule_transaction_rollback, transaction_commit,
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
        retry_settings: RetrySettings,
    ) -> Self {
        Self {
            ctx: ClientExecContext {
                connection_manager,
                session_pool,
                retry_settings,
            },
        }
    }

    /// Run a callback inside a retried interactive transaction.
    ///
    /// The callback must implement [`RetryTxAttempt`] trait.
    /// Currently it's only implemented for output of [`closure`](crate::closure)
    /// macro. In future it can be implemented for plain asynchronous
    /// closures when their traits are expressible enough to do it.
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # use ydb::{AccessTokenCredentials, ClientBuilder, TxMode, YdbResultWithCustomerErr, closure};
    /// #
    /// # #[tokio::main]
    /// # async fn main() -> YdbResultWithCustomerErr<()> {
    /// # let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?
    /// #     .with_credentials(AccessTokenCredentials::from("token"))
    /// #     .build()
    /// #     .await?;
    /// client.query_client()
    ///     .retry_tx(closure!(async |_tx| Ok(())))
    ///     .isolation(TxMode::SerializableReadWrite)
    ///     .timeout(Duration::from_secs(30))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn retry_tx<F, T>(&self, callback: F) -> RetryTxBuilder<'_, F, T>
    where
        F: RetryTxAttempt<T>,
    {
        RetryTxBuilder::new(self, callback)
    }

    async fn try_attempt_body<F, T>(
        &self,
        callback: &mut F,
        mut tx: Transaction,
        idempotency: Idempotency,
    ) -> ControlFlow<YdbResultWithCustomerErr<T>, YdbOrCustomerError>
    where
        F: RetryTxAttempt<T>,
        T: Send,
    {
        #[instrument(name = "ydb.Try.Attempt", skip_all, fields(db.system.name = "ydb"))]
        async fn try_attempt<F, T>(
            callback: &mut F,
            tx: &mut Transaction,
        ) -> Result<T, YdbOrCustomerError>
        where
            F: RetryTxAttempt<T>,
        {
            callback.attempt(tx).await
        }

        match try_attempt(callback, &mut tx).await {
            Ok(value) => match resolve_post_callback_action(&tx.ctx.state) {
                PostCallbackAction::Return => {
                    tx.notify_hooks();
                    ControlFlow::Break(Ok(value))
                }
                PostCallbackAction::Commit => {
                    ControlFlow::Break(match tx.commit().await {
                        Ok(()) => {
                            tx.notify_hooks();
                            Ok(value)
                        }
                        // Commit outcome is ambiguous on transport errors; never retry.
                        Err(e) => {
                            tx.notify_hooks();
                            Err(e.into())
                        }
                    })
                }
                PostCallbackAction::Retry(err) => {
                    tx.notify_hooks();
                    ControlFlow::Continue(err.into())
                }
                PostCallbackAction::Fail(err) => {
                    tx.notify_hooks();
                    ControlFlow::Break(Err(err.into()))
                }
            },
            Err(err) => {
                tx.rollback_quiet().await;
                let outcome = match &tx.ctx.state {
                    TxState::Committed => ControlFlow::Break(Err(err)),
                    TxState::Ambiguous(transaction_error) => {
                        ControlFlow::Break(Err(transaction_error.clone().into()))
                    }
                    TxState::RolledBack | TxState::Invalidated(_) => ControlFlow::Continue(err),
                    TxState::Active(_) => ControlFlow::Break(Err(YdbError::InternalError(
                        "transaction remained active after rollback".to_string(),
                    )
                    .into())),
                };
                tx.notify_hooks();
                outcome
            }
        }?
        .retry_flow(idempotency)
    }

    #[instrument(name = "ydb.RunWithRetry", skip_all, fields(db.system.name = "ydb", ydb.Query.idempotent = idempotency.is_idempotent()), err)]
    pub(crate) async fn run_retry_tx<F, T>(
        &self,
        callback: F,
        options: TransactionOptions,
        wall_timeout: Option<Duration>,
        idempotency: Idempotency,
    ) -> YdbResultWithCustomerErr<T>
    where
        F: RetryTxAttempt<T>,
        T: Send,
    {
        let result = self
            .ctx
            .retry_settings
            .clone()
            .with_deadline(wall_timeout)
            .retry(closure!(
                [&client = self, callback, &options],
                async |retry: &RetryState| {
                    let lease = match client.ctx.session_pool.acquire_explicit().await {
                        Ok(lease) => lease,
                        Err(err) => {
                            return YdbOrCustomerError::from(err)
                                .retry_flow(Idempotency::Idempotent);
                        }
                    };
                    let tx = Transaction::new(
                        client.ctx.connection_manager.clone(),
                        lease,
                        options.clone(),
                        wall_timeout.map(|d| retry.start_time + d),
                    );

                    client.try_attempt_body(callback, tx, idempotency).await
                }
            ))
            .await;

        match result {
            ControlFlow::Continue(err) => Err(err.unwrap_or(YdbError::DeadlineExceeded.into())),
            ControlFlow::Break(Err(err)) => Err(err),
            ControlFlow::Break(Ok(value)) => Ok(value),
        }
    }

    /// Analyze a query's execution plan without running it (`EXEC_MODE_EXPLAIN`).
    ///
    /// The server compiles the query — resolving types and schema, so a syntax error or a missing
    /// table fails here — and returns the plan and MiniKQL AST as [`ExplainResult`]. Nothing is
    /// executed and no data is touched.
    ///
    /// Statements with nothing to plan (DDL, for instance) come back without statistics and
    /// produce an error rather than an empty [`ExplainResult`].
    ///
    /// One-shot only (implicit session, no transaction control, no parameters); not available
    /// inside [`Transaction`]. Retried as idempotent; bound it with
    /// [`.timeout()`](ExplainQueryBuilder::timeout).
    ///
    /// ```no_run
    /// # use ydb::{ClientBuilder, YdbResult};
    /// # #[tokio::main]
    /// # async fn main() -> YdbResult<()> {
    /// # let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?
    /// #     .build()
    /// #     .await?;
    /// let plan = client.query_client().explain("SELECT 1").await?;
    /// println!("{}", plan.query_plan);
    /// # Ok(())
    /// # }
    /// ```
    pub fn explain(&self, text: impl Into<String>) -> ExplainQueryBuilder<'_> {
        ExplainQueryBuilder::new(&self.ctx, text.into())
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
    Return,
    Commit,
    Retry(YdbError),
    Fail(YdbError),
}

fn resolve_post_callback_action(state: &TxState) -> PostCallbackAction {
    match state {
        TxState::RolledBack | TxState::Committed => PostCallbackAction::Return,
        TxState::Invalidated(error) => PostCallbackAction::Retry(error.clone()),
        TxState::Ambiguous(err) => PostCallbackAction::Fail(err.clone()),
        TxState::Active(_) => PostCallbackAction::Commit,
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
        lease: crate::session_pool::SessionPoolLease,
        options: TransactionOptions,
        retry_deadline: Option<Instant>,
    ) -> Self {
        Self {
            ctx: transaction_exec_context(connection_manager, lease, options, retry_deadline),
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
        transaction_ensure_begin(&mut self.ctx).await
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

    fn notify_hooks(&mut self) {
        let status = if matches!(self.ctx.state, TxState::Committed) {
            QueryTxCommitStatus::Committed
        } else {
            QueryTxCommitStatus::Aborted
        };
        // Transaction state selects the outcome; consuming the hooks records delivery separately,
        // so cancellation can resolve every registered hook exactly once from any state.
        for mut hook in self.ctx.hooks.drain(..) {
            hook.after_commit(status);
        }
    }

    pub(crate) async fn tx_identity(&mut self) -> YdbResult<QueryTxIdentity> {
        let (session_id, transaction_id) = transaction_identity(&mut self.ctx).await?;
        Ok(QueryTxIdentity {
            transaction_id,
            session_id,
        })
    }

    pub(crate) async fn uri(&mut self) -> YdbResult<&Uri> {
        transaction_ensure_begin(&mut self.ctx).await?;
        Ok(self.ctx.session_lease()?.node_uri())
    }

    #[cfg(test)]
    pub(crate) fn tx_id_for_test(&self) -> Option<&str> {
        self.ctx.transaction_id().map(|id| id.as_str())
    }
}

pub(crate) struct QueryTxIdentity {
    pub(crate) transaction_id: String,
    pub(crate) session_id: String,
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.notify_hooks();
        let state = std::mem::replace(&mut self.ctx.state, TxState::RolledBack);
        match state {
            TxState::Active(active) => {
                schedule_transaction_rollback(self.ctx.connection_manager.clone(), active);
            }
            TxState::Committed
            | TxState::RolledBack
            | TxState::Invalidated(_)
            | TxState::Ambiguous(_) => {}
        }
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
pub use explain_query::{ExplainQueryBuilder, ExplainResult};
pub use retry_tx::{RetryTxAttempt, RetryTxBuilder};
pub use script::{ExecuteScriptBuilder, FetchScriptResultsBuilder};
pub use script::{ExecuteScriptOperation, FetchScriptResult};
pub use stream_facade::{QueryResultPart, QueryStats, QueryStream};

#[cfg(test)]
mod unit_tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use crate::GrpcOptions;
    use crate::errors::YdbStatusError;
    use crate::grpc_wrapper::raw_query_service::transaction_control::TransactionId;
    use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;
    use crate::grpc_wrapper::raw_table_service::value::{RawColumn, RawResultSet, RawValue};
    use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
    use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
    use crate::result::ResultSet;
    use crate::session_pool::SessionPoolSettings;
    use http::Uri;
    use ydb_grpc::ydb_proto::status_ids::StatusCode;

    use builders::{exactly_one_set, take_single_row};

    struct AbortCounter(Arc<AtomicUsize>);

    impl QueryTxHook for AbortCounter {
        fn after_commit(&mut self, status: QueryTxCommitStatus) {
            if status == QueryTxCommitStatus::Aborted {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    struct CommitCounter(Arc<AtomicUsize>);

    impl QueryTxHook for CommitCounter {
        fn after_commit(&mut self, status: QueryTxCommitStatus) {
            if status == QueryTxCommitStatus::Committed {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

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

    fn test_connection_manager() -> GrpcConnectionManager {
        GrpcConnectionManager::new(
            SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(
                Uri::from_static("http://127.0.0.1/bench"),
            ))),
            "bench".to_string(),
            MultiInterceptor::new(),
            GrpcOptions::default(),
        )
    }

    async fn invalidated_transaction(
        pool: &SessionPool,
        status: StatusCode,
    ) -> (Transaction, String) {
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let session_id = lease.session_id().to_string();
        let mut tx = Transaction::new(
            test_connection_manager(),
            lease,
            TransactionOptions::default(),
            None,
        );
        tx.ctx
            .apply_query_error(&YdbError::YdbStatusError(YdbStatusError {
                message: "transaction failed".to_string(),
                operation_status: status as i32,
                issues: Vec::new(),
            }));
        assert!(matches!(tx.ctx.state, TxState::Invalidated(_)));
        (tx, session_id)
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
            PostCallbackAction::Return
        ));
        assert!(matches!(
            resolve_post_callback_action(&TxState::RolledBack),
            PostCallbackAction::Return
        ));
    }

    #[tokio::test]
    async fn active_state_needs_a_real_commit() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let state = transaction_exec_context(
            test_connection_manager(),
            lease,
            TransactionOptions::default(),
            None,
        )
        .state;
        assert!(matches!(
            resolve_post_callback_action(&state),
            PostCallbackAction::Commit
        ));
    }

    #[tokio::test]
    async fn retry_transaction_owns_a_session_before_the_callback() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let client = QueryClient::new(
            test_connection_manager(),
            pool.clone(),
            RetrySettings::dont_retry(),
        );
        let observed_pool = pool.clone();

        let result: YdbResultWithCustomerErr<()> = client
            .retry_tx(closure!([&observed_pool], async |_tx| {
                assert_eq!(observed_pool.stats().in_use, 1);
                Ok(())
            }))
            .await;

        result.expect("transaction without queries must commit locally");
        assert_eq!(pool.stats().in_use, 0);
    }

    #[tokio::test]
    async fn retry_transaction_does_not_call_user_code_without_a_session() {
        let pool = SessionPool::new_explicit_bench_with_create_failures(
            SessionPoolSettings::new().with_limit(1),
            1,
        );
        let client = QueryClient::new(test_connection_manager(), pool, RetrySettings::dont_retry());
        let callback_called = Arc::new(AtomicBool::new(false));
        let observed_called = callback_called.clone();

        let result: YdbResultWithCustomerErr<()> = client
            .retry_tx(closure!([observed_called], async |_tx| {
                observed_called.store(true, Ordering::Relaxed);
                Ok(())
            }))
            .await;

        assert!(result.is_err());
        assert!(!callback_called.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn retry_transaction_retries_session_acquisition_before_user_code() {
        let pool = SessionPool::new_explicit_bench_with_create_failures(
            SessionPoolSettings::new().with_limit(1),
            1,
        );
        let client = QueryClient::new(
            test_connection_manager(),
            pool,
            RetrySettings::with_default_backoff().with_deadline(Duration::from_secs(1)),
        );
        let callback_called = Arc::new(AtomicBool::new(false));
        let observed_called = callback_called.clone();

        let result: YdbResultWithCustomerErr<()> = client
            .retry_tx(closure!([observed_called], async |_tx| {
                observed_called.store(true, Ordering::Relaxed);
                Ok(())
            }))
            .await;

        result.expect("session acquisition must be retried before user code runs");
        assert!(callback_called.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn cancelled_transaction_stream_fails_and_notifies_abort_hooks_once() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let client = QueryClient::new(test_connection_manager(), pool, RetrySettings::dont_retry());
        let aborts = Arc::new(AtomicUsize::new(0));
        let observed_aborts = aborts.clone();

        let result: YdbResultWithCustomerErr<()> = client
            .retry_tx(closure!([observed_aborts], async |tx: &mut Transaction| {
                exec::apply_stream_tx_id(&mut tx.ctx, TransactionId::from_server("tx-1".into()));
                tx.ctx
                    .abort_unconfirmed(YdbError::Transport("stream failed".into()));
                tx.register_hook(AbortCounter(observed_aborts.clone()));
                Ok(())
            }))
            .await;

        assert!(result.is_err());
        assert_eq!(aborts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn dropping_terminal_transaction_notifies_abort_hooks() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let aborts = Arc::new(AtomicUsize::new(0));
        let mut tx = Transaction::new(
            test_connection_manager(),
            lease,
            TransactionOptions::default(),
            None,
        );
        tx.register_hook(AbortCounter(aborts.clone()));
        tx.ctx
            .abort_unconfirmed(YdbError::Transport("stream failed".into()));

        drop(tx);

        assert_eq!(aborts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn dropping_transaction_does_not_notify_hooks_twice() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let aborts = Arc::new(AtomicUsize::new(0));
        let mut tx = Transaction::new(
            test_connection_manager(),
            lease,
            TransactionOptions::default(),
            None,
        );
        tx.register_hook(AbortCounter(aborts.clone()));
        tx.notify_hooks();

        drop(tx);

        assert_eq!(aborts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn dropping_committed_transaction_preserves_hook_status() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let commits = Arc::new(AtomicUsize::new(0));
        let mut tx = Transaction::new(
            test_connection_manager(),
            lease,
            TransactionOptions::default(),
            None,
        );
        tx.register_hook(CommitCounter(commits.clone()));
        exec::transaction_finish_query(&mut tx.ctx, true).expect("commit via query");

        drop(tx);

        assert_eq!(commits.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn invalidated_transaction_immediately_returns_healthy_session() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let (_tx, session_id) = invalidated_transaction(&pool, StatusCode::Aborted).await;

        let lease = pool
            .acquire_explicit()
            .await
            .expect("reacquire test session");
        assert_eq!(lease.session_id(), session_id);
        lease.return_to_pool();
    }

    #[tokio::test]
    async fn invalidated_transaction_immediately_discards_broken_session() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let (_tx, session_id) = invalidated_transaction(&pool, StatusCode::BadSession).await;

        let lease = pool
            .acquire_explicit()
            .await
            .expect("acquire replacement session");
        assert_ne!(lease.session_id(), session_id);
        lease.return_to_pool();
    }
}
