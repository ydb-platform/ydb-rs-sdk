//! Query Service public facade (<https://github.com/ydb-platform/ydb-rs-sdk/issues/207>).
//!
//! Requires Rust 1.85+ (`AsyncFnMut` in [`QueryClient::retry_tx`]).

mod builders;
mod exec;
mod explain_query;
pub(crate) mod hooks;
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
use crate::client_metrics::names::MetricsNames;
use crate::closure;
use crate::errors::{
    Idempotency, YdbError, YdbOrCustomerError, YdbResult, YdbResultWithCustomerErr,
};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::result::Row;

use crate::retry_settings::{RetrySettings, RetryState};
use crate::session_pool::SessionPool;
use builders::{impl_client_query_methods, impl_tx_query_methods};
use exec::{
    ClientExecContext, TxExecContext, ensure_interactive_tx_mode, tx_commit, tx_ensure_begin,
    tx_exec_context, tx_identity, tx_rollback,
};
use hooks::QueryTxHook;

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
        metrics_names: MetricsNames,
    ) -> Self {
        Self {
            ctx: ClientExecContext {
                connection_manager,
                session_pool,
                retry_settings,
                metrics_names,
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

    async fn run_tx_attempt<F, T>(
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

        let callback_result = try_attempt(callback, &mut tx).await;
        if let Err(error) = tx.ctx.resolve_unobserved_operation() {
            return ControlFlow::Break(Err(error.into()));
        }

        let value = match callback_result {
            Ok(value) => value,
            Err(err) => {
                if matches!(&tx.ctx.state, TxState::Committed) {
                    return ControlFlow::Break(Err(err));
                }
                let mut retry_error = retry_error_before_cleanup(&tx.ctx.state, err);
                if let Err(error) = tx_rollback(&mut tx.ctx).await {
                    tracing::warn!(
                        %error,
                        "rollback after transaction callback failure did not complete"
                    );
                    if matches!(&tx.ctx.state, TxState::Undetermined(_)) {
                        retry_error = RetryError::UndeterminedTx(error);
                    }
                }
                return retry_error.retry_flow(idempotency);
            }
        };

        let retry_error = match &tx.ctx.state {
            TxState::Committed | TxState::RolledBack => {
                return ControlFlow::Break(Ok(value));
            }
            TxState::AttemptFailed(error) => RetryError::AccordingToError(error.clone().into()),
            TxState::Undetermined(error) => RetryError::UndeterminedTx(error.clone()),
            TxState::Active(_) => match tx_commit(&mut tx.ctx).await {
                Ok(()) => return ControlFlow::Break(Ok(value)),
                Err(error) => {
                    if matches!(&tx.ctx.state, TxState::Committed) {
                        return ControlFlow::Break(Err(error.into()));
                    }
                    retry_error_for_operation(&tx.ctx.state, error)
                }
            },
        };

        retry_error.retry_flow(idempotency)
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
        ensure_interactive_tx_mode(options.mode())?;
        let result = self
            .ctx
            .retry_settings
            .clone()
            .with_deadline(wall_timeout)
            .retry(closure!(
                [&client = self, callback, &options],
                async |retry: &RetryState| {
                    let tx = match client
                        .create_tx_attempt(
                            options.clone(),
                            wall_timeout.map(|duration| retry.start_time + duration),
                        )
                        .await
                    {
                        Ok(tx) => tx,
                        Err(err) => {
                            return YdbOrCustomerError::from(err)
                                .retry_flow(Idempotency::Idempotent);
                        }
                    };

                    client.run_tx_attempt(callback, tx, idempotency).await
                }
            ))
            .await;

        match result {
            ControlFlow::Continue(err) => Err(err.unwrap_or(YdbError::DeadlineExceeded.into())),
            ControlFlow::Break(Err(err)) => Err(err),
            ControlFlow::Break(Ok(value)) => Ok(value),
        }
    }

    async fn create_tx_attempt(
        &self,
        options: TransactionOptions,
        retry_deadline: Option<Instant>,
    ) -> YdbResult<Transaction> {
        let lease = self.ctx.session_pool.acquire_explicit().await?;
        let query_client = match self
            .ctx
            .connection_manager
            .get_auth_service_to_node(RawQueryClient::new, lease.node_uri())
            .await
        {
            Ok(query_client) => query_client,
            Err(error) => return lease.finish(Err(error)),
        };

        Ok(Transaction::new(
            query_client,
            lease,
            options,
            retry_deadline,
            self.ctx.metrics_names.clone(),
        ))
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

enum RetryError {
    /// The error's own retry classification is sufficient.
    AccordingToError(YdbOrCustomerError),
    /// Repeating an unknown transaction outcome additionally requires an idempotent callback.
    UndeterminedTx(YdbError),
}

impl RetryError {
    fn retry_flow<T>(
        self,
        idempotency: Idempotency,
    ) -> ControlFlow<YdbResultWithCustomerErr<T>, YdbOrCustomerError> {
        match self {
            Self::AccordingToError(error) => error.retry_flow(idempotency),
            Self::UndeterminedTx(error) if idempotency.is_idempotent() => {
                YdbOrCustomerError::from(error).retry_flow(idempotency)
            }
            Self::UndeterminedTx(error) => ControlFlow::Break(Err(error.into())),
        }
    }
}

/// Keep the operation error as the retry cause; undetermined state only adds the requirement that
/// repeating the whole callback must be safe.
fn retry_error_for_operation(state: &TxState, error: YdbError) -> RetryError {
    match state {
        TxState::Undetermined(_) => RetryError::UndeterminedTx(error),
        TxState::Active(_)
        | TxState::Committed
        | TxState::RolledBack
        | TxState::AttemptFailed(_) => RetryError::AccordingToError(error.into()),
    }
}

/// Select the retry cause before cleanup, whose rollback attempt may replace the transaction state.
fn retry_error_before_cleanup(state: &TxState, callback_error: YdbOrCustomerError) -> RetryError {
    match state {
        TxState::AttemptFailed(error) => RetryError::AccordingToError(error.clone().into()),
        TxState::Undetermined(error) => RetryError::UndeterminedTx(error.clone()),
        TxState::Active(_) | TxState::Committed | TxState::RolledBack => {
            RetryError::AccordingToError(callback_error)
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
        self.ctx.metrics_names.client_query_row_counter.increment(1);
        QueryClient::query_row(self, text)
    }
}

pub struct Transaction {
    ctx: TxExecContext,
}

impl Transaction {
    impl_tx_query_methods!();

    fn new(
        query_client: RawQueryClient,
        lease: crate::session_pool::SessionPoolLease,
        options: TransactionOptions,
        retry_deadline: Option<Instant>,
        metrics_names: MetricsNames,
    ) -> Self {
        Self {
            ctx: tx_exec_context(query_client, lease, options, retry_deadline, metrics_names),
        }
    }

    pub fn mode(&self) -> TxMode {
        self.ctx.tx_mode
    }

    pub(crate) fn register_hook(&mut self, hook: impl QueryTxHook) -> YdbResult<()> {
        self.ctx.register_hook(Box::new(hook))
    }

    /// Explicitly open the transaction via `BeginTransaction` RPC.
    ///
    /// By default (lazy tx) the transaction materializes on the first query. Call this when you
    /// need `tx_id` before any YQL, or configure [`TransactionOptions::with_begin`]
    /// on the client so the first operation does this automatically.
    pub async fn begin(&mut self) -> YdbResult<()> {
        tx_ensure_begin(&mut self.ctx).await
    }

    /// Materialize the transaction and return its session-scoped identity for topic offset updates.
    ///
    /// The returned identifiers belong together and the transaction continues to own the session.
    pub(crate) async fn identity(&mut self) -> YdbResult<(String, String)> {
        tx_identity(&mut self.ctx).await
    }

    pub async fn rollback(&mut self) -> YdbResult<()> {
        tx_rollback(&mut self.ctx).await
    }

    /// Materialize the transaction and return its session-scoped identity.
    ///
    /// The returned identifiers belong together and the transaction continues to own the session.
    pub(crate) async fn tx_identity(&mut self) -> YdbResult<QueryTxIdentity> {
        let (session_id, transaction_id) = tx_identity(&mut self.ctx).await?;
        Ok(QueryTxIdentity {
            transaction_id,
            session_id,
        })
    }

    pub(crate) async fn uri(&mut self) -> YdbResult<&Uri> {
        tx_ensure_begin(&mut self.ctx).await?;
        Ok(self.ctx.session_lease()?.node_uri())
    }

    #[cfg(test)]
    pub(crate) fn tx_id_for_test(&self) -> Option<&str> {
        self.ctx.transaction_id()
    }
}

pub(crate) struct QueryTxIdentity {
    /// Transaction identifier issued within `session_id`.
    pub(crate) transaction_id: String,
    /// Session that owns `transaction_id` and remains leased by the transaction.
    pub(crate) session_id: String,
}

impl QueryExecutor for Transaction {
    type Scope = builders::Interactive;

    fn exec(&mut self, text: impl Into<String>) -> ExecBuilder<'_, Self::Scope> {
        self.ctx
            .metrics_names
            .client_transaction_exec_counter
            .increment(1);
        Transaction::exec(self, text)
    }

    fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_, Self::Scope> {
        Transaction::query(self, text)
    }

    fn query_result_set(&mut self, text: impl Into<String>) -> ResultSetBuilder<'_, Self::Scope> {
        Transaction::query_result_set(self, text)
    }

    fn query_row(&mut self, text: impl Into<String>) -> QueryRowBuilder<'_, Row, Self::Scope> {
        self.ctx
            .metrics_names
            .client_transaction_query_row_counter
            .increment(1);
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
pub use stream_facade::{QueryStats, QueryStream};

#[cfg(test)]
mod unit_tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use crate::GrpcOptions;
    use crate::errors::YdbStatusError;
    use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
    use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;
    use crate::grpc_wrapper::raw_table_service::value::{RawColumn, RawResultSet, RawValue};
    use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
    use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
    use crate::result::ResultSet;
    use crate::session_pool::SessionPoolSettings;
    use http::Uri;
    use ydb_grpc::ydb_proto::status_ids::StatusCode;

    use builders::{exactly_one_set, take_single_row};

    struct AbortCounterHook(Arc<AtomicUsize>);

    #[async_trait::async_trait]
    impl QueryTxHook for AbortCounterHook {
        fn after_commit(&mut self, status: hooks::QueryTxCommitStatus) {
            if status == hooks::QueryTxCommitStatus::Aborted {
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

    async fn test_transaction(lease: crate::session_pool::SessionPoolLease) -> Transaction {
        let manager = test_connection_manager();
        let query_client = manager
            .get_auth_service_to_node(RawQueryClient::new, lease.node_uri())
            .await
            .expect("create test query client");
        Transaction::new(
            query_client,
            lease,
            TransactionOptions::default(),
            None,
            MetricsNames::new(None),
        )
    }

    async fn failed_transaction(pool: &SessionPool, status: StatusCode) -> (Transaction, String) {
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let session_id = lease.session_id().to_string();
        let mut tx = test_transaction(lease).await;
        tx.ctx.mark_query_in_flight_for_test("tx-1");
        exec::tx_handle_query_error(
            &mut tx.ctx,
            &YdbError::YdbStatusError(YdbStatusError::new(
                "transaction failed",
                status as i32,
                Vec::new(),
            )),
        )
        .expect("in-flight query error must end the transaction attempt");
        assert!(matches!(tx.ctx.state, TxState::AttemptFailed(_)));
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
    async fn retry_transaction_validates_mode_before_acquiring_session() {
        let pool = SessionPool::new_explicit_bench_with_create_failures(
            SessionPoolSettings::new().with_limit(1),
            1,
        );
        let client = QueryClient::new(test_connection_manager(), pool, RetrySettings::dont_retry());
        let callback_called = Arc::new(AtomicBool::new(false));

        let observed_called = callback_called.clone();
        let invalid_mode: YdbResultWithCustomerErr<()> = client
            .retry_tx(closure!([observed_called], async |_tx| {
                observed_called.store(true, Ordering::Relaxed);
                Ok(())
            }))
            .isolation(TxMode::Implicit)
            .await;
        assert!(invalid_mode.is_err());
        assert!(!callback_called.load(Ordering::Relaxed));

        let observed_called = callback_called.clone();
        let valid_mode: YdbResultWithCustomerErr<()> = client
            .retry_tx(closure!([observed_called], async |_tx| {
                observed_called.store(true, Ordering::Relaxed);
                Ok(())
            }))
            .await;
        assert!(valid_mode.is_err());
        assert!(
            !callback_called.load(Ordering::Relaxed),
            "invalid options must not consume the injected session failure"
        );
    }

    #[tokio::test]
    async fn retry_transaction_retries_session_acquisition_before_callback() {
        let pool = SessionPool::new_explicit_bench_with_create_failures(
            SessionPoolSettings::new().with_limit(1),
            1,
        );
        let client = QueryClient::new(
            test_connection_manager(),
            pool,
            RetrySettings::with_default_backoff(),
        );
        let callback_calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = callback_calls.clone();

        let result: YdbResultWithCustomerErr<()> = client
            .retry_tx(closure!([observed_calls], async |_tx| {
                observed_calls.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }))
            .timeout(Duration::from_secs(1))
            .await;

        result.expect("session acquisition must retry before running user code");
        assert_eq!(callback_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn failed_transaction_immediately_discards_broken_session() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let (_tx, session_id) = failed_transaction(&pool, StatusCode::BadSession).await;

        let lease = pool
            .acquire_explicit()
            .await
            .expect("acquire replacement session");
        assert_ne!(lease.session_id(), session_id);
        lease.return_to_pool();
    }

    #[tokio::test]
    async fn cancelled_query_notifies_hooks_once_before_transaction_drop() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let mut tx = test_transaction(lease).await;
        let aborted = Arc::new(AtomicUsize::new(0));
        tx.register_hook(AbortCounterHook(Arc::clone(&aborted)))
            .expect("active transaction must accept a hook");
        tx.ctx.mark_query_in_flight_for_test("tx-1");

        exec::tx_cancel_query(&mut tx.ctx);
        assert_eq!(aborted.load(Ordering::Relaxed), 1);

        drop(tx);
        assert_eq!(aborted.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn dropping_drained_unclosed_stream_marks_transaction_undetermined() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let mut tx = test_transaction(lease).await;
        tx.ctx.mark_query_in_flight_for_test("tx-1");

        {
            let mut stream = QueryStream::from_tx(
                ExecuteQueryStream::from_test_parts(Vec::new()),
                &mut tx.ctx,
                false,
            );
            assert!(
                stream
                    .next_result_set()
                    .await
                    .expect("drain test stream")
                    .is_none()
            );
        }

        assert!(matches!(tx.ctx.state, TxState::Undetermined(_)));
    }
}
