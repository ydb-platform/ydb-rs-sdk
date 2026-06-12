//! Query Service public facade (<https://github.com/ydb-platform/ydb-rs-sdk/issues/207>).
//!
//! Requires Rust 1.85+ (`AsyncFnMut` in [`QueryClient::retry_transaction`]).

mod exec;

use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

use tokio::time::sleep;

use crate::client::TimeoutSettings;
use crate::errors::{YdbError, YdbOrCustomerError, YdbResult, YdbResultWithCustomerErr};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::result::{ResultSet, Row};
use crate::types::Value;

use exec::{
    apply_stream_tx_id, backoff, check_retry_error, client_begin_stream, client_run,
    transaction_begin_stream, transaction_commit, transaction_exec_context, transaction_rollback,
    transaction_run, ClientExecContext, TransactionExecContext,
};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// How [`QueryClient`] acquires a YDB session for each call.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum QuerySessionMode {
    /// Empty `session_id` in `ExecuteQueryRequest`: the server creates a session,
    /// runs the query, and closes the session (first release default).
    #[default]
    Implicit,
    /// Explicit session from a pool (not implemented yet).
    Pool,
}

/// Row-to-struct mapping (the sqlx `FromRow` analogue).
pub trait FromYdbRow: Sized {
    fn from_row(row: Row) -> YdbResult<Self>;
}

impl FromYdbRow for Row {
    fn from_row(row: Row) -> YdbResult<Self> {
        Ok(row)
    }
}

mod private {
    use super::*;

    pub(crate) enum ExecCore {
        Client(ClientExecContext),
        Transaction(TransactionExecContext),
    }

    impl Clone for ExecCore {
        fn clone(&self) -> Self {
            match self {
                ExecCore::Client(ctx) => ExecCore::Client(ctx.clone()),
                ExecCore::Transaction(_) => {
                    panic!("query transaction exec context must not be cloned")
                }
            }
        }
    }

    impl ExecCore {
        pub(crate) async fn run(
            &mut self,
            text: &str,
            params: &HashMap<String, Value>,
            opts: &CallOptions,
        ) -> YdbResult<Vec<ResultSet>> {
            match self {
                ExecCore::Client(ctx) => client_run(ctx, text, params, opts).await,
                ExecCore::Transaction(ctx) => transaction_run(ctx, text, params, opts).await,
            }
        }

        pub(crate) async fn begin_stream(
            &mut self,
            text: String,
            params: HashMap<String, Value>,
            opts: CallOptions,
        ) -> YdbResult<ExecuteQueryStream> {
            match self {
                ExecCore::Client(ctx) => client_begin_stream(ctx, text, params, opts).await,
                ExecCore::Transaction(ctx) => {
                    transaction_begin_stream(ctx, text, params, opts).await
                }
            }
        }
    }

    #[derive(Clone, Debug, Default)]
    pub(crate) struct CallOptions {
        pub timeout: Option<Duration>,
        pub idempotent: Option<bool>,
        pub collect_stats: bool,
        pub session_mode: Option<QuerySessionMode>,
    }

    pub(crate) trait HasCore {
        fn core_mut(&mut self) -> &mut ExecCore;
    }
}

use private::{CallOptions, ExecCore, HasCore};

pub enum ExecCall {}
pub struct OneRow<T>(PhantomData<T>);
pub struct OptionalRow<T>(PhantomData<T>);
pub enum OneResultSet {}
pub enum Streamed {}

pub type ExecBuilder<'a> = CallBuilder<'a, ExecCall>;
pub type QueryRowBuilder<'a, T = Row> = CallBuilder<'a, OneRow<T>>;
pub type OptionalRowBuilder<'a, T = Row> = CallBuilder<'a, OptionalRow<T>>;
pub type ResultSetBuilder<'a> = CallBuilder<'a, OneResultSet>;
pub type QueryStreamBuilder<'a> = CallBuilder<'a, Streamed>;

pub struct CallBuilder<'a, K> {
    core: &'a mut ExecCore,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
    _kind: PhantomData<fn() -> K>,
}

impl<'a, K> CallBuilder<'a, K> {
    fn new(core: &'a mut ExecCore, text: String) -> Self {
        Self {
            core,
            text,
            params: HashMap::new(),
            opts: CallOptions::default(),
            _kind: PhantomData,
        }
    }

    fn into_kind<K2>(self) -> CallBuilder<'a, K2> {
        CallBuilder {
            core: self.core,
            text: self.text,
            params: self.params,
            opts: self.opts,
            _kind: PhantomData,
        }
    }

    pub fn param(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.params.insert(name.into(), value.into());
        self
    }

    pub fn params(mut self, params: HashMap<String, Value>) -> Self {
        self.params.extend(params);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.opts.timeout = Some(timeout);
        self
    }

    pub fn idempotent(mut self, idempotent: bool) -> Self {
        self.opts.idempotent = Some(idempotent);
        self
    }

    pub fn collect_stats(mut self) -> Self {
        self.opts.collect_stats = true;
        self
    }

    /// Override session acquisition for this call (default: implicit session).
    pub fn session_mode(mut self, mode: QuerySessionMode) -> Self {
        self.opts.session_mode = Some(mode);
        self
    }

    /// Shorthand for [`Self::session_mode`] ([`QuerySessionMode::Implicit`]).
    pub fn implicit_session(self) -> Self {
        self.session_mode(QuerySessionMode::Implicit)
    }

    /// Shorthand for [`Self::session_mode`] ([`QuerySessionMode::Pool`]).
    pub fn pooled_session(self) -> Self {
        self.session_mode(QuerySessionMode::Pool)
    }
}

impl<'a, T> CallBuilder<'a, OneRow<T>> {
    pub fn typed<U: FromYdbRow>(self) -> CallBuilder<'a, OneRow<U>> {
        self.into_kind()
    }

    pub fn optional(self) -> CallBuilder<'a, OptionalRow<T>> {
        self.into_kind()
    }
}

fn exactly_one_set(mut sets: Vec<ResultSet>) -> YdbResult<ResultSet> {
    match sets.len() {
        0 => Err(YdbError::Custom("no result set".to_string())),
        1 => Ok(sets.pop().expect("len checked")),
        _ => Err(YdbError::Custom("more than one result set".to_string())),
    }
}

fn take_single_row(sets: Vec<ResultSet>) -> YdbResult<Option<Row>> {
    let mut rows = exactly_one_set(sets)?.into_iter();
    let row = rows.next();
    if rows.next().is_some() {
        return Err(YdbError::Custom(
            "result set has more than one row".to_string(),
        ));
    }
    Ok(row)
}

impl<'a> IntoFuture for CallBuilder<'a, ExecCall> {
    type Output = YdbResult<()>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            self.core.run(&self.text, &self.params, &self.opts).await?;
            Ok(())
        })
    }
}

impl<'a, T: FromYdbRow + 'a> IntoFuture for CallBuilder<'a, OneRow<T>> {
    type Output = YdbResult<T>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let sets = self.core.run(&self.text, &self.params, &self.opts).await?;
            let row = take_single_row(sets)?.ok_or(YdbError::NoRows)?;
            T::from_row(row)
        })
    }
}

impl<'a, T: FromYdbRow + 'a> IntoFuture for CallBuilder<'a, OptionalRow<T>> {
    type Output = YdbResult<Option<T>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let sets = self.core.run(&self.text, &self.params, &self.opts).await?;
            take_single_row(sets)?.map(T::from_row).transpose()
        })
    }
}

impl<'a> IntoFuture for CallBuilder<'a, OneResultSet> {
    type Output = YdbResult<ResultSet>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let sets = self.core.run(&self.text, &self.params, &self.opts).await?;
            exactly_one_set(sets)
        })
    }
}

impl<'a> IntoFuture for CallBuilder<'a, Streamed> {
    type Output = YdbResult<QueryStream<'a>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let stream = self
                .core
                .begin_stream(self.text, self.params, self.opts)
                .await?;
            Ok(QueryStream {
                core: self.core,
                stream,
            })
        })
    }
}

pub struct QueryStream<'a> {
    core: &'a mut ExecCore,
    stream: ExecuteQueryStream,
}

impl QueryStream<'_> {
    pub async fn next_result_set(&mut self) -> YdbResult<Option<ResultSet>> {
        let (raw, tx_id) = match self.stream.next_result_set().await? {
            Some(v) => v,
            None => return Ok(None),
        };
        if let ExecCore::Transaction(ctx) = self.core {
            apply_stream_tx_id(ctx, tx_id);
        }
        Ok(Some(ResultSet::try_from(raw)?))
    }

    pub fn stats(&self) -> Option<&QueryStats> {
        None
    }

    pub async fn close(self) -> YdbResult<()> {
        self.stream.close().await.map_err(Into::into)
    }
}

#[derive(Debug, Default)]
pub struct QueryStats {
    pub total_duration: Duration,
}

#[allow(private_bounds)]
pub trait QueryExecutor: HasCore {
    fn exec(&mut self, text: impl Into<String>) -> ExecBuilder<'_> {
        CallBuilder::new(self.core_mut(), text.into())
    }

    fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_> {
        CallBuilder::new(self.core_mut(), text.into())
    }

    fn query_result_set(&mut self, text: impl Into<String>) -> ResultSetBuilder<'_> {
        CallBuilder::new(self.core_mut(), text.into())
    }

    fn query_row(&mut self, text: impl Into<String>) -> QueryRowBuilder<'_, Row> {
        CallBuilder::new(self.core_mut(), text.into())
    }
}

macro_rules! impl_query_methods {
    () => {
        pub fn exec(&mut self, text: impl Into<String>) -> ExecBuilder<'_> {
            QueryExecutor::exec(self, text)
        }

        pub fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_> {
            QueryExecutor::query(self, text)
        }

        pub fn query_result_set(&mut self, text: impl Into<String>) -> ResultSetBuilder<'_> {
            QueryExecutor::query_result_set(self, text)
        }

        pub fn query_row(&mut self, text: impl Into<String>) -> QueryRowBuilder<'_, Row> {
            QueryExecutor::query_row(self, text)
        }
    };
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum QueryTxMode {
    #[default]
    SerializableReadWrite,
    SnapshotReadOnly,
    StaleReadOnly,
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

#[derive(Clone)]
pub struct QueryClient {
    core: ExecCore,
    tx_options: QueryTransactionOptions,
}

impl QueryClient {
    impl_query_methods!();

    pub(crate) fn new(
        connection_manager: GrpcConnectionManager,
        timeouts: TimeoutSettings,
    ) -> Self {
        Self {
            core: ExecCore::Client(ClientExecContext {
                connection_manager,
                timeouts,
                session_mode: QuerySessionMode::Implicit,
                idempotent_operation: false,
                retry_timeout: Duration::from_secs(5),
                max_attempts: 3,
            }),
            tx_options: QueryTransactionOptions::default(),
        }
    }

    pub fn clone_with_idempotent_operations(&self, idempotent: bool) -> Self {
        let Self { core, tx_options } = self.clone();
        let ExecCore::Client(mut ctx) = core else {
            return self.clone();
        };
        ctx.idempotent_operation = idempotent;
        Self {
            core: ExecCore::Client(ctx),
            tx_options,
        }
    }

    pub fn clone_with_transaction_options(&self, opts: QueryTransactionOptions) -> Self {
        Self {
            tx_options: opts,
            ..self.clone()
        }
    }

    pub fn clone_with_retry_timeout(&self, timeout: Duration) -> Self {
        let Self { core, tx_options } = self.clone();
        let ExecCore::Client(mut ctx) = core else {
            return self.clone();
        };
        ctx.retry_timeout = timeout;
        Self {
            core: ExecCore::Client(ctx),
            tx_options,
        }
    }

    pub fn clone_with_no_retry(&self) -> Self {
        let Self { core, tx_options } = self.clone();
        let ExecCore::Client(mut ctx) = core else {
            return self.clone();
        };
        ctx.max_attempts = 1;
        Self {
            core: ExecCore::Client(ctx),
            tx_options,
        }
    }

    pub fn clone_with_session_mode(&self, session_mode: QuerySessionMode) -> Self {
        let Self { core, tx_options } = self.clone();
        let ExecCore::Client(mut ctx) = core else {
            return self.clone();
        };
        ctx.session_mode = session_mode;
        Self {
            core: ExecCore::Client(ctx),
            tx_options,
        }
    }

    pub async fn retry_transaction<T>(
        &self,
        mut callback: impl AsyncFnMut(&mut QueryTransaction) -> YdbResultWithCustomerErr<T>,
    ) -> YdbResultWithCustomerErr<T> {
        let ExecCore::Client(client_ctx) = &self.core else {
            return Err(YdbOrCustomerError::YDB(YdbError::Custom(
                "invalid query client state".to_string(),
            )));
        };
        let idempotent = client_ctx.idempotent_operation;
        let max_attempts = client_ctx.max_attempts;
        let retry_timeout = client_ctx.retry_timeout;
        let mut attempt = 0;

        loop {
            attempt += 1;
            let mut tx = QueryTransaction::new(
                client_ctx.connection_manager.clone(),
                client_ctx.timeouts,
                client_ctx.session_mode,
                self.tx_options.clone(),
            );

            let err = match callback(&mut tx).await {
                Ok(value) => {
                    if tx.state == TxState::RolledBack {
                        return Ok(value);
                    }
                    match tx.commit().await {
                        Ok(()) => return Ok(value),
                        Err(e) => YdbOrCustomerError::YDB(e),
                    }
                }
                Err(err) => {
                    tx.rollback_quiet().await;
                    err
                }
            };

            if !check_retry_error(idempotent, &err) || attempt >= max_attempts {
                return Err(err);
            }
            sleep(backoff(retry_timeout, attempt)).await;
        }
    }
}

impl HasCore for QueryClient {
    fn core_mut(&mut self) -> &mut ExecCore {
        &mut self.core
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
    core: ExecCore,
    state: TxState,
}

impl QueryTransaction {
    impl_query_methods!();

    fn new(
        connection_manager: GrpcConnectionManager,
        timeouts: TimeoutSettings,
        session_mode: QuerySessionMode,
        options: QueryTransactionOptions,
    ) -> Self {
        Self {
            core: ExecCore::Transaction(transaction_exec_context(
                connection_manager,
                timeouts,
                session_mode,
                options,
            )),
            state: TxState::Active,
        }
    }

    pub fn mode(&self) -> QueryTxMode {
        match &self.core {
            ExecCore::Transaction(ctx) => ctx.tx_mode,
            ExecCore::Client(_) => QueryTxMode::default(),
        }
    }

    pub async fn rollback(&mut self) -> YdbResult<()> {
        if self.state != TxState::Active {
            return Err(YdbError::Custom("transaction already finished".to_string()));
        }
        transaction_rollback(self.tx_ctx_mut()).await?;
        self.state = TxState::RolledBack;
        Ok(())
    }

    async fn commit(&mut self) -> YdbResult<()> {
        transaction_commit(self.tx_ctx_mut()).await?;
        self.state = TxState::Committed;
        Ok(())
    }

    async fn rollback_quiet(&mut self) {
        if self.state == TxState::Active {
            let _ = transaction_rollback(self.tx_ctx_mut()).await;
            self.state = TxState::RolledBack;
        }
    }

    fn tx_ctx_mut(&mut self) -> &mut TransactionExecContext {
        match &mut self.core {
            ExecCore::Transaction(ctx) => ctx,
            ExecCore::Client(_) => panic!("transaction state expected"),
        }
    }
}

impl HasCore for QueryTransaction {
    fn core_mut(&mut self) -> &mut ExecCore {
        &mut self.core
    }
}

impl QueryExecutor for QueryTransaction {}
