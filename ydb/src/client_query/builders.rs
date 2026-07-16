use std::collections::HashMap;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::time::Duration;

use crate::TxMode;
use crate::errors::{YdbError, YdbResult};
use crate::result::{ResultSet, Row};
use crate::types::Value;

use super::FromYdbRow;
use super::exec::{CallOptions, ClientExecContext, TransactionExecContext, resolve_commit_tx};
use super::internal::ExecCoreRef;
use super::stream_facade::{QueryStream, materialize_query};

pub enum ExecCall {}
pub struct OneRow<T>(PhantomData<T>);
pub struct OptionalRow<T>(PhantomData<T>);
pub enum OneResultSet {}
pub enum Streamed {}

/// One-shot [`QueryClient`] calls (`exec`, `query_row`, …).
pub struct ClientOneShot;
/// Calls inside [`Transaction`] (`retry_tx` callback).
pub struct Interactive;

pub type ExecBuilder<'a, S = ClientOneShot> = CallBuilder<'a, ExecCall, S>;
pub type QueryRowBuilder<'a, T = Row, S = ClientOneShot> = CallBuilder<'a, OneRow<T>, S>;
pub type OptionalRowBuilder<'a, T = Row, S = ClientOneShot> = CallBuilder<'a, OptionalRow<T>, S>;
pub type ResultSetBuilder<'a, S = ClientOneShot> = CallBuilder<'a, OneResultSet, S>;
pub type QueryStreamBuilder<'a, S = ClientOneShot> = CallBuilder<'a, Streamed, S>;

pub struct CallBuilder<'a, K, S = ClientOneShot> {
    core: ExecCoreRef<'a>,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
    _kind: PhantomData<fn() -> K>,
    _scope: PhantomData<S>,
}

impl<'a, K> CallBuilder<'a, K, ClientOneShot> {
    pub(crate) fn new_client(ctx: &'a mut ClientExecContext, text: String) -> Self {
        Self {
            core: ExecCoreRef::Client(ctx),
            text,
            params: HashMap::new(),
            opts: CallOptions::default(),
            _kind: PhantomData,
            _scope: PhantomData,
        }
    }

    /// Execute with an empty `session_id` (server-side implicit session) instead of
    /// leasing from the driver pool.
    pub fn with_implicit_session(mut self) -> Self {
        self.opts.implicit_session = true;
        self
    }
}

impl<'a, K> CallBuilder<'a, K, Interactive> {
    pub(crate) fn new_transaction(ctx: &'a mut TransactionExecContext, text: String) -> Self {
        Self {
            core: ExecCoreRef::Transaction(ctx),
            text,
            params: HashMap::new(),
            opts: CallOptions::default(),
            _kind: PhantomData,
            _scope: PhantomData,
        }
    }
}

impl<'a, K, S> CallBuilder<'a, K, S> {
    fn into_kind<K2>(self) -> CallBuilder<'a, K2, S> {
        CallBuilder {
            core: self.core,
            text: self.text,
            params: self.params,
            opts: self.opts,
            _kind: PhantomData,
            _scope: PhantomData,
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

    /// Wall-clock limit for the call. Retries transient errors until this deadline when
    /// combined with [`.idempotent(true)`](Self::idempotent). Without `.timeout()`, retries
    /// continue until a non-retryable error.
    ///
    /// For [`QueryStream`](Self) the timeout bounds opening the gRPC stream and any retries;
    /// iterating result sets is not bounded by this value.
    ///
    /// Inside [`retry_tx`](crate::QueryClient::retry_tx), per-call `.timeout()` is capped by
    /// the remaining `retry_tx` deadline.
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

    /// Override auto-commit (`commit_tx` in Query Service `TxControl`).
    ///
    /// One-shot defaults depend on [`Self::with_tx_mode`]: implicit mode relies on the server;
    /// explicit modes default to `commit_tx: true`. Interactive transactions default to
    /// `commit_tx: false` unless [`Self::with_commit(true)`] is set on the last query.
    ///
    /// When using [`Self::query`] with `with_commit(true)` inside a transaction, you must
    /// fully drain the stream and call [`QueryStream::close`] — dropping the stream early
    /// cancels the gRPC call and does not commit.
    pub fn with_commit(mut self, commit: bool) -> Self {
        self.opts.commit_tx = Some(commit);
        self
    }

    /// Set transaction isolation for this call.
    ///
    /// Default on [`QueryClient`] is [`TxMode::Implicit`] (no `tx_control`; the server
    /// infers isolation from the SQL). Interactive transactions use the mode from
    /// [`TransactionOptions`] unless overridden here.
    ///
    /// [`TxMode::Implicit`] inside [`Transaction`] returns a runtime error — DDL and
    /// other non-transactional statements must run on [`QueryClient`], not inside a transaction.
    pub fn with_tx_mode(mut self, mode: TxMode) -> Self {
        self.opts.tx_mode = Some(mode);
        self
    }

    /// Shorthand for [`Self::with_tx_mode`](TxMode::Implicit) (ImplicitTx / NoTx).
    ///
    /// [`TxMode::Implicit`] inside [`Transaction`] returns a runtime error — DDL and
    /// other non-transactional statements must run on [`QueryClient`], not inside a transaction.
    pub fn implicit_tx(self) -> Self {
        self.with_tx_mode(TxMode::Implicit)
    }
}

impl<'a, T, S> CallBuilder<'a, OneRow<T>, S> {
    pub fn typed<U: FromYdbRow>(self) -> CallBuilder<'a, OneRow<U>, S> {
        self.into_kind()
    }

    pub fn optional(self) -> CallBuilder<'a, OptionalRow<T>, S> {
        self.into_kind()
    }
}

pub(crate) fn exactly_one_set(mut sets: Vec<ResultSet>) -> YdbResult<ResultSet> {
    match sets.len() {
        0 => Err(YdbError::Custom("expected 1 result set, got 0".to_string())),
        1 => Ok(sets.pop().expect("len checked")),
        count => Err(YdbError::Custom(format!(
            "expected 1 result set, got {count}"
        ))),
    }
}

pub(crate) fn take_single_row(result_set: ResultSet) -> YdbResult<Option<Row>> {
    let mut rows = result_set.into_iter();
    let row = rows.next();
    if rows.next().is_some() {
        return Err(YdbError::Custom(
            "expected at most 1 row in result set, got more".to_string(),
        ));
    }
    Ok(row)
}

impl<'a, S> IntoFuture for CallBuilder<'a, ExecCall, S> {
    type Output = YdbResult<()>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            materialize_query(&mut self.core, self.text, self.params, self.opts).await?;
            Ok(())
        })
    }
}

impl<'a, T: FromYdbRow + 'a, S> IntoFuture for CallBuilder<'a, OneRow<T>, S> {
    type Output = YdbResult<T>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            let set = exactly_one_set(
                materialize_query(&mut self.core, self.text, self.params, self.opts).await?,
            )?;
            let row = take_single_row(set)?.ok_or(YdbError::NoRows)?;
            T::from_row(row)
        })
    }
}

impl<'a, T: FromYdbRow + 'a, S> IntoFuture for CallBuilder<'a, OptionalRow<T>, S> {
    type Output = YdbResult<Option<T>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            let set = exactly_one_set(
                materialize_query(&mut self.core, self.text, self.params, self.opts).await?,
            )?;
            take_single_row(set)?.map(T::from_row).transpose()
        })
    }
}

impl<'a, S> IntoFuture for CallBuilder<'a, OneResultSet, S> {
    type Output = YdbResult<ResultSet>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            exactly_one_set(
                materialize_query(&mut self.core, self.text, self.params, self.opts).await?,
            )
        })
    }
}

impl<'a, S> IntoFuture for CallBuilder<'a, Streamed, S> {
    type Output = YdbResult<QueryStream<'a>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            let commit_tx = resolve_commit_tx(&self.core, &self.opts);
            let stream = self
                .core
                .begin_stream(self.text, self.params, self.opts, false)
                .await?;
            Ok(QueryStream {
                core: self.core,
                stream,
                commit_tx,
            })
        })
    }
}

/// Query execution entry points for [`QueryClient`](crate::QueryClient) and
/// [`Transaction`](crate::Transaction).
///
/// One-shot helpers are layered on [`Self::query`]:
///
/// - [`Self::query`] — streaming response (lazy via [`QueryStream`])
/// - [`Self::query_result_set`] — `query` + drain + exactly one result set
/// - [`Self::query_row`] — `query_result_set` + at most one row
/// - [`Self::exec`] — `query` + drain and discard (success = no error)
pub trait QueryExecutor {
    type Scope;
    fn exec(&mut self, text: impl Into<String>) -> ExecBuilder<'_, Self::Scope>;
    fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_, Self::Scope>;
    fn query_result_set(&mut self, text: impl Into<String>) -> ResultSetBuilder<'_, Self::Scope>;
    fn query_row(&mut self, text: impl Into<String>) -> QueryRowBuilder<'_, Row, Self::Scope>;
}

macro_rules! impl_client_query_methods {
    () => {
        pub fn exec(&mut self, text: impl Into<String>) -> ExecBuilder<'_, ClientOneShot> {
            CallBuilder::new_client(&mut self.ctx, text.into())
        }

        pub fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_, ClientOneShot> {
            CallBuilder::new_client(&mut self.ctx, text.into())
        }

        pub fn query_result_set(
            &mut self,
            text: impl Into<String>,
        ) -> ResultSetBuilder<'_, ClientOneShot> {
            CallBuilder::new_client(&mut self.ctx, text.into())
        }

        pub fn query_row(
            &mut self,
            text: impl Into<String>,
        ) -> QueryRowBuilder<'_, Row, ClientOneShot> {
            CallBuilder::new_client(&mut self.ctx, text.into())
        }
    };
}

macro_rules! impl_transaction_query_methods {
    () => {
        pub fn exec(&mut self, text: impl Into<String>) -> ExecBuilder<'_, Interactive> {
            CallBuilder::new_transaction(&mut self.ctx, text.into())
        }

        pub fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_, Interactive> {
            CallBuilder::new_transaction(&mut self.ctx, text.into())
        }

        pub fn query_result_set(
            &mut self,
            text: impl Into<String>,
        ) -> ResultSetBuilder<'_, Interactive> {
            CallBuilder::new_transaction(&mut self.ctx, text.into())
        }

        pub fn query_row(
            &mut self,
            text: impl Into<String>,
        ) -> QueryRowBuilder<'_, Row, Interactive> {
            CallBuilder::new_transaction(&mut self.ctx, text.into())
        }
    };
}

use futures_util::future::BoxFuture;
pub(crate) use impl_client_query_methods;
pub(crate) use impl_transaction_query_methods;
