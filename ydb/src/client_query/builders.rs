use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

use crate::errors::{YdbError, YdbResult};
use crate::result::{ResultSet, Row};
use crate::types::Value;
use crate::QuerySessionMode;

use super::exec::CallOptions;
use super::internal::{ExecCoreRef, HasCore};
use super::stream_facade::{materialize_query, QueryStream};
use super::FromYdbRow;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

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
    core: ExecCoreRef<'a>,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
    _kind: PhantomData<fn() -> K>,
}

impl<'a, K> CallBuilder<'a, K> {
    pub(crate) fn new(core: ExecCoreRef<'a>, text: String) -> Self {
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

    /// Per-call operation timeout.
    ///
    /// For one-shot calls (`exec`, `query_row`, …) this limits the full RPC.
    /// For [`QueryStream`](Self) the timeout applies only while opening the gRPC
    /// stream; iterating result sets is not bounded by this value.
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

    /// Commit the interactive transaction as part of this query (`commit_tx: true` in Query Service).
    ///
    /// Only meaningful on [`QueryTransaction`]: the server commits when the full response stream
    /// is consumed. A later query in the same transaction fails; [`QueryClient::retry_transaction`]
    /// treats the implicit commit as success (explicit `commit` is a no-op).
    pub fn with_commit(mut self) -> Self {
        self.opts.with_commit = true;
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
    ///
    /// Pool mode is not implemented yet and currently returns a runtime error.
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

impl<'a> IntoFuture for CallBuilder<'a, ExecCall> {
    type Output = YdbResult<()>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            materialize_query(&mut self.core, self.text, self.params, self.opts).await?;
            Ok(())
        })
    }
}

impl<'a, T: FromYdbRow + 'a> IntoFuture for CallBuilder<'a, OneRow<T>> {
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

impl<'a, T: FromYdbRow + 'a> IntoFuture for CallBuilder<'a, OptionalRow<T>> {
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

impl<'a> IntoFuture for CallBuilder<'a, OneResultSet> {
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

impl<'a> IntoFuture for CallBuilder<'a, Streamed> {
    type Output = YdbResult<QueryStream<'a>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            let with_commit = self.opts.with_commit;
            let stream = self
                .core
                .begin_stream(self.text, self.params, self.opts)
                .await?;
            Ok(QueryStream {
                core: self.core,
                stream,
                with_commit,
            })
        })
    }
}

/// Query execution entry points for [`QueryClient`](crate::QueryClient) and
/// [`QueryTransaction`](crate::QueryTransaction).
///
/// One-shot helpers are layered on [`Self::query`]:
///
/// - [`Self::query`] — streaming response (lazy via [`QueryStream`])
/// - [`Self::query_result_set`] — `query` + drain + exactly one result set
/// - [`Self::query_row`] — `query_result_set` + at most one row
/// - [`Self::exec`] — `query` + drain and discard (success = no error)
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

pub(crate) use impl_query_methods;
