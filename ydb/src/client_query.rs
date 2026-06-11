//! PROTOTYPE of the Query Service public facade
//! (<https://github.com/ydb-platform/ydb-rs-sdk/issues/207>).
//!
//! Interface only: execution paths return a "not implemented" error. The
//! goal is to fix the public API shape and the borrow-checker guarantees;
//! the gRPC layer (`grpc_wrapper/raw_query_service`) comes later.
//!
//! BEFORE changing this module read `.agents/context/query-service-design.md`
//! — it records the design invariants (and *why*) so the implementation does
//! not silently regress them.
//!
//! Requires Rust 1.85+ (`AsyncFnMut`) — pending the MSRV bump decision.
//!
//! Key decisions encoded here:
//!
//! - [`QueryClient::retry_transaction`] takes
//!   `AsyncFnMut(&mut QueryTransaction)`: the callback borrows the
//!   environment naturally (including mutably), no `async move` /
//!   `let mut t = t;` / manual clone-per-attempt as in the table API.
//! - Commit on `Ok`, rollback on `Err`; explicit [`QueryTransaction::rollback`]
//!   finishes the transaction without an error (no commit, no retry).
//! - Query methods are sync and return awaitable builders ([`IntoFuture`]):
//!   `tx.query_row("...").param("$id", 1_i64).await?`. There is no separate
//!   statement type: the builder is the statement.
//! - All five builders are one generic [`CallBuilder`]`<'a, K>` where `K`
//!   marks the result shape; the option methods are written once, the
//!   `IntoFuture` impls differ per `K`. Public names are type aliases.
//!   (`'a` is only the executor borrow — see [`QueryStream`].)
//! - No conversion traits and no data lifetimes: methods take
//!   `impl Into<String>` for text and `impl Into<Value>` for params, owned.
//!   The builder is consumed by `.await`, so an owned `Value` passed by
//!   value is *moved*, not copied — borrowing bought nothing here, because
//!   the gRPC request needs an owned `String` / protobuf value at the end
//!   anyway. `Into<Value>` also means a new `Value` conversion is a valid
//!   parameter automatically — no list to maintain. The one case where
//!   borrowing would have saved a copy (a big `Value` you must keep, reused
//!   in a hot loop) is an explicit `.clone()` at the call site.
//! - The query surface (`exec` / `query` / `query_result_set` / `query_row`)
//!   exists twice with identical signatures: as **inherent** methods on both
//!   [`QueryClient`] (one-shot calls, retried internally) and
//!   [`QueryTransaction`] — for IDE completion with no `use` import — and as
//!   the sealed [`QueryExecutor`] trait, for external code generic over
//!   client/transaction (ORM adapters). Inherent methods forward to the
//!   trait and win method resolution, so importing the trait never changes a
//!   direct call.
//! - Strict `query_row` (exactly one row), `.optional()` for 0-or-1,
//!   `.typed::<T>()` for struct mapping (derive macro later).
//! - [`QueryStream`] borrows the executor: it cannot leak out of a retry
//!   attempt, and a second concurrent query on one transaction does not
//!   compile.

use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

use tokio::time::sleep;

use crate::errors::{NeedRetry, YdbError, YdbOrCustomerError, YdbResult, YdbResultWithCustomerErr};
use crate::result::{ResultSet, Row};
use crate::types::Value;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Row-to-struct mapping (the sqlx `FromRow` analogue). The real
/// implementation would ship `#[derive(FromYdbRow)]`.
pub trait FromYdbRow: Sized {
    fn from_row(row: Row) -> YdbResult<Self>;
}

impl FromYdbRow for Row {
    fn from_row(row: Row) -> YdbResult<Self> {
        Ok(row)
    }
}

// ---------------------------------------------------------------------------
// Crate-private execution core (seals QueryExecutor)
// ---------------------------------------------------------------------------

mod private {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::{ResultSet, Value, YdbError, YdbResult};

    /// In the real implementation this wraps session acquisition and the
    /// `ExecuteQuery` gRPC streaming machinery. For `QueryClient` it also
    /// retries one-shot calls internally.
    #[derive(Clone, Debug, Default)]
    pub struct ExecCore {
        pub kind: &'static str,
        /// Set after commit/rollback: further queries must fail.
        pub finished: bool,
    }

    impl ExecCore {
        pub async fn run(
            &mut self,
            text: &str,
            params: &HashMap<String, Value>,
            opts: &CallOptions,
        ) -> YdbResult<Vec<ResultSet>> {
            if self.finished {
                return Err(YdbError::Custom(
                    "transaction already finished (committed or rolled back)".to_string(),
                ));
            }
            Err(YdbError::Custom(format!(
                "prototype ({}): execution is not implemented; would run {:?} \
                 with {} param(s), timeout={:?}, idempotent={:?}, collect_stats={}",
                self.kind,
                text,
                params.len(),
                opts.timeout,
                opts.idempotent,
                opts.collect_stats,
            )))
        }
    }

    /// Per-call options, filled by builder methods before `.await`.
    #[derive(Clone, Debug, Default)]
    pub struct CallOptions {
        pub timeout: Option<Duration>,
        /// Per-call override of the client-level idempotency flag
        /// (the scylla `set_is_idempotent` / Go `query.WithIdempotent()`
        /// analogue).
        pub idempotent: Option<bool>,
        pub collect_stats: bool,
    }

    /// Seals [`super::QueryExecutor`] and gives its default methods access to
    /// the execution core. Not nameable outside the crate, so the trait
    /// cannot be implemented for foreign types.
    pub trait HasCore {
        fn core_mut(&mut self) -> &mut ExecCore;
    }
}

use private::{CallOptions, ExecCore, HasCore};

// ---------------------------------------------------------------------------
// The builder
// ---------------------------------------------------------------------------

/// Result-shape markers for [`CallBuilder`]. Never instantiated.
pub enum ExecCall {}
pub struct OneRow<T>(PhantomData<T>);
pub struct OptionalRow<T>(PhantomData<T>);
pub enum OneResultSet {}
pub enum Streamed {}

/// Builder for [`QueryExecutor::exec`]: DML/DDL without result rows.
pub type ExecBuilder<'a> = CallBuilder<'a, ExecCall>;
/// Builder for [`QueryExecutor::query_row`]: exactly one row.
pub type QueryRowBuilder<'a, T = Row> = CallBuilder<'a, OneRow<T>>;
/// See [`CallBuilder::optional`].
pub type OptionalRowBuilder<'a, T = Row> = CallBuilder<'a, OptionalRow<T>>;
/// Builder for [`QueryExecutor::query_result_set`]: exactly one set.
pub type ResultSetBuilder<'a> = CallBuilder<'a, OneResultSet>;
/// Builder for [`QueryExecutor::query`]: streaming result.
pub type QueryStreamBuilder<'a> = CallBuilder<'a, Streamed>;

/// One awaitable call. `K` marks the result shape (one row, one result set,
/// stream, ...): the option methods below are shared, the `IntoFuture`
/// impls — and so the type produced by `.await` — differ per `K`.
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

    /// Bind a parameter for this call. An owned `Value` (or anything
    /// `Into<Value>`) is moved in — pass `value.clone()` if you must keep it.
    pub fn param(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.params.insert(name.into(), value.into());
        self
    }

    /// Bind many parameters at once (works with the `ydb_params!` macro).
    pub fn params(mut self, params: HashMap<String, Value>) -> Self {
        self.params.extend(params);
        self
    }

    /// Per-call operation timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.opts.timeout = Some(timeout);
        self
    }

    /// Per-call idempotency override (affects which errors are retried).
    pub fn idempotent(mut self, idempotent: bool) -> Self {
        self.opts.idempotent = Some(idempotent);
        self
    }

    /// Request execution stats with the result.
    pub fn collect_stats(mut self) -> Self {
        self.opts.collect_stats = true;
        self
    }
}

impl<'a, T> CallBuilder<'a, OneRow<T>> {
    /// Map the row into `U` (the sqlx `query_as` analogue).
    pub fn typed<U: FromYdbRow>(self) -> CallBuilder<'a, OneRow<U>> {
        self.into_kind()
    }

    /// 0 rows -> `Ok(None)` instead of `Err(NoRows)` (the sqlx
    /// `fetch_optional` analogue). More than one row is still an error.
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
            // Real impl: send ExecuteQuery and obtain the gRPC stream here.
            Ok(QueryStream {
                core: self.core,
                text: self.text,
                params: self.params,
                opts: self.opts,
            })
        })
    }
}

/// Streaming result of `ExecuteQuery`. Borrows the executor mutably: it
/// cannot outlive a retry attempt, and a second query on the same
/// transaction while the stream is alive does not compile.
///
/// `ExecuteQueryResponsePart` is hidden: parts are assembled into logical
/// [`ResultSet`]s by `result_set_index` internally.
pub struct QueryStream<'a> {
    core: &'a mut ExecCore,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
}

impl QueryStream<'_> {
    /// Next result set; `Ok(None)` means the stream is exhausted.
    pub async fn next_result_set(&mut self) -> YdbResult<Option<ResultSet>> {
        // Prototype plumbing only.
        let mut sets = self.core.run(&self.text, &self.params, &self.opts).await?;
        Ok(sets.pop())
    }

    /// Stats, if requested via `collect_stats()` and the stream is finished.
    pub fn stats(&self) -> Option<&QueryStats> {
        None
    }

    /// Explicitly cancel/close the underlying gRPC stream and release the
    /// borrow of the executor. `Drop` does a synchronous best-effort cancel
    /// (async Drop is not available in stable Rust).
    pub async fn close(self) -> YdbResult<()> {
        Ok(())
    }
}

/// Execution statistics (placeholder).
#[derive(Debug, Default)]
pub struct QueryStats {
    pub total_duration: Duration,
}

// ---------------------------------------------------------------------------
// Query surface
// ---------------------------------------------------------------------------

/// The shared query surface of [`QueryClient`] (one-shot calls with internal
/// retries) and [`QueryTransaction`] (inside
/// [`QueryClient::retry_transaction`]).
///
/// Sealed: implemented only by SDK types. Use it as a bound
/// (`&mut impl QueryExecutor`) to write code generic over the client and a
/// transaction — e.g. an ORM adapter or a shared helper:
///
/// ```ignore
/// async fn fetch_sum(e: &mut impl QueryExecutor) -> YdbResult<i64> {
///     let mut row = e.query_row("SELECT SUM(id) AS s FROM test").await?;
///     row.remove_field_by_name("s")?.try_into()
/// }
/// ```
///
/// For direct calls (`tx.exec(...)`) the **inherent** methods of the same
/// name are used instead — they autocomplete in an IDE with no `use
/// ydb::QueryExecutor` import. The inherent methods and these trait methods
/// have identical signatures and behaviour; inherent methods win method
/// resolution, so importing the trait never changes a direct call.
///
/// async-fn-in-trait would make this trait not dyn-compatible regardless, so
/// generic bounds (not `dyn QueryExecutor`) are the intended use.
pub trait QueryExecutor: HasCore {
    /// DML/DDL without result rows.
    fn exec(&mut self, text: impl Into<String>) -> ExecBuilder<'_> {
        CallBuilder::new(self.core_mut(), text.into())
    }

    /// Streaming result; the primary path for big data / multi result sets.
    fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_> {
        CallBuilder::new(self.core_mut(), text.into())
    }

    /// Materialize exactly one result set (error on 0 or >1).
    fn query_result_set(&mut self, text: impl Into<String>) -> ResultSetBuilder<'_> {
        CallBuilder::new(self.core_mut(), text.into())
    }

    /// Materialize exactly one row of exactly one result set
    /// (0 rows -> [`YdbError::NoRows`], >1 -> error). See `.optional()` and
    /// `.typed()` on the returned builder.
    fn query_row(&mut self, text: impl Into<String>) -> QueryRowBuilder<'_, Row> {
        CallBuilder::new(self.core_mut(), text.into())
    }
}

/// Generates the **inherent** copies of the [`QueryExecutor`] methods on a
/// concrete type. They forward to the trait, so behaviour has one source —
/// but being inherent they autocomplete in an IDE without importing the
/// trait, and win method resolution for direct calls. Rust has no
/// inheritance of inherent methods, hence the macro over two types.
macro_rules! impl_query_methods {
    () => {
        /// DML/DDL without result rows.
        pub fn exec(&mut self, text: impl Into<String>) -> ExecBuilder<'_> {
            QueryExecutor::exec(self, text)
        }

        /// Streaming result; the primary path for big data / multi result sets.
        pub fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_> {
            QueryExecutor::query(self, text)
        }

        /// Materialize exactly one result set (error on 0 or >1).
        pub fn query_result_set(&mut self, text: impl Into<String>) -> ResultSetBuilder<'_> {
            QueryExecutor::query_result_set(self, text)
        }

        /// Materialize exactly one row of exactly one result set
        /// (0 rows -> [`YdbError::NoRows`], >1 -> error). See `.optional()`
        /// and `.typed()` on the returned builder.
        pub fn query_row(&mut self, text: impl Into<String>) -> QueryRowBuilder<'_, Row> {
            QueryExecutor::query_row(self, text)
        }
    };
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Transaction mode for [`QueryClient::retry_transaction`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum QueryTxMode {
    #[default]
    SerializableReadWrite,
    SnapshotReadOnly,
    StaleReadOnly,
    OnlineReadOnly,
}

/// Options for transactions started by [`QueryClient::retry_transaction`].
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
}

/// Public facade of the Query Service. Cheap to clone; configured with
/// `clone_with_*` methods, mirroring [`crate::TableClient`].
///
/// One-shot [`QueryExecutor`] methods on the client acquire a session and
/// retry internally (the MongoDB retryable-reads/writes model: the best
/// retry is the one you don't see). After a `query()` stream has started,
/// errors are not retried — restart via [`Self::retry_transaction`] if
/// needed.
#[derive(Clone)]
pub struct QueryClient {
    core: ExecCore,
    tx_options: QueryTransactionOptions,
    idempotent_operation: bool,
    retry_timeout: Duration,
    max_attempts: usize,
}

impl QueryClient {
    impl_query_methods!();

    pub(crate) fn new() -> Self {
        Self {
            core: ExecCore {
                kind: "client one-shot",
                finished: false,
            },
            tx_options: QueryTransactionOptions::default(),
            idempotent_operation: false,
            retry_timeout: Duration::from_secs(5),
            max_attempts: 3,
        }
    }

    pub fn clone_with_idempotent_operations(&self, idempotent: bool) -> Self {
        Self {
            idempotent_operation: idempotent,
            ..self.clone()
        }
    }

    pub fn clone_with_transaction_options(&self, opts: QueryTransactionOptions) -> Self {
        Self {
            tx_options: opts,
            ..self.clone()
        }
    }

    pub fn clone_with_retry_timeout(&self, timeout: Duration) -> Self {
        Self {
            retry_timeout: timeout,
            ..self.clone()
        }
    }

    pub fn clone_with_no_retry(&self) -> Self {
        Self {
            max_attempts: 1,
            ..self.clone()
        }
    }

    /// Run `callback` inside a transaction, retrying the whole transaction
    /// on retryable errors (the Go `DoTx` analogue).
    ///
    /// - `Ok(_)` from the callback => commit (unless
    ///   [`QueryTransaction::rollback`] was called explicitly), the value is
    ///   returned;
    /// - `Err(_)` => rollback; retried per retry policy / idempotency;
    /// - [`YdbOrCustomerError::Customer`] is never retried.
    ///
    /// The callback is `AsyncFnMut` (Rust 1.85+): it may borrow the
    /// environment — including mutably (counters, query texts, parameters) —
    /// with no `async move` / manual clone-per-attempt dance.
    ///
    /// WARNING: the callback may run multiple times. Mutations of captured
    /// state made by a failed attempt persist; accumulate results inside the
    /// callback and return them via `Ok(...)` instead of pushing into a
    /// captured collection.
    pub async fn retry_transaction<T>(
        &self,
        mut callback: impl AsyncFnMut(&mut QueryTransaction) -> YdbResultWithCustomerErr<T>,
    ) -> YdbResultWithCustomerErr<T> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut tx = QueryTransaction::new(self.tx_options.clone());

            let err = match callback(&mut tx).await {
                Ok(value) => {
                    if tx.state == TxState::RolledBack {
                        // Explicit tx.rollback() in the callback:
                        // no commit, no retry, the value is the result.
                        return Ok(value);
                    }
                    match tx.commit().await {
                        Ok(()) => return Ok(value),
                        // Note for the real impl: commit outcome may be
                        // unknown on transport errors; retry only when safe.
                        Err(e) => YdbOrCustomerError::YDB(e),
                    }
                }
                Err(err) => {
                    tx.rollback_quiet().await;
                    err
                }
            };

            if !self.check_retry_error(&err) || attempt >= self.max_attempts {
                return Err(err);
            }
            sleep(self.backoff(attempt)).await;
        }
    }

    /// Same rules as `TableClient::check_retry_error`; the real impl reuses
    /// the shared `Retry` policy.
    fn check_retry_error(&self, err: &YdbOrCustomerError) -> bool {
        let ydb_err = match err {
            YdbOrCustomerError::Customer(_) => return false,
            YdbOrCustomerError::YDB(err) => err,
        };
        match ydb_err.need_retry() {
            NeedRetry::True => true,
            NeedRetry::IdempotentOnly => self.idempotent_operation,
            NeedRetry::False => false,
        }
    }

    fn backoff(&self, attempt: usize) -> Duration {
        (Duration::from_millis(10) * 2u32.pow(attempt.min(10) as u32)).min(self.retry_timeout)
    }
}

impl HasCore for QueryClient {
    fn core_mut(&mut self) -> &mut ExecCore {
        &mut self.core
    }
}

impl QueryExecutor for QueryClient {}

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
enum TxState {
    Active,
    Committed,
    RolledBack,
}

/// An interactive transaction, passed by `&mut` into
/// [`QueryClient::retry_transaction`] callbacks: it cannot escape the
/// attempt; commit/rollback on completion is driven by the SDK.
pub struct QueryTransaction {
    core: ExecCore,
    state: TxState,
    options: QueryTransactionOptions,
}

impl QueryTransaction {
    impl_query_methods!();

    fn new(options: QueryTransactionOptions) -> Self {
        Self {
            core: ExecCore {
                kind: "transaction",
                finished: false,
            },
            state: TxState::Active,
            options,
        }
    }

    pub fn mode(&self) -> QueryTxMode {
        self.options.mode
    }

    /// Finish the transaction without an error (the Spring
    /// `setRollbackOnly()` analogue): releases server resources immediately;
    /// subsequent queries fail; returning `Ok` from the callback after this
    /// neither commits nor retries.
    pub async fn rollback(&mut self) -> YdbResult<()> {
        if self.state != TxState::Active {
            return Err(YdbError::Custom("transaction already finished".to_string()));
        }
        // Real impl: RollbackTransaction RPC.
        self.state = TxState::RolledBack;
        self.core.finished = true;
        Ok(())
    }

    async fn commit(&mut self) -> YdbResult<()> {
        // Real impl: CommitTransaction RPC.
        self.state = TxState::Committed;
        self.core.finished = true;
        Ok(())
    }

    /// Best-effort rollback after a callback error.
    async fn rollback_quiet(&mut self) {
        if self.state == TxState::Active {
            self.state = TxState::RolledBack;
            self.core.finished = true;
        }
    }
}

impl HasCore for QueryTransaction {
    fn core_mut(&mut self) -> &mut ExecCore {
        &mut self.core
    }
}

impl QueryExecutor for QueryTransaction {}
