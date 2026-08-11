use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_util::Stream;
use futures_util::stream::FusedStream;

use crate::closure;
use crate::errors::{YdbError, YdbResult};
use crate::grpc_wrapper::raw_query_service::execute_query::append_result_set_part;
use crate::grpc_wrapper::raw_query_service::stream::{ExecuteQueryStream, RawQueryResultPart};
use crate::grpc_wrapper::raw_query_service::transaction_control::TransactionId;
use crate::grpc_wrapper::raw_table_service::value::RawResultSet;
use crate::result::ResultSet;
use crate::types::Value;

use super::exec::{
    CallOptions, ClientExecContext, ClientQuerySession, OpenedClientQueryStream,
    TransactionExecContext, apply_stream_tx_id, client_begin_stream_once, transaction_begin_stream,
    transaction_finish_query,
};
use super::internal::ExecCoreRef;

/// One chunk of a logical result set returned by YDB.
///
/// A query may produce multiple result sets, and each result set may be split into multiple parts.
/// Parts are identified by [`Self::result_set_index`] and may be interleaved when concurrent result
/// sets are enabled.
#[derive(Debug)]
pub struct QueryResultPart {
    result_set_index: i64,
    result_set: ResultSet,
}

impl QueryResultPart {
    /// Zero-based index of the logical result set produced by the query.
    pub fn result_set_index(&self) -> i64 {
        self.result_set_index
    }

    /// Rows and column metadata carried by this response part.
    pub fn result_set(&self) -> &ResultSet {
        &self.result_set
    }

    /// Consume the part and return its rows and column metadata.
    pub fn into_result_set(self) -> ResultSet {
        self.result_set
    }
}

impl TryFrom<RawQueryResultPart> for QueryResultPart {
    type Error = YdbError;

    fn try_from(part: RawQueryResultPart) -> YdbResult<Self> {
        Ok(Self {
            result_set_index: part.result_set_index,
            result_set: ResultSet::try_from(part.result_set)?,
        })
    }
}

/// Streaming query result parts.
///
/// Poll the stream until it returns `None` to confirm successful query completion and release its
/// session. [`Self::finish`] drains unread parts when their rows are not needed. Dropping the
/// stream earlier cancels the gRPC call and discards or invalidates the session.
#[must_use = "QueryStream must be fully consumed or finished"]
pub struct QueryStream<'a> {
    stream: ExecuteQueryStream,
    lifecycle: QueryStreamLifecycle<'a>,
}

enum QueryStreamLifecycle<'a> {
    Active(QueryStreamOwner<'a>),
    Finished,
}

enum QueryStreamOwner<'a> {
    Client(ClientQuerySession),
    Transaction {
        context: &'a mut TransactionExecContext,
        commit_at_end: bool,
    },
}

impl Drop for QueryStream<'_> {
    fn drop(&mut self) {
        self.cancel_active(None);
    }
}

impl Stream for QueryStream<'_> {
    type Item = YdbResult<QueryResultPart>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match ready!(this.poll_next_raw(cx)) {
            Some(Ok(part)) => match QueryResultPart::try_from(part) {
                Ok(part) => Poll::Ready(Some(Ok(part))),
                Err(err) => {
                    this.terminate_with_error(&err);
                    Poll::Ready(Some(Err(err)))
                }
            },
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => Poll::Ready(None),
        }
    }
}

impl FusedStream for QueryStream<'_> {
    fn is_terminated(&self) -> bool {
        matches!(self.lifecycle, QueryStreamLifecycle::Finished)
    }
}

impl QueryStream<'_> {
    fn poll_next_raw(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<YdbResult<RawQueryResultPart>>> {
        match ready!(Pin::new(&mut self.stream).poll_next(cx)) {
            Some(Ok(part)) => {
                self.apply_captured_transaction_id();
                Poll::Ready(Some(Ok(part)))
            }
            Some(Err(err)) => {
                let error = YdbError::from(err);
                self.terminate_with_error(&error);
                Poll::Ready(Some(Err(error)))
            }
            None => {
                self.apply_captured_transaction_id();
                match self.complete() {
                    Ok(()) => Poll::Ready(None),
                    Err(error) => Poll::Ready(Some(Err(error))),
                }
            }
        }
    }

    async fn next_raw(&mut self) -> YdbResult<Option<RawQueryResultPart>> {
        std::future::poll_fn(|cx| self.poll_next_raw(cx))
            .await
            .transpose()
    }

    fn cancel_active(&mut self, error: Option<YdbError>) {
        self.apply_captured_transaction_id();
        let completion_unconfirmed = self.stream.completion_unconfirmed();
        self.stream.cancel();
        let lifecycle = std::mem::replace(&mut self.lifecycle, QueryStreamLifecycle::Finished);
        if let QueryStreamLifecycle::Active(QueryStreamOwner::Transaction { context, .. }) =
            lifecycle
            && completion_unconfirmed
        {
            context.abort_unconfirmed(error.unwrap_or_else(|| {
                YdbError::Custom("query stream dropped before completion was confirmed".to_string())
            }));
        }
    }
}

impl<'a> QueryStream<'a> {
    pub(crate) fn from_client(opened: OpenedClientQueryStream) -> Self {
        Self {
            stream: opened.stream,
            lifecycle: QueryStreamLifecycle::Active(QueryStreamOwner::Client(opened.session)),
        }
    }

    pub(crate) fn from_transaction(
        stream: ExecuteQueryStream,
        context: &'a mut TransactionExecContext,
        commit_at_end: bool,
    ) -> Self {
        Self {
            stream,
            lifecycle: QueryStreamLifecycle::Active(QueryStreamOwner::Transaction {
                context,
                commit_at_end,
            }),
        }
    }

    fn apply_transaction_id(&mut self, transaction_id: Option<TransactionId>) {
        if let QueryStreamLifecycle::Active(QueryStreamOwner::Transaction { context, .. }) =
            &mut self.lifecycle
        {
            apply_stream_tx_id(context, transaction_id);
        }
    }

    fn apply_captured_transaction_id(&mut self) {
        let transaction_id = self.stream.take_captured_tx_id();
        self.apply_transaction_id(transaction_id);
    }

    fn terminate_with_error(&mut self, error: &YdbError) {
        if let QueryStreamLifecycle::Active(QueryStreamOwner::Transaction { context, .. }) =
            &mut self.lifecycle
        {
            context.apply_query_error(error);
        }
        // If the error did not already end the transaction, unconfirmed stream completion makes
        // its retained session unsafe to reuse.
        self.cancel_active(Some(error.clone()));
    }

    fn complete(&mut self) -> YdbResult<()> {
        let lifecycle = std::mem::replace(&mut self.lifecycle, QueryStreamLifecycle::Finished);
        match lifecycle {
            QueryStreamLifecycle::Active(QueryStreamOwner::Client(session)) => {
                session.complete();
                Ok(())
            }
            QueryStreamLifecycle::Active(QueryStreamOwner::Transaction {
                context,
                commit_at_end,
            }) => transaction_finish_query(context, commit_at_end),
            QueryStreamLifecycle::Finished => Ok(()),
        }
    }

    pub fn stats(&self) -> Option<QueryStats> {
        self.stream
            .stats()
            .map(|total_duration| QueryStats { total_duration })
    }

    /// Drain unread result parts and wait for successful query completion.
    ///
    /// This returns a pooled session only after YDB closes the response stream successfully.
    pub async fn finish(mut self) -> YdbResult<()> {
        while self.next_raw().await?.is_some() {}
        Ok(())
    }
}

/// Drain a [`query`](super::QueryExecutor::query) stream into materialized result sets.
///
/// Used by one-shot builders (`exec`, `query_result_set`, `query_row`) on both
/// [`QueryClient`](super::QueryClient) and [`Transaction`](super::Transaction).
///
/// On [`QueryClient`], the full open+drain+close cycle is retried on retryable errors.
/// Interactive transactions are materialized once since tx retries are owned by [`QueryClient::retry_tx`] loop.
pub(crate) async fn materialize_query(
    core: &mut ExecCoreRef<'_>,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
) -> YdbResult<Vec<ResultSet>> {
    let commit_at_end = opts.commit_tx;
    match core {
        ExecCoreRef::Client(ctx) => {
            ctx.retry_settings
                .clone()
                .with_deadline(opts.timeout)
                .retry_on_retriable_errors(
                    opts.idempotency,
                    closure!([&ctx, &text, &params, &opts], |_| {
                        materialize_client_once(ctx, text, params, opts)
                    }),
                )
                .await
        }
        ExecCoreRef::Transaction(context) => {
            materialize_transaction_once(context, text, params, opts, commit_at_end).await
        }
    }
}

async fn materialize_client_once(
    ctx: &ClientExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
) -> YdbResult<Vec<ResultSet>> {
    let opened = client_begin_stream_once(ctx, text, params, opts, true).await?;
    materialize_stream(QueryStream::from_client(opened)).await
}

async fn materialize_transaction_once(
    context: &mut TransactionExecContext,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
    commit_at_end: bool,
) -> YdbResult<Vec<ResultSet>> {
    let stream = transaction_begin_stream(context, text, params, opts, true).await?;
    materialize_stream(QueryStream::from_transaction(
        stream,
        context,
        commit_at_end,
    ))
    .await
}

#[derive(Default)]
struct PartialResultSet {
    columns: Vec<crate::grpc_wrapper::raw_table_service::value::RawColumn>,
    rows: Vec<Vec<crate::grpc_wrapper::raw_table_service::value::RawValue>>,
    truncated: bool,
}

async fn materialize_stream(mut stream: QueryStream<'_>) -> YdbResult<Vec<ResultSet>> {
    let mut by_index: BTreeMap<i64, PartialResultSet> = BTreeMap::new();
    while let Some(part) = stream.next_raw().await? {
        let partial = by_index.entry(part.result_set_index).or_default();
        append_result_set_part(
            &mut partial.columns,
            &mut partial.rows,
            &mut partial.truncated,
            part.result_set,
        )?;
    }

    by_index
        .into_values()
        .map(|partial| {
            ResultSet::try_from(RawResultSet {
                columns: partial.columns,
                rows: partial.rows,
                truncated: partial.truncated,
            })
        })
        .collect()
}

#[derive(Debug, Default)]
pub struct QueryStats {
    pub total_duration: Duration,
}
