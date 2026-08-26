use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_util::stream::FusedStream;
use futures_util::{Stream, TryStreamExt};

use crate::closure;
use crate::errors::{YdbError, YdbResult};
use crate::grpc_wrapper::raw_query_service::stream::{ExecuteQueryStream, RawQueryResultPart};
use crate::grpc_wrapper::raw_table_service::value::RawResultSet;
use crate::result::ResultSet;
use crate::types::Value;

use super::exec::{
    CallOptions, ClientExecContext, ClientQuerySession, ExecTarget, OpenedClientQueryStream,
    TxExecContext, apply_stream_tx_id, client_begin_stream_once, resolve_commit_tx,
    tx_begin_stream, tx_cancel_query, tx_finish_query, tx_handle_query_error,
};
use super::result_set_cursor::ResultSetCursor;

/// A transaction query response exposed as a sequence of lazy logical result sets.
///
/// Call [`Self::next_result_set`] until it returns `None` to confirm successful response completion.
/// This makes an ordinary transaction ready for its next operation, or completes one configured
/// with `with_commit(true)`. [`Self::finish`] drains unread result sets when their rows are not
/// needed. Dropping the query stream earlier cancels the gRPC call and invalidates the transaction.
#[must_use = "QueryStream must be fully consumed or finished"]
pub struct QueryStream<'a> {
    cursor: ResultSetCursor<ManagedQueryParts<'a>>,
}

/// Turns the decoded gRPC response into a lifecycle-aware stream of result parts.
struct ManagedQueryParts<'a> {
    stream: ExecuteQueryStream,
    lifecycle: QueryStreamLifecycle<'a>,
}

enum QueryStreamLifecycle<'a> {
    Active(QueryStreamOwner<'a>),
    Finished,
}

enum QueryStreamOwner<'a> {
    Client(ClientQuerySession),
    Tx {
        context: &'a mut TxExecContext,
        commit_at_end: bool,
    },
}

impl Drop for ManagedQueryParts<'_> {
    fn drop(&mut self) {
        self.abort();
    }
}

impl Stream for ManagedQueryParts<'_> {
    type Item = YdbResult<RawQueryResultPart>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match ready!(Pin::new(&mut this.stream).poll_next(cx)) {
            Some(Ok(part)) => match this.apply_captured_transaction_id() {
                Ok(()) => Poll::Ready(Some(Ok(part))),
                Err(error) => {
                    let error = this.terminate_with_error(error);
                    Poll::Ready(Some(Err(error)))
                }
            },
            Some(Err(err)) => {
                let error = YdbError::from(err);
                let error = this.terminate_with_error(error);
                Poll::Ready(Some(Err(error)))
            }
            None => {
                if let Err(error) = this.apply_captured_transaction_id() {
                    let error = this.terminate_with_error(error);
                    return Poll::Ready(Some(Err(error)));
                }
                match this.complete() {
                    Ok(()) => Poll::Ready(None),
                    Err(error) => Poll::Ready(Some(Err(error))),
                }
            }
        }
    }
}

impl FusedStream for ManagedQueryParts<'_> {
    fn is_terminated(&self) -> bool {
        matches!(self.lifecycle, QueryStreamLifecycle::Finished)
    }
}

impl<'a> ManagedQueryParts<'a> {
    fn from_client(opened: OpenedClientQueryStream) -> Self {
        Self {
            stream: opened.stream,
            lifecycle: QueryStreamLifecycle::Active(QueryStreamOwner::Client(opened.session)),
        }
    }

    fn from_tx(
        stream: ExecuteQueryStream,
        context: &'a mut TxExecContext,
        commit_at_end: bool,
    ) -> Self {
        Self {
            stream,
            lifecycle: QueryStreamLifecycle::Active(QueryStreamOwner::Tx {
                context,
                commit_at_end,
            }),
        }
    }

    fn apply_captured_transaction_id(&mut self) -> YdbResult<()> {
        let transaction_id = self.stream.take_captured_tx_id();
        if let QueryStreamLifecycle::Active(QueryStreamOwner::Tx { context, .. }) =
            &mut self.lifecycle
        {
            apply_stream_tx_id(context, transaction_id)?;
        }
        Ok(())
    }

    fn terminate_with_error(&mut self, error: YdbError) -> YdbError {
        let mut reported_error = error;
        if let QueryStreamLifecycle::Active(QueryStreamOwner::Tx { context, .. }) =
            &mut self.lifecycle
            && context.state.is_active()
            && let Err(error) = tx_handle_query_error(context, &reported_error)
        {
            reported_error = error;
        }
        // If the error did not already end the transaction, cancelling its still-owned stream
        // finalizes the transaction lifecycle.
        self.abort();
        reported_error
    }

    fn complete(&mut self) -> YdbResult<()> {
        let lifecycle = std::mem::replace(&mut self.lifecycle, QueryStreamLifecycle::Finished);
        match lifecycle {
            QueryStreamLifecycle::Active(QueryStreamOwner::Client(session)) => {
                session.release();
                Ok(())
            }
            QueryStreamLifecycle::Active(QueryStreamOwner::Tx {
                context,
                commit_at_end,
            }) => tx_finish_query(context, commit_at_end),
            QueryStreamLifecycle::Finished => Ok(()),
        }
    }

    fn abort(&mut self) {
        if let Err(error) = self.apply_captured_transaction_id() {
            tracing::warn!(%error, "failed to capture transaction id while cancelling query stream");
        }
        self.stream.cancel();
        let lifecycle = std::mem::replace(&mut self.lifecycle, QueryStreamLifecycle::Finished);
        if let QueryStreamLifecycle::Active(QueryStreamOwner::Tx { context, .. }) = lifecycle {
            tx_cancel_query(context);
        }
    }
}

impl<'a> QueryStream<'a> {
    pub(crate) fn from_client(opened: OpenedClientQueryStream) -> Self {
        Self {
            cursor: ResultSetCursor::new(ManagedQueryParts::from_client(opened)),
        }
    }

    pub(crate) fn from_tx(
        stream: ExecuteQueryStream,
        context: &'a mut TxExecContext,
        commit_at_end: bool,
    ) -> Self {
        Self {
            cursor: ResultSetCursor::new(ManagedQueryParts::from_tx(
                stream,
                context,
                commit_at_end,
            )),
        }
    }

    pub fn stats(&self) -> Option<QueryStats> {
        self.cursor
            .source()
            .stream
            .stats()
            .map(|total_duration| QueryStats { total_duration })
    }

    /// Return the next logical result set as a lazy stream of its response parts.
    ///
    /// Only one result set can be active at a time. Each yielded [`ResultSet`] is one response
    /// part and can be consumed row by row with [`ResultSet::rows`]. If the returned result set is
    /// dropped before it reaches `None`, its unread parts are discarded by the next call to this
    /// method. Errors and query metadata encountered while discarding are still processed.
    pub async fn next_result_set(&mut self) -> YdbResult<Option<QueryResultSet<'_, 'a>>> {
        let Some(result_set_index) = self.cursor.next_result_set_index().await? else {
            return Ok(None);
        };
        Ok(Some(QueryResultSet {
            query: self,
            result_set_index,
            terminated: false,
        }))
    }

    /// Drain unread result parts and wait for successful query completion.
    ///
    /// An ordinary query leaves the transaction active and ready for its next operation. A query
    /// configured with `with_commit(true)` completes the transaction and returns its session only
    /// after YDB closes the response stream successfully.
    pub async fn finish(mut self) -> YdbResult<()> {
        self.cursor.clear_active_result_set();
        while self.cursor.next_raw().await?.is_some() {}
        Ok(())
    }
}

/// A lazy stream of response parts belonging to one logical query result set.
///
/// Obtain this value through [`QueryStream::next_result_set`]. Consume it through `None` or call
/// [`Self::discard`] to discard its remaining parts immediately. Dropping it early defers that
/// discard until the next call to [`QueryStream::next_result_set`].
#[must_use = "QueryResultSet must be consumed or discarded"]
pub struct QueryResultSet<'stream, 'query> {
    query: &'stream mut QueryStream<'query>,
    result_set_index: i64,
    terminated: bool,
}

impl QueryResultSet<'_, '_> {
    /// Zero-based index of this logical result set.
    pub fn result_set_index(&self) -> i64 {
        self.result_set_index
    }

    /// Drain and discard all unread parts of this result set.
    pub async fn discard(mut self) -> YdbResult<()> {
        while self.try_next().await?.is_some() {}
        Ok(())
    }

    fn terminate(&mut self) {
        self.terminated = true;
        self.query.cursor.clear_active_result_set();
    }

    fn poll_next_result_set_part(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<YdbResult<ResultSet>>> {
        if self.terminated {
            return Poll::Ready(None);
        }

        match ready!(
            self.query
                .cursor
                .poll_next_result_set_part(self.result_set_index, cx)
        ) {
            Some(Ok(part)) => match ResultSet::try_from(part.result_set) {
                Ok(result_set) => Poll::Ready(Some(Ok(result_set))),
                Err(err) => {
                    let err = self.query.cursor.source_mut().terminate_with_error(err);
                    self.terminate();
                    Poll::Ready(Some(Err(err)))
                }
            },
            Some(Err(err)) => {
                self.terminate();
                Poll::Ready(Some(Err(err)))
            }
            None => {
                self.terminate();
                Poll::Ready(None)
            }
        }
    }
}

impl Stream for QueryResultSet<'_, '_> {
    type Item = YdbResult<ResultSet>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_next_result_set_part(cx)
    }
}

impl FusedStream for QueryResultSet<'_, '_> {
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

/// Drain a query stream into materialized result sets.
///
/// Used by one-shot builders (`exec`, `query_result_set`, `query_row`) on both
/// [`QueryClient`](super::QueryClient) and [`Transaction`](super::Transaction).
///
/// On [`QueryClient`], the full open+drain+close cycle is retried on retryable errors.
/// Interactive transactions are materialized once since tx retries are owned by [`QueryClient::retry_tx`] loop.
pub(crate) async fn materialize_query(
    core: ExecTarget<'_>,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
) -> YdbResult<Vec<ResultSet>> {
    let commit_at_end = resolve_commit_tx(&core, &opts);
    match core {
        ExecTarget::Client(ctx) => {
            ctx.retry_settings
                .clone()
                .with_deadline(opts.timeout)
                .retry_on_retriable_errors(
                    opts.idempotency(),
                    closure!([&ctx, &text, &params, &opts], |_| {
                        materialize_client_once(ctx, text, params, opts)
                    }),
                )
                .await
        }
        ExecTarget::Tx(context) => {
            let stream = tx_begin_stream(context, text, params, opts, true).await?;
            materialize_stream(QueryStream::from_tx(stream, context, commit_at_end)).await
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

async fn materialize_stream(mut stream: QueryStream<'_>) -> YdbResult<Vec<ResultSet>> {
    let mut by_index: BTreeMap<i64, RawResultSet> = BTreeMap::new();
    while let Some(part) = stream.cursor.next_raw().await? {
        let result_set = by_index.entry(part.result_set_index).or_default();
        result_set.truncated |= part.result_set.truncated;
        if result_set.columns.is_empty() {
            result_set.columns = part.result_set.columns;
        }
        result_set.rows.extend(part.result_set.rows);
    }

    by_index.into_values().map(ResultSet::try_from).collect()
}

#[derive(Debug, Default)]
pub struct QueryStats {
    pub total_duration: Duration,
}
