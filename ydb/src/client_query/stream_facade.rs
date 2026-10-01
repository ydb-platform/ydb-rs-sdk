use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::closure;
use crate::errors::{YdbError, YdbResult};
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::grpc_wrapper::raw_table_service::value::RawResultSet;
use crate::result::ResultSet;
use crate::types::Value;

use super::exec::{
    CallOptions, ClientExecContext, ClientQuerySession, ExecTarget, OpenedClientQueryStream,
    TxExecContext, apply_stream_tx_id, client_begin_stream_once, resolve_commit_tx,
    tx_begin_stream, tx_cancel_query, tx_finish_query, tx_handle_query_error,
};
use super::virtual_timestamp::VirtualTimestamp;

/// Streaming query result. Drain all result sets and call [`Self::close`] to return an owned
/// pooled session for reuse. Dropping the stream cancels it and discards that session.
/// Inside a transaction, [`CallBuilder::with_commit(true)`] commits only on successful close.
#[must_use = "QueryStream must be fully consumed and closed"]
pub struct QueryStream<'a> {
    stream: ExecuteQueryStream,
    lifecycle: QueryStreamLifecycle<'a>,
    timestamp_scope: Arc<str>,
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

impl Drop for QueryStream<'_> {
    fn drop(&mut self) {
        self.abort();
    }
}

impl QueryStream<'_> {
    fn abort(&mut self) {
        if let Err(error) = self.apply_captured_transaction_id() {
            tracing::warn!(%error, "failed to capture transaction id while cancelling query stream");
        }
        self.stream.cancel();
        self.finish_cancelled_lifecycle();
    }

    fn finish_cancelled_lifecycle(&mut self) {
        let lifecycle = std::mem::replace(&mut self.lifecycle, QueryStreamLifecycle::Finished);
        if let QueryStreamLifecycle::Active(QueryStreamOwner::Tx { context, .. }) = lifecycle {
            tx_cancel_query(context);
        }
    }
}

impl<'a> QueryStream<'a> {
    pub(crate) fn from_client(opened: OpenedClientQueryStream, timestamp_scope: Arc<str>) -> Self {
        Self {
            stream: opened.stream,
            lifecycle: QueryStreamLifecycle::Active(QueryStreamOwner::Client(opened.session)),
            timestamp_scope,
        }
    }

    pub(crate) fn from_tx(
        stream: ExecuteQueryStream,
        context: &'a mut TxExecContext,
        commit_at_end: bool,
    ) -> Self {
        let timestamp_scope = context.timestamp_scope.clone();
        Self {
            stream,
            lifecycle: QueryStreamLifecycle::Active(QueryStreamOwner::Tx {
                context,
                commit_at_end,
            }),
            timestamp_scope,
        }
    }

    fn apply_transaction_id(&mut self, transaction_id: Option<String>) -> YdbResult<()> {
        if let QueryStreamLifecycle::Active(QueryStreamOwner::Tx { context, .. }) =
            &mut self.lifecycle
        {
            apply_stream_tx_id(context, transaction_id)?;
        }
        Ok(())
    }

    fn apply_captured_transaction_id(&mut self) -> YdbResult<()> {
        let transaction_id = self.stream.take_captured_tx_id();
        self.apply_transaction_id(transaction_id)
    }

    fn handle_error(&mut self, error: &YdbError) -> YdbResult<()> {
        if let QueryStreamLifecycle::Active(QueryStreamOwner::Tx { context, .. }) =
            &mut self.lifecycle
        {
            tx_handle_query_error(context, error)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> YdbResult<()> {
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

    pub async fn next_result_set(&mut self) -> YdbResult<Option<ResultSet>> {
        let next = match self.stream.next_result_set().await {
            Ok(v) => v,
            Err(err) => {
                let ydb_err = YdbError::from(err);
                self.stream.cancel();
                self.handle_error(&ydb_err)?;
                if matches!(
                    &self.lifecycle,
                    QueryStreamLifecycle::Active(QueryStreamOwner::Client(_))
                ) && !ydb_err.requires_session_discard()
                {
                    return Err(ydb_err);
                }
                self.lifecycle = QueryStreamLifecycle::Finished;
                return Err(ydb_err);
            }
        };
        match next {
            Some((raw, transaction_id)) => {
                self.apply_transaction_id(transaction_id)?;
                ResultSet::try_from(raw).map(Some)
            }
            None => {
                self.apply_captured_transaction_id()?;
                Ok(None)
            }
        }
    }

    pub fn stats(&self) -> Option<QueryStats> {
        self.stream
            .stats()
            .map(|total_duration| QueryStats { total_duration })
    }

    pub async fn close(self) -> YdbResult<()> {
        self.close_with_commit_timestamp().await.map(|_| ())
    }

    /// Finish the stream and return the optional commit timestamp from its final trailing part.
    ///
    /// Fully drain the stream before calling this method. Closing before EOF returns `None`
    /// because the trailing response has not been observed.
    pub async fn close_with_commit_timestamp(mut self) -> YdbResult<Option<VirtualTimestamp>> {
        match self.stream.close().await {
            Ok(meta) => {
                self.apply_transaction_id(meta.tx_id)?;
                self.finish()?;
                Ok(meta.commit_timestamp.map(|timestamp| {
                    VirtualTimestamp::from_proto(timestamp, self.timestamp_scope.clone())
                }))
            }
            Err(err) => {
                let ydb_err = YdbError::from(err);
                self.handle_error(&ydb_err)?;
                self.lifecycle = QueryStreamLifecycle::Finished;
                Err(ydb_err)
            }
        }
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
    core: &mut ExecTarget<'_>,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
) -> YdbResult<MaterializedQuery> {
    let commit_at_end = resolve_commit_tx(core, &opts);
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
            materialize_tx_once(context, text, params, opts, commit_at_end).await
        }
    }
}

pub(crate) struct MaterializedQuery {
    pub result_sets: Vec<ResultSet>,
    pub commit_timestamp: Option<VirtualTimestamp>,
}

async fn materialize_client_once(
    ctx: &ClientExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
) -> YdbResult<MaterializedQuery> {
    let mut opened = client_begin_stream_once(ctx, text, params, opts, true).await?;
    let result: YdbResult<_> = async {
        let raw_sets = drain_result_sets(&mut opened.stream).await?;
        let meta = opened.stream.close().await?;
        Ok((raw_sets, meta))
    }
    .await;
    match result {
        Ok((raw_sets, meta)) => {
            opened.session.release();
            Ok(MaterializedQuery {
                result_sets: convert_result_sets(raw_sets)?,
                commit_timestamp: meta.commit_timestamp.map(|timestamp| {
                    VirtualTimestamp::from_proto(timestamp, ctx.timestamp_scope.clone())
                }),
            })
        }
        Err(error) => Err(error),
    }
}

async fn materialize_tx_once(
    context: &mut TxExecContext,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
    commit_at_end: bool,
) -> YdbResult<MaterializedQuery> {
    let mut stream = tx_begin_stream(context, text, params, opts, true).await?;
    let raw_sets = match drain_result_sets(&mut stream).await {
        Ok(raw_sets) => raw_sets,
        Err(ydb_err) => {
            tx_handle_query_error(context, &ydb_err)?;
            return Err(ydb_err);
        }
    };
    let sets = match convert_result_sets(raw_sets) {
        Ok(sets) => sets,
        Err(ydb_err) => {
            tx_handle_query_error(context, &ydb_err)?;
            return Err(ydb_err);
        }
    };
    let commit_timestamp = match stream.close().await {
        Ok(meta) => {
            apply_stream_tx_id(context, meta.tx_id)?;
            tx_finish_query(context, commit_at_end)?;
            meta.commit_timestamp.map(|timestamp| {
                VirtualTimestamp::from_proto(timestamp, context.timestamp_scope.clone())
            })
        }
        Err(err) => {
            let ydb_err = YdbError::from(err);
            tx_handle_query_error(context, &ydb_err)?;
            return Err(ydb_err);
        }
    };
    Ok(MaterializedQuery {
        result_sets: sets,
        commit_timestamp,
    })
}

async fn drain_result_sets(stream: &mut ExecuteQueryStream) -> YdbResult<Vec<RawResultSet>> {
    stream
        .materialize_all_result_sets()
        .await
        .map_err(YdbError::from)
}

fn convert_result_sets(raw_sets: Vec<RawResultSet>) -> YdbResult<Vec<ResultSet>> {
    let mut sets = Vec::with_capacity(raw_sets.len());
    for raw in raw_sets {
        sets.push(ResultSet::try_from(raw)?);
    }
    Ok(sets)
}

#[derive(Debug, Default)]
pub struct QueryStats {
    pub total_duration: Duration,
}
