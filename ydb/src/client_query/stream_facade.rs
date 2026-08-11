use std::collections::HashMap;
use std::time::Duration;

use crate::closure;
use crate::errors::{YdbError, YdbResult};
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::grpc_wrapper::raw_query_service::transaction_control::TransactionId;
use crate::grpc_wrapper::raw_table_service::value::RawResultSet;
use crate::result::ResultSet;
use crate::types::Value;

use super::exec::{
    CallOptions, ClientExecContext, ClientQuerySession, OpenedClientQueryStream,
    TransactionExecContext, client_begin_stream_once, transaction_begin_stream,
};
use super::internal::ExecCoreRef;

/// Streaming query result. Reaching EOF returns an owned pooled session for reuse;
/// [`Self::close`] safely drains any unread response parts first. Dropping before EOF cancels the
/// stream and discards its session. Inside a transaction, [`CallBuilder::with_commit(true)`]
/// commits only after EOF is observed by iteration or `close`.
#[must_use = "QueryStream must be fully consumed or closed"]
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
        self.cancel_active();
    }
}

impl<'a> QueryStream<'a> {
    fn take_owner(&mut self) -> Option<QueryStreamOwner<'a>> {
        match std::mem::replace(&mut self.lifecycle, QueryStreamLifecycle::Finished) {
            QueryStreamLifecycle::Active(owner) => Some(owner),
            QueryStreamLifecycle::Finished => None,
        }
    }

    fn cancel_active(&mut self) {
        let stream_in_progress = self.stream.in_progress();
        self.stream.cancel();
        match self.take_owner() {
            Some(QueryStreamOwner::Client(session)) => {
                if !stream_in_progress {
                    session.complete();
                }
            }
            Some(QueryStreamOwner::Transaction {
                context,
                commit_at_end,
            }) => {
                let transaction_id = self.stream.take_captured_tx_id();
                let result = context
                    .apply_stream_transaction_id(transaction_id)
                    .and_then(|()| {
                        if stream_in_progress {
                            context.cancel_query()
                        } else {
                            context.finish_query(commit_at_end)
                        }
                    });
                if let Err(error) = result {
                    tracing::error!(%error, "failed to finish dropped transaction query stream");
                }
            }
            None => {}
        }
    }

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

    fn apply_transaction_id(&mut self, transaction_id: Option<TransactionId>) -> YdbResult<()> {
        if let QueryStreamLifecycle::Active(QueryStreamOwner::Transaction { context, .. }) =
            &mut self.lifecycle
        {
            context.apply_stream_transaction_id(transaction_id)?;
        }
        Ok(())
    }

    fn apply_captured_transaction_id(&mut self) -> YdbResult<()> {
        let transaction_id = self.stream.take_captured_tx_id();
        self.apply_transaction_id(transaction_id)
    }

    fn fail(&mut self, error: &YdbError) -> YdbResult<()> {
        let transaction_id = self.stream.take_captured_tx_id();
        self.stream.cancel();
        if let Some(QueryStreamOwner::Transaction { context, .. }) = self.take_owner() {
            context.apply_stream_transaction_id(transaction_id)?;
            context.handle_query_error(error)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> YdbResult<()> {
        match self.take_owner() {
            Some(QueryStreamOwner::Client(session)) => {
                session.complete();
                Ok(())
            }
            Some(QueryStreamOwner::Transaction {
                context,
                commit_at_end,
            }) => context.finish_query(commit_at_end),
            None => Ok(()),
        }
    }

    pub async fn next_result_set(&mut self) -> YdbResult<Option<ResultSet>> {
        let next = match self.stream.next_result_set().await {
            Ok(v) => v,
            Err(err) => {
                let ydb_err = YdbError::from(err);
                self.fail(&ydb_err)?;
                return Err(ydb_err);
            }
        };
        match next {
            Some(raw) => {
                self.apply_captured_transaction_id()?;
                let result_set = ResultSet::try_from(raw);
                if !self.stream.in_progress() {
                    self.finish()?;
                }
                result_set.map(Some)
            }
            None => {
                self.apply_captured_transaction_id()?;
                self.finish()?;
                Ok(None)
            }
        }
    }

    async fn materialize_all_result_sets(&mut self) -> YdbResult<Vec<RawResultSet>> {
        match self.stream.materialize_all_result_sets().await {
            Ok(result_sets) => Ok(result_sets),
            Err(error) => {
                let error = YdbError::from(error);
                self.fail(&error)?;
                Err(error)
            }
        }
    }

    pub fn stats(&self) -> Option<QueryStats> {
        self.stream
            .stats()
            .map(|total_duration| QueryStats { total_duration })
    }

    pub async fn close(mut self) -> YdbResult<()> {
        match self.stream.close().await {
            Ok(transaction_id) => {
                self.apply_transaction_id(transaction_id)?;
                self.finish()
            }
            Err(err) => {
                let ydb_err = YdbError::from(err);
                self.fail(&ydb_err)?;
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
    let mut stream = QueryStream::from_client(opened);
    let raw_sets = stream.materialize_all_result_sets().await?;
    stream.close().await?;
    convert_result_sets(raw_sets)
}

async fn materialize_transaction_once(
    context: &mut TransactionExecContext,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
    commit_at_end: bool,
) -> YdbResult<Vec<ResultSet>> {
    let stream = transaction_begin_stream(context, text, params, opts, true).await?;
    let mut stream = QueryStream::from_transaction(stream, context, commit_at_end);
    let result_sets = stream.materialize_all_result_sets().await?;
    stream.close().await?;
    convert_result_sets(result_sets)
}

fn convert_result_sets(raw_sets: Vec<RawResultSet>) -> YdbResult<Vec<ResultSet>> {
    raw_sets.into_iter().map(ResultSet::try_from).collect()
}

#[derive(Debug, Default)]
pub struct QueryStats {
    pub total_duration: Duration,
}
