use std::collections::HashMap;
use std::time::Duration;

use crate::closure;
use crate::errors::{YdbError, YdbResult};
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::result::ResultSet;
use crate::session_pool::SessionPoolLease;
use crate::types::Value;

use super::exec::{
    CallOptions, ClientExecContext, apply_stream_tx_id, client_begin_stream_once,
    resolve_commit_tx, transaction_finish_committed_via_query,
    transaction_mark_invalidated_on_query_error,
};
use super::internal::ExecCoreRef;

/// Streaming query result. Drain all result sets and call [`Self::close`] to return an owned
/// pooled session for reuse. Dropping the stream cancels it and discards that session.
/// Inside a transaction, [`CallBuilder::with_commit(true)`] commits only on successful close.
#[must_use = "QueryStream must be fully consumed and closed"]
pub struct QueryStream<'a> {
    pub(crate) core: ExecCoreRef<'a>,
    pub(crate) stream: ExecuteQueryStream,
    /// Lease owned by this stream for a pooled `QueryClient` query.
    pub(crate) owned_lease: Option<SessionPoolLease>,
    pub(crate) commit_tx: bool,
}

impl Drop for QueryStream<'_> {
    fn drop(&mut self) {
        if let Some(tx_id) = self.stream.take_captured_tx_id()
            && let ExecCoreRef::Transaction(ctx) = &mut self.core
        {
            apply_stream_tx_id(ctx, Some(tx_id));
        }
        // Do not mark the transaction finished here: with_commit(true) requires
        // draining the stream and calling close() so the server can commit.
        let dropped_mid_stream = self.stream.in_progress();
        self.stream.cancel();
        if let ExecCoreRef::Transaction(ctx) = &mut self.core
            && let Some(lease) = &mut ctx.pooled_lease
            && dropped_mid_stream
        {
            lease.invalidate();
        }
    }
}

impl QueryStream<'_> {
    pub async fn next_result_set(&mut self) -> YdbResult<Option<ResultSet>> {
        let next = match self.stream.next_result_set().await {
            Ok(v) => v,
            Err(err) => {
                let ydb_err = YdbError::from(err);
                if let ExecCoreRef::Transaction(ctx) = &mut self.core {
                    transaction_mark_invalidated_on_query_error(ctx, &ydb_err);
                }
                return Err(ydb_err);
            }
        };
        let Some((raw, tx_id)) = next else {
            return Ok(None);
        };
        if let ExecCoreRef::Transaction(ctx) = &mut self.core {
            apply_stream_tx_id(ctx, tx_id);
        }
        Ok(Some(ResultSet::try_from(raw)?))
    }

    pub fn stats(&self) -> Option<QueryStats> {
        self.stream
            .stats()
            .map(|total_duration| QueryStats { total_duration })
    }

    pub async fn close(mut self) -> YdbResult<()> {
        match self.stream.close().await {
            Ok(meta) => {
                if let Some(lease) = self.owned_lease.take() {
                    lease.return_to_pool();
                }
                if let ExecCoreRef::Transaction(ctx) = &mut self.core {
                    apply_stream_tx_id(ctx, meta.tx_id);
                    if self.commit_tx {
                        transaction_finish_committed_via_query(ctx);
                    }
                }
                Ok(())
            }
            Err(err) => {
                let ydb_err = YdbError::from(err);
                if let ExecCoreRef::Transaction(ctx) = &mut self.core {
                    transaction_mark_invalidated_on_query_error(ctx, &ydb_err);
                }
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
    match core {
        ExecCoreRef::Client(ctx) => {
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
        ExecCoreRef::Transaction(_) => materialize_transaction_once(core, text, params, opts).await,
    }
}

async fn materialize_client_once(
    ctx: &ClientExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
) -> YdbResult<Vec<ResultSet>> {
    let mut opened = client_begin_stream_once(ctx, text, params, opts, true).await?;
    let sets = collect_result_sets(&mut opened.stream).await?;
    opened.stream.close().await?;
    if let Some(lease) = opened.owned_lease.take() {
        lease.return_to_pool();
    }
    Ok(sets)
}

async fn materialize_transaction_once(
    core: &mut ExecCoreRef<'_>,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
) -> YdbResult<Vec<ResultSet>> {
    let commit_tx = resolve_commit_tx(core, &opts);
    let result: YdbResult<Vec<ResultSet>> = async {
        let mut opened = core.begin_stream(text, params, opts, true).await?;
        let sets = match collect_result_sets(&mut opened.stream).await {
            Ok(sets) => sets,
            Err(ydb_err) => {
                if let ExecCoreRef::Transaction(ctx) = core {
                    transaction_mark_invalidated_on_query_error(ctx, &ydb_err);
                }
                return Err(ydb_err);
            }
        };
        match opened.stream.close().await {
            Ok(meta) => {
                if let Some(lease) = opened.owned_lease.take() {
                    lease.return_to_pool();
                }
                if let ExecCoreRef::Transaction(ctx) = core {
                    apply_stream_tx_id(ctx, meta.tx_id);
                    if commit_tx {
                        transaction_finish_committed_via_query(ctx);
                    }
                }
            }
            Err(err) => {
                let ydb_err = YdbError::from(err);
                if let ExecCoreRef::Transaction(ctx) = core {
                    transaction_mark_invalidated_on_query_error(ctx, &ydb_err);
                }
                return Err(ydb_err);
            }
        }
        Ok(sets)
    }
    .await;

    result
}

async fn collect_result_sets(stream: &mut ExecuteQueryStream) -> YdbResult<Vec<ResultSet>> {
    let raw_sets = stream
        .materialize_all_result_sets()
        .await
        .map_err(YdbError::from)?;
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
