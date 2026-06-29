use std::collections::HashMap;
use std::time::Duration;

use crate::errors::{YdbError, YdbResult};
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::result::ResultSet;
use crate::types::Value;

use super::exec::{
    apply_stream_tx_id, resolve_commit_tx, transaction_finish_committed_via_query,
    transaction_mark_invalidated_on_query_error, CallOptions,
};
use super::internal::ExecCoreRef;

/// Streaming query result. When obtained with [`CallBuilder::with_commit(true)`] inside a
/// transaction, you must drain all result sets and call [`Self::close`]; dropping early
/// cancels the gRPC stream and does not commit.
#[must_use = "QueryStream must be fully consumed; call close() when using with_commit(true)"]
pub struct QueryStream<'a> {
    pub(crate) core: ExecCoreRef<'a>,
    pub(crate) stream: ExecuteQueryStream,
    pub(crate) commit_tx: bool,
}

impl Drop for QueryStream<'_> {
    fn drop(&mut self) {
        if let Some(tx_id) = self.stream.take_captured_tx_id() {
            if let ExecCoreRef::Transaction(ctx) = &mut self.core {
                apply_stream_tx_id(ctx, Some(tx_id));
            }
        }
        // Do not mark the transaction finished here: with_commit(true) requires
        // draining the stream and calling close() so the server can commit.
        self.stream.cancel();
        if let ExecCoreRef::Transaction(ctx) = &mut self.core {
            if let Some(lease) = &mut ctx.pooled_lease {
                lease.end_use();
            }
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
                if let ExecCoreRef::Transaction(ctx) = &mut self.core {
                    apply_stream_tx_id(ctx, meta.tx_id);
                    if self.commit_tx {
                        transaction_finish_committed_via_query(ctx).await;
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
/// [`QueryClient`](super::QueryClient) and [`QueryTransaction`](super::QueryTransaction).
pub(crate) async fn materialize_query(
    core: &mut ExecCoreRef<'_>,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
) -> YdbResult<Vec<ResultSet>> {
    let commit_tx = resolve_commit_tx(core, &opts);
    let result: YdbResult<Vec<ResultSet>> = async {
        let mut stream = core.begin_stream(text, params, opts, true).await?;
        let raw_sets = match stream.materialize_all_result_sets().await {
            Ok(v) => v,
            Err(err) => {
                let ydb_err = YdbError::from(err);
                if let ExecCoreRef::Transaction(ctx) = core {
                    transaction_mark_invalidated_on_query_error(ctx, &ydb_err);
                }
                return Err(ydb_err);
            }
        };
        let mut sets = Vec::with_capacity(raw_sets.len());
        for raw in raw_sets {
            sets.push(ResultSet::try_from(raw)?);
        }
        match stream.close().await {
            Ok(meta) => {
                if let ExecCoreRef::Transaction(ctx) = core {
                    apply_stream_tx_id(ctx, meta.tx_id);
                    if commit_tx {
                        transaction_finish_committed_via_query(ctx).await;
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

    if let ExecCoreRef::Transaction(ctx) = core {
        if let Some(lease) = &mut ctx.pooled_lease {
            lease.end_use();
        }
    }

    result
}

#[derive(Debug, Default)]
pub struct QueryStats {
    pub total_duration: Duration,
}
