use std::collections::HashMap;
use std::time::Duration;

use crate::errors::{YdbError, YdbResult};
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::result::ResultSet;
use crate::types::Value;

use super::exec::{
    apply_stream_tx_id, resolve_commit_tx, transaction_finish_committed_via_query, CallOptions,
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
        let (raw, tx_id) = match self.stream.next_result_set().await? {
            Some(v) => v,
            None => return Ok(None),
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
        let meta = self.stream.close().await.map_err(YdbError::from)?;
        if let ExecCoreRef::Transaction(ctx) = &mut self.core {
            apply_stream_tx_id(ctx, meta.tx_id);
            if self.commit_tx {
                transaction_finish_committed_via_query(ctx).await;
            }
        }
        Ok(())
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
    let mut stream = core.begin_stream(text, params, opts).await?;
    let mut sets = Vec::new();
    let mut drain_err: Option<YdbError> = None;
    while drain_err.is_none() {
        match stream.next_result_set().await {
            Ok(Some((raw, tx_id))) => {
                if let ExecCoreRef::Transaction(ctx) = core {
                    apply_stream_tx_id(ctx, tx_id);
                }
                match ResultSet::try_from(raw) {
                    Ok(set) => sets.push(set),
                    Err(err) => drain_err = Some(err),
                }
            }
            Ok(None) => break,
            Err(err) => drain_err = Some(YdbError::from(err)),
        }
    }
    if drain_err.is_none() {
        match stream.close().await {
            Ok(meta) => {
                if let ExecCoreRef::Transaction(ctx) = core {
                    apply_stream_tx_id(ctx, meta.tx_id);
                    if commit_tx {
                        transaction_finish_committed_via_query(ctx).await;
                    }
                }
            }
            Err(err) => drain_err = Some(YdbError::from(err)),
        }
    }
    if let ExecCoreRef::Transaction(ctx) = core {
        if let Some(lease) = &mut ctx.pooled_lease {
            lease.end_use();
        }
    }
    if let Some(err) = drain_err {
        return Err(err);
    }
    Ok(sets)
}

#[derive(Debug, Default)]
pub struct QueryStats {
    pub total_duration: Duration,
}
