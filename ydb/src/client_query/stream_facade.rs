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
        if self.commit_tx {
            if let ExecCoreRef::Transaction(ctx) = &mut self.core {
                // Server committed via `commit_tx`; sync client state so cleanup does not
                // issue a spurious CommitTransaction. Session release runs in retry_transaction.
                ctx.finished = true;
                ctx.tx_id = None;
            }
        }
        self.stream.cancel();
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
            apply_stream_tx_id(ctx, meta.tx_id.clone());
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
    while let Some((raw, tx_id)) = stream.next_result_set().await.map_err(YdbError::from)? {
        if let ExecCoreRef::Transaction(ctx) = core {
            apply_stream_tx_id(ctx, tx_id);
        }
        sets.push(ResultSet::try_from(raw)?);
    }
    let meta = stream.close().await.map_err(YdbError::from)?;
    if let ExecCoreRef::Transaction(ctx) = core {
        apply_stream_tx_id(ctx, meta.tx_id);
        if commit_tx {
            transaction_finish_committed_via_query(ctx).await;
        }
    }
    Ok(sets)
}

#[derive(Debug, Default)]
pub struct QueryStats {
    pub total_duration: Duration,
}
