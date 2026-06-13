use std::time::Duration;

use crate::errors::{YdbError, YdbResult};
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::result::ResultSet;

use super::exec::apply_stream_tx_id;
use super::internal::ExecCoreRef;

pub struct QueryStream<'a> {
    pub(crate) core: ExecCoreRef<'a>,
    pub(crate) stream: ExecuteQueryStream,
}

impl Drop for QueryStream<'_> {
    fn drop(&mut self) {
        if let Some(tx_id) = self.stream.take_captured_tx_id() {
            if let ExecCoreRef::Transaction(ctx) = &mut self.core {
                apply_stream_tx_id(ctx, Some(tx_id));
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
        let meta_result = self.stream.close().await.map_err(YdbError::from);
        if let ExecCoreRef::Transaction(ctx) = &mut self.core {
            match &meta_result {
                Ok(meta) => apply_stream_tx_id(ctx, meta.tx_id.clone()),
                Err(_) => {
                    if let Some(tx_id) = self.stream.take_captured_tx_id() {
                        apply_stream_tx_id(ctx, Some(tx_id));
                    }
                }
            }
        }
        meta_result.map(|_| ())
    }
}

#[derive(Debug, Default)]
pub struct QueryStats {
    pub total_duration: Duration,
}
