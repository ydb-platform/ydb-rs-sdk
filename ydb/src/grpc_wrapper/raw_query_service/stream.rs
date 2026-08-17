use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_util::{Stream, TryStreamExt};

use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::raw_query_service::execute_query::{
    RawQueryStatsPlan, check_part, columns_compatible, plan_from_part, stats_from_part,
    tx_id_from_part,
};
use crate::grpc_wrapper::raw_table_service::value::{RawColumn, RawResultSet};
use ydb_grpc::ydb_proto::query::ExecuteQueryResponsePart;

pub(crate) struct RawQueryResultPart {
    pub(crate) result_set_index: i64,
    pub(crate) result_set: RawResultSet,
}

struct ActiveQueryResponse {
    stream: tonic::Streaming<ExecuteQueryResponsePart>,
    pending_result: Option<RawQueryResultPart>,
}

#[derive(Default)]
struct QueryResponseMetadata {
    tx_id: Option<String>,
    stats: Option<Duration>,
    plan: Option<RawQueryStatsPlan>,
    columns_by_result_set: HashMap<i64, Vec<RawColumn>>,
}

pub(crate) struct ExecuteQueryStream {
    active: Option<ActiveQueryResponse>,
    metadata: QueryResponseMetadata,
}

impl Drop for ExecuteQueryStream {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl Stream for ExecuteQueryStream {
    type Item = RawResult<RawQueryResultPart>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            let Some(active) = &mut this.active else {
                return Poll::Ready(None);
            };
            let received = if let Some(part) = active.pending_result.take() {
                return Poll::Ready(Some(Ok(part)));
            } else {
                ready!(Pin::new(&mut active.stream).poll_next(cx))
            };

            let Some(received) = received else {
                this.active = None;
                return Poll::Ready(None);
            };
            let part = match received {
                Ok(part) => part,
                Err(status) => {
                    this.active = None;
                    return Poll::Ready(Some(Err(RawError::from(status))));
                }
            };

            match this.decode_part(part) {
                Ok(Some(part)) => return Poll::Ready(Some(Ok(part))),
                Ok(None) => continue,
                Err(err) => {
                    this.active = None;
                    return Poll::Ready(Some(Err(err)));
                }
            }
        }
    }
}

impl ExecuteQueryStream {
    pub fn new(stream: tonic::Streaming<ExecuteQueryResponsePart>) -> Self {
        Self {
            active: Some(ActiveQueryResponse {
                stream,
                pending_result: None,
            }),
            metadata: QueryResponseMetadata::default(),
        }
    }

    pub fn stats(&self) -> Option<Duration> {
        self.metadata.stats
    }

    /// Query plan and AST from `exec_stats`, whichever the server filled in.
    ///
    /// In practice only `EXPLAIN` responses carry them: `collect_stats` sends `STATS_MODE_BASIC`,
    /// which reports neither. See [`RawQueryStatsPlan`] for the full matrix.
    pub(crate) fn take_query_plan(&mut self) -> Option<RawQueryStatsPlan> {
        self.metadata.plan.take()
    }

    fn absorb_part_metadata(&mut self, part: &ExecuteQueryResponsePart) {
        if let Some(duration) = stats_from_part(part) {
            self.metadata.stats = Some(duration);
        }
        if let Some(plan) = plan_from_part(part) {
            self.metadata.plan = Some(plan);
        }
        if let Some(id) = tx_id_from_part(part) {
            self.metadata.tx_id = Some(id);
        }
    }

    fn decode_part(
        &mut self,
        part: ExecuteQueryResponsePart,
    ) -> RawResult<Option<RawQueryResultPart>> {
        self.absorb_part_metadata(&part);
        check_part(&part)?;
        let result_set_index = part.result_set_index;
        let Some(result_set) = part.result_set else {
            return Ok(None);
        };
        let mut result_set = RawResultSet::try_from(result_set)?;
        self.apply_result_set_columns(result_set_index, &mut result_set)?;
        Ok(Some(RawQueryResultPart {
            result_set_index,
            result_set,
        }))
    }

    fn apply_result_set_columns(
        &mut self,
        result_set_index: i64,
        result_set: &mut RawResultSet,
    ) -> RawResult<()> {
        if let Some(columns) = self.metadata.columns_by_result_set.get(&result_set_index) {
            if result_set.columns.is_empty() {
                result_set.columns = columns.clone();
            } else if !columns_compatible(columns, &result_set.columns) {
                return Err(RawError::custom(format!(
                    "column metadata mismatch for result set {result_set_index}"
                )));
            }
        } else if !result_set.columns.is_empty() {
            self.metadata
                .columns_by_result_set
                .insert(result_set_index, result_set.columns.clone());
        } else if !result_set.rows.is_empty() {
            return Err(RawError::custom(format!(
                "result set {result_set_index} contains rows before column metadata"
            )));
        }
        Ok(())
    }

    pub(crate) async fn drain(&mut self) -> RawResult<()> {
        while self.try_next().await?.is_some() {}
        Ok(())
    }

    /// Read the first response part so transaction `tx_id` is captured before iteration.
    pub async fn prime_first_part(&mut self) -> RawResult<()> {
        let Some(active) = &mut self.active else {
            return Ok(());
        };
        if active.pending_result.is_some() {
            return Ok(());
        }
        let Some(part) = active.stream.message().await? else {
            self.active = None;
            return Ok(());
        };

        match self.decode_part(part) {
            Ok(Some(part)) => {
                let Some(active) = &mut self.active else {
                    return Err(RawError::custom(
                        "query response became inactive while priming its first part".to_string(),
                    ));
                };
                active.pending_result = Some(part);
            }
            Ok(None) => {}
            Err(error) => {
                self.active = None;
                return Err(error);
            }
        }
        Ok(())
    }

    pub fn take_captured_tx_id(&mut self) -> Option<String> {
        self.metadata.tx_id.take()
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.is_some()
    }

    /// Drop the gRPC stream without draining unread parts (sends RST_STREAM).
    pub fn cancel(&mut self) {
        self.active = None;
    }
}
