use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_util::{Stream, TryStreamExt};

use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::raw_query_service::execute_query::{
    RawQueryStatsPlan, check_part, plan_from_part, stats_from_part, tx_id_from_part,
};
use crate::grpc_wrapper::raw_query_service::transaction_control::TransactionId;
use crate::grpc_wrapper::raw_table_service::value::RawResultSet;
use ydb_grpc::ydb_proto::query::ExecuteQueryResponsePart;

pub(crate) struct RawQueryResultPart {
    pub(crate) result_set_index: i64,
    pub(crate) result_set: RawResultSet,
}

struct ActiveQueryResponse {
    stream: tonic::Streaming<ExecuteQueryResponsePart>,
    pending_part: Option<ExecuteQueryResponsePart>,
}

enum QueryResponseState {
    Active(Box<ActiveQueryResponse>),
    Exhausted,
    Cancelled,
}

#[derive(Default)]
struct QueryResponseMetadata {
    tx_id: Option<TransactionId>,
    stats: Option<Duration>,
    plan: Option<RawQueryStatsPlan>,
}

pub(crate) struct ExecuteQueryStream {
    state: QueryResponseState,
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
            let received = match &mut this.state {
                QueryResponseState::Active(active) => {
                    if let Some(part) = active.pending_part.take() {
                        Some(Ok(part))
                    } else {
                        ready!(Pin::new(&mut active.stream).poll_next(cx))
                    }
                }
                QueryResponseState::Exhausted | QueryResponseState::Cancelled => {
                    return Poll::Ready(None);
                }
            };

            let Some(received) = received else {
                this.state = QueryResponseState::Exhausted;
                return Poll::Ready(None);
            };
            let part = match received {
                Ok(part) => part,
                Err(status) => {
                    this.state = QueryResponseState::Cancelled;
                    return Poll::Ready(Some(Err(RawError::from(status))));
                }
            };

            if let Err(err) = this.ingest_part(&part) {
                this.state = QueryResponseState::Cancelled;
                return Poll::Ready(Some(Err(err)));
            }

            let result_set_index = part.result_set_index;
            let Some(result_set) = part.result_set else {
                continue;
            };
            let result_set = match RawResultSet::try_from(result_set) {
                Ok(result_set) => result_set,
                Err(err) => {
                    this.state = QueryResponseState::Cancelled;
                    return Poll::Ready(Some(Err(err)));
                }
            };
            return Poll::Ready(Some(Ok(RawQueryResultPart {
                result_set_index,
                result_set,
            })));
        }
    }
}

impl ExecuteQueryStream {
    pub fn new(stream: tonic::Streaming<ExecuteQueryResponsePart>) -> Self {
        Self {
            state: QueryResponseState::Active(Box::new(ActiveQueryResponse {
                stream,
                pending_part: None,
            })),
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

    fn absorb_part_metadata(&mut self, part: &ExecuteQueryResponsePart) -> Option<TransactionId> {
        if let Some(duration) = stats_from_part(part) {
            self.metadata.stats = Some(duration);
        }
        if let Some(plan) = plan_from_part(part) {
            self.metadata.plan = Some(plan);
        }
        if let Some(id) = tx_id_from_part(part) {
            self.metadata.tx_id = Some(id.clone());
            return Some(id);
        }
        None
    }

    fn ingest_part(&mut self, part: &ExecuteQueryResponsePart) -> RawResult<Option<TransactionId>> {
        let tx_id = self.absorb_part_metadata(part);
        check_part(part)?;
        Ok(tx_id)
    }

    async fn recv_part(&mut self) -> RawResult<Option<ExecuteQueryResponsePart>> {
        let received = match &mut self.state {
            QueryResponseState::Active(active) if active.pending_part.is_some() => {
                return Ok(active.pending_part.take());
            }
            QueryResponseState::Active(active) => active.stream.message().await?,
            QueryResponseState::Exhausted | QueryResponseState::Cancelled => return Ok(None),
        };

        if received.is_none() {
            self.state = QueryResponseState::Exhausted;
        }
        Ok(received)
    }

    /// Read and validate the next response part containing result rows.
    ///
    /// Status-, statistics-, and transaction-only protocol messages are absorbed internally.
    pub(crate) async fn next_part(&mut self) -> RawResult<Option<RawQueryResultPart>> {
        self.try_next().await
    }

    fn set_pending_part(&mut self, part: ExecuteQueryResponsePart) {
        if let QueryResponseState::Active(active) = &mut self.state {
            active.pending_part = Some(part);
        }
    }

    pub(crate) async fn drain(&mut self) -> RawResult<()> {
        while self.next_part().await?.is_some() {}
        Ok(())
    }

    /// Read the first response part so transaction `tx_id` is captured before iteration.
    pub async fn prime_first_part(&mut self) -> RawResult<()> {
        if !matches!(
            self.state,
            QueryResponseState::Active(ref active) if active.pending_part.is_none()
        ) {
            return Ok(());
        }
        if let Some(part) = self.recv_part().await? {
            self.ingest_part(&part)?;
            self.set_pending_part(part);
        }
        Ok(())
    }

    pub fn take_captured_tx_id(&mut self) -> Option<TransactionId> {
        self.metadata.tx_id.take()
    }

    pub(crate) fn in_progress(&self) -> bool {
        matches!(self.state, QueryResponseState::Active(_))
    }

    /// Drop the gRPC stream without draining unread parts (sends RST_STREAM).
    pub fn cancel(&mut self) {
        let old_state = std::mem::replace(&mut self.state, QueryResponseState::Cancelled);
        match old_state {
            QueryResponseState::Active(mut active) => {
                if let Some(part) = active.pending_part.take() {
                    self.absorb_part_metadata(&part);
                }
            }
            terminal => self.state = terminal,
        }
    }
}
