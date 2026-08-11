use std::collections::BTreeMap;
use std::time::Duration;

use tracing::warn;

use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_query_service::execute_query::{
    RawQueryStatsPlan, append_rows_from_part, check_part, plan_from_part, stats_from_part,
    tx_id_from_part,
};
use crate::grpc_wrapper::raw_table_service::value::RawResultSet;
use ydb_grpc::ydb_proto::query::ExecuteQueryResponsePart;

#[derive(Debug)]
pub(crate) struct StreamCloseMeta {
    pub tx_id: Option<String>,
}

enum QueryResponseSource {
    Grpc(Box<tonic::Streaming<ExecuteQueryResponsePart>>),
    #[cfg(test)]
    Parts(Vec<ExecuteQueryResponsePart>),
}

struct ActiveQueryResponse {
    source: QueryResponseSource,
    pending_part: Option<ExecuteQueryResponsePart>,
}

enum QueryResponseState {
    Active(Box<ActiveQueryResponse>),
    Exhausted,
    Cancelled,
}

#[derive(Default)]
struct QueryResponseMetadata {
    tx_id: Option<String>,
    stats: Option<Duration>,
    plan: Option<RawQueryStatsPlan>,
}

#[derive(Default)]
struct PartialResultSet {
    columns: Vec<crate::grpc_wrapper::raw_table_service::value::RawColumn>,
    rows: Vec<Vec<crate::grpc_wrapper::raw_table_service::value::RawValue>>,
    truncated: bool,
}

pub(crate) struct ExecuteQueryStream {
    state: QueryResponseState,
    next_index: i64,
    metadata: QueryResponseMetadata,
}

impl Drop for ExecuteQueryStream {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl ExecuteQueryStream {
    pub fn new(stream: tonic::Streaming<ExecuteQueryResponsePart>) -> Self {
        Self {
            state: QueryResponseState::Active(Box::new(ActiveQueryResponse {
                source: QueryResponseSource::Grpc(Box::new(stream)),
                pending_part: None,
            })),
            next_index: 0,
            metadata: QueryResponseMetadata::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_test_parts(mut parts: Vec<ExecuteQueryResponsePart>) -> Self {
        parts.reverse();
        Self {
            state: QueryResponseState::Active(Box::new(ActiveQueryResponse {
                source: QueryResponseSource::Parts(parts),
                pending_part: None,
            })),
            next_index: 0,
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

    fn absorb_part_metadata(&mut self, part: &ExecuteQueryResponsePart) -> Option<String> {
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

    fn ingest_part(&mut self, part: &ExecuteQueryResponsePart) -> RawResult<Option<String>> {
        let tx_id = self.absorb_part_metadata(part);
        check_part(part)?;
        Ok(tx_id)
    }

    async fn recv_part(&mut self) -> RawResult<Option<ExecuteQueryResponsePart>> {
        let received = match &mut self.state {
            QueryResponseState::Active(active) if active.pending_part.is_some() => {
                return Ok(active.pending_part.take());
            }
            QueryResponseState::Active(active) => match &mut active.source {
                QueryResponseSource::Grpc(stream) => stream.message().await?,
                #[cfg(test)]
                QueryResponseSource::Parts(parts) => parts.pop(),
            },
            QueryResponseState::Exhausted | QueryResponseState::Cancelled => return Ok(None),
        };

        if received.is_none() {
            self.state = QueryResponseState::Exhausted;
        }
        Ok(received)
    }

    fn set_pending_part(&mut self, part: ExecuteQueryResponsePart) {
        if let QueryResponseState::Active(active) = &mut self.state {
            active.pending_part = Some(part);
        }
    }

    fn append_part_to_index(
        by_index: &mut BTreeMap<i64, PartialResultSet>,
        part: ExecuteQueryResponsePart,
    ) -> RawResult<()> {
        if part.result_set.is_none() {
            return Ok(());
        }
        let index = part.result_set_index;
        let partial = by_index.entry(index).or_default();
        append_rows_from_part(
            &mut partial.columns,
            &mut partial.rows,
            &mut partial.truncated,
            part,
        )
    }

    /// Drain the stream and assemble all result sets, buffering parts by
    /// `result_set_index`. Required when `concurrent_result_sets=true` because
    /// the server may interleave parts from different result sets.
    pub async fn materialize_all_result_sets(&mut self) -> RawResult<Vec<RawResultSet>> {
        let mut by_index: BTreeMap<i64, PartialResultSet> = BTreeMap::new();

        let result: RawResult<Vec<RawResultSet>> = async {
            while let Some(part) = self.recv_part().await? {
                self.ingest_part(&part)?;
                Self::append_part_to_index(&mut by_index, part)?;
            }

            Ok(by_index
                .into_values()
                .map(|partial| RawResultSet {
                    columns: partial.columns,
                    rows: partial.rows,
                    truncated: partial.truncated,
                })
                .collect())
        }
        .await;

        if result.is_err() {
            self.cancel();
        }
        result
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

    pub async fn next_result_set(&mut self) -> RawResult<Option<(RawResultSet, Option<String>)>> {
        if !matches!(self.state, QueryResponseState::Active(_)) {
            return Ok(None);
        }

        let mut columns = Vec::new();
        let mut rows = Vec::new();
        let mut truncated = false;
        let mut tx_id = None;

        loop {
            let target_index = self.next_index;
            let Some(part) = self.recv_part().await? else {
                if rows.is_empty() && columns.is_empty() {
                    return Ok(None);
                }
                self.next_index += 1;
                return Ok(Some((
                    RawResultSet {
                        columns,
                        rows,
                        truncated,
                    },
                    tx_id,
                )));
            };

            let tx_id_from_part = self.ingest_part(&part)?;
            if tx_id_from_part.is_some() {
                tx_id = tx_id_from_part;
            }

            if part.result_set_index < target_index {
                warn!(
                    got = part.result_set_index,
                    expected = target_index,
                    "dropping stream part with stale result_set_index"
                );
                continue;
            }

            if part.result_set_index > target_index {
                if rows.is_empty() && columns.is_empty() {
                    if part.result_set_index > self.next_index + 1 {
                        warn!(
                            from = self.next_index,
                            to = part.result_set_index,
                            "skipping result set indices in stream"
                        );
                    }
                    self.next_index = part.result_set_index;
                } else {
                    self.set_pending_part(part);
                    self.next_index += 1;
                    return Ok(Some((
                        RawResultSet {
                            columns,
                            rows,
                            truncated,
                        },
                        tx_id,
                    )));
                }
            }

            append_rows_from_part(&mut columns, &mut rows, &mut truncated, part)?;
        }
    }

    pub fn take_captured_tx_id(&mut self) -> Option<String> {
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

    async fn drain_to_end(&mut self) -> RawResult<()> {
        let result = async {
            while let Some(part) = self.recv_part().await? {
                self.ingest_part(&part)?;
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            self.cancel();
        }
        result
    }

    pub async fn close(&mut self) -> RawResult<StreamCloseMeta> {
        self.drain_to_end().await?;
        Ok(StreamCloseMeta {
            tx_id: self.metadata.tx_id.take(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc_wrapper::raw_table_service::value::{RawResultSet, RawValue};
    use ydb_grpc::ydb_proto::query::ExecuteQueryResponsePart;
    use ydb_grpc::ydb_proto::status_ids::StatusCode;

    fn part_with_row(index: i64, column: &str, value: i64) -> ExecuteQueryResponsePart {
        let col_type = crate::grpc_wrapper::raw_table_service::value::r#type::RawType::Int64.into();
        let row = ydb_grpc::ydb_proto::Value {
            items: vec![RawValue::Int64(value).into()],
            ..Default::default()
        };
        part_with_rows(
            index,
            Some(vec![ydb_grpc::ydb_proto::Column {
                name: column.to_string(),
                r#type: Some(col_type),
            }]),
            vec![row],
        )
    }

    fn part_with_rows(
        index: i64,
        columns: Option<Vec<ydb_grpc::ydb_proto::Column>>,
        rows: Vec<ydb_grpc::ydb_proto::Value>,
    ) -> ExecuteQueryResponsePart {
        ExecuteQueryResponsePart {
            status: StatusCode::Success as i32,
            issues: vec![],
            result_set_index: index,
            result_set: columns.map(|columns| ydb_grpc::ydb_proto::ResultSet {
                columns,
                rows,
                truncated: false,
                ..Default::default()
            }),
            exec_stats: None,
            tx_meta: None,
        }
    }

    fn error_part(index: i64) -> ExecuteQueryResponsePart {
        ExecuteQueryResponsePart {
            status: StatusCode::BadRequest as i32,
            issues: vec![],
            result_set_index: index,
            result_set: None,
            exec_stats: None,
            tx_meta: None,
        }
    }

    fn metadata_only_part(index: i64) -> ExecuteQueryResponsePart {
        ExecuteQueryResponsePart {
            status: StatusCode::Success as i32,
            issues: vec![],
            result_set_index: index,
            result_set: None,
            exec_stats: None,
            tx_meta: None,
        }
    }

    fn transaction_part(id: &str) -> ExecuteQueryResponsePart {
        ExecuteQueryResponsePart {
            status: StatusCode::Success as i32,
            issues: vec![],
            result_set_index: 0,
            result_set: None,
            exec_stats: None,
            tx_meta: Some(ydb_grpc::ydb_proto::query::TransactionMeta { id: id.to_string() }),
        }
    }

    fn row_values(set: &RawResultSet) -> Vec<i64> {
        set.rows
            .iter()
            .map(|row| match row.first() {
                Some(RawValue::Int64(v)) => *v,
                other => panic!("unexpected cell: {other:?}"),
            })
            .collect()
    }

    #[tokio::test]
    async fn materialize_all_result_sets_reassembles_interleaved_parts() {
        // Server may deliver RS0/RS1 parts out of order when concurrent_result_sets=true.
        let mut stream = ExecuteQueryStream::from_test_parts(vec![
            part_with_row(0, "a", 10),
            part_with_row(1, "b", 20),
            part_with_row(0, "a", 11),
            part_with_row(1, "b", 21),
        ]);

        let sets = stream
            .materialize_all_result_sets()
            .await
            .expect("materialize stream");

        assert_eq!(sets.len(), 2);
        assert_eq!(row_values(&sets[0]), vec![10, 11]);
        assert_eq!(row_values(&sets[1]), vec![20, 21]);
    }

    #[tokio::test]
    async fn materialize_all_result_sets_accepts_empty_continuation_parts() {
        let col_type = crate::grpc_wrapper::raw_table_service::value::r#type::RawType::Int64.into();
        let row = |v: i64| ydb_grpc::ydb_proto::Value {
            items: vec![RawValue::Int64(v).into()],
            ..Default::default()
        };
        let columns = vec![ydb_grpc::ydb_proto::Column {
            name: "a".to_string(),
            r#type: Some(col_type),
        }];

        let mut stream = ExecuteQueryStream::from_test_parts(vec![
            part_with_rows(0, Some(columns.clone()), vec![row(10)]),
            part_with_rows(0, None, vec![]),
            part_with_rows(0, Some(vec![]), vec![row(11)]),
        ]);

        let sets = stream
            .materialize_all_result_sets()
            .await
            .expect("materialize stream");

        assert_eq!(sets.len(), 1);
        assert_eq!(row_values(&sets[0]), vec![10, 11]);
        assert!(matches!(stream.state, QueryResponseState::Exhausted));
    }

    #[tokio::test]
    async fn materialize_all_result_sets_propagates_part_errors() {
        let mut stream =
            ExecuteQueryStream::from_test_parts(vec![part_with_row(0, "a", 10), error_part(1)]);

        let err = stream
            .materialize_all_result_sets()
            .await
            .expect_err("expected part status error");

        assert!(matches!(
            err,
            crate::grpc_wrapper::raw_errors::RawError::YdbStatus(_)
        ));
        assert!(matches!(stream.state, QueryResponseState::Cancelled));
    }

    #[tokio::test]
    async fn materialize_all_result_sets_skips_metadata_only_parts() {
        let mut stream = ExecuteQueryStream::from_test_parts(vec![
            metadata_only_part(0),
            part_with_row(0, "a", 10),
            metadata_only_part(1),
        ]);

        let sets = stream
            .materialize_all_result_sets()
            .await
            .expect("materialize stream");

        assert_eq!(sets.len(), 1);
        assert_eq!(row_values(&sets[0]), vec![10]);
    }

    #[tokio::test]
    async fn next_result_set_advances_until_the_stream_is_exhausted() {
        let mut stream = ExecuteQueryStream::from_test_parts(vec![
            part_with_row(0, "a", 10),
            part_with_row(0, "a", 11),
            part_with_row(1, "b", 20),
        ]);

        let first = stream
            .next_result_set()
            .await
            .expect("read first result set")
            .expect("first result set");
        let second = stream
            .next_result_set()
            .await
            .expect("read second result set")
            .expect("second result set");

        assert_eq!(row_values(&first.0), vec![10, 11]);
        assert_eq!(row_values(&second.0), vec![20]);
        assert!(
            stream
                .next_result_set()
                .await
                .expect("observe end of stream")
                .is_none()
        );
        assert!(matches!(stream.state, QueryResponseState::Exhausted));
    }

    #[tokio::test]
    async fn close_drains_unread_parts_before_reporting_success() {
        let mut stream = ExecuteQueryStream::from_test_parts(vec![
            part_with_row(0, "a", 10),
            transaction_part("tx-1"),
        ]);

        let metadata = stream.close().await.expect("close stream");

        assert_eq!(metadata.tx_id.as_deref(), Some("tx-1"));
        assert!(matches!(stream.state, QueryResponseState::Exhausted));
    }

    #[tokio::test]
    async fn close_cancels_after_an_unread_part_error() {
        let mut stream =
            ExecuteQueryStream::from_test_parts(vec![part_with_row(0, "a", 10), error_part(0)]);

        let error = stream.close().await.expect_err("close must validate parts");

        assert!(matches!(
            error,
            crate::grpc_wrapper::raw_errors::RawError::YdbStatus(_)
        ));
        assert!(matches!(stream.state, QueryResponseState::Cancelled));
    }
}
