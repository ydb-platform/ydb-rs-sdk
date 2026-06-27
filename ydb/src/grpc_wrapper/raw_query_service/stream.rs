use std::collections::BTreeMap;
use std::time::Duration;

use tracing::warn;

use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_query_service::execute_query::{
    append_rows_from_part, check_part, stats_from_part, tx_id_from_part,
};
use crate::grpc_wrapper::raw_table_service::value::RawResultSet;
use ydb_grpc::ydb_proto::query::ExecuteQueryResponsePart;

pub(crate) struct StreamCloseMeta {
    pub tx_id: Option<String>,
}

#[derive(Default)]
struct PartialResultSet {
    columns: Vec<crate::grpc_wrapper::raw_table_service::value::RawColumn>,
    rows: Vec<Vec<crate::grpc_wrapper::raw_table_service::value::RawValue>>,
    truncated: bool,
}

/// Holds a pooled session lease until the stream is finished.
struct SessionStreamGuard(#[allow(dead_code)] Option<Box<dyn std::any::Any + Send>>);

impl SessionStreamGuard {
    fn hold<T: Send + 'static>(value: T) -> Self {
        Self(Some(Box::new(value)))
    }
}

pub(crate) struct ExecuteQueryStream {
    grpc: Option<tonic::Streaming<ExecuteQueryResponsePart>>,
    next_index: i64,
    pending_part: Option<ExecuteQueryResponsePart>,
    captured_tx_id: Option<String>,
    finished: bool,
    stats: Option<Duration>,
    // Dropped last (after `grpc`) so the pooled lease outlives the stream.
    // `Drop` also calls `cancel()` before field destructors run.
    session_guard: SessionStreamGuard,
    #[cfg(test)]
    test_parts: Option<Vec<ExecuteQueryResponsePart>>,
}

impl Drop for ExecuteQueryStream {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl ExecuteQueryStream {
    pub fn new(stream: tonic::Streaming<ExecuteQueryResponsePart>) -> Self {
        Self {
            grpc: Some(stream),
            next_index: 0,
            pending_part: None,
            captured_tx_id: None,
            finished: false,
            stats: None,
            session_guard: SessionStreamGuard(None),
            #[cfg(test)]
            test_parts: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_test_parts(mut parts: Vec<ExecuteQueryResponsePart>) -> Self {
        parts.reverse();
        Self {
            grpc: None,
            next_index: 0,
            pending_part: None,
            captured_tx_id: None,
            finished: false,
            stats: None,
            session_guard: SessionStreamGuard(None),
            test_parts: Some(parts),
        }
    }

    pub fn with_session_guard(mut self, guard: impl Send + 'static) -> Self {
        self.session_guard = SessionStreamGuard::hold(guard);
        self
    }

    pub fn stats(&self) -> Option<Duration> {
        self.stats
    }

    fn absorb_part_metadata(&mut self, part: &ExecuteQueryResponsePart) -> Option<String> {
        if let Some(duration) = stats_from_part(part) {
            self.stats = Some(duration);
        }
        if let Some(id) = tx_id_from_part(part) {
            self.captured_tx_id = Some(id.clone());
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
        if self.finished {
            return Ok(None);
        }
        if let Some(part) = self.pending_part.take() {
            return Ok(Some(part));
        }
        #[cfg(test)]
        if let Some(parts) = &mut self.test_parts {
            return Ok(parts.pop());
        }
        match self.grpc.as_mut() {
            Some(stream) => match stream.message().await? {
                Some(part) => Ok(Some(part)),
                None => {
                    self.finished = true;
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    }

    fn append_part_to_index(
        by_index: &mut BTreeMap<i64, PartialResultSet>,
        part: ExecuteQueryResponsePart,
    ) -> RawResult<()> {
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

        while let Some(part) = self.recv_part().await? {
            self.ingest_part(&part)?;
            Self::append_part_to_index(&mut by_index, part)?;
        }

        drop(self.grpc.take());
        self.finished = true;

        Ok(by_index.into_values().map(|partial| RawResultSet {
                columns: partial.columns,
                rows: partial.rows,
                truncated: partial.truncated,
            })
            .collect())
    }

    /// Read the first response part so transaction `tx_id` is captured before iteration.
    pub async fn prime_first_part(&mut self) -> RawResult<()> {
        if self.pending_part.is_some() || self.grpc.is_none() || self.finished {
            return Ok(());
        }
        let Some(stream) = self.grpc.as_mut() else {
            return Ok(());
        };
        match stream.message().await? {
            Some(part) => {
                self.ingest_part(&part)?;
                self.pending_part = Some(part);
            }
            None => self.finished = true,
        }
        Ok(())
    }

    pub async fn next_result_set(&mut self) -> RawResult<Option<(RawResultSet, Option<String>)>> {
        if self.grpc.is_none() || self.finished {
            return Ok(None);
        }

        let mut columns = Vec::new();
        let mut rows = Vec::new();
        let mut truncated = false;
        let mut tx_id = None;

        loop {
            let target_index = self.next_index;
            let part = if let Some(part) = self.pending_part.take() {
                part
            } else {
                match self.grpc.as_mut() {
                    Some(stream) => match stream.message().await? {
                        Some(part) => part,
                        None => {
                            self.finished = true;
                            if rows.is_empty() && columns.is_empty() {
                                return Ok(None);
                            }
                            return Ok(Some((
                                RawResultSet {
                                    columns,
                                    rows,
                                    truncated,
                                },
                                tx_id,
                            )));
                        }
                    },
                    None => {
                        self.finished = true;
                        return Ok(None);
                    }
                }
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
                    self.pending_part = Some(part);
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

            let stream = self.grpc.as_mut().expect("grpc stream");
            let collecting_index = self.next_index;
            match stream.message().await? {
                Some(next) => {
                    let tx_id_from_part = self.ingest_part(&next)?;
                    if tx_id_from_part.is_some() {
                        tx_id = tx_id_from_part;
                    }
                    if next.result_set_index > collecting_index {
                        self.pending_part = Some(next);
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
                    if next.result_set_index < collecting_index {
                        warn!(
                            got = next.result_set_index,
                            expected = collecting_index,
                            "dropping stream part with stale result_set_index"
                        );
                        continue;
                    }
                    append_rows_from_part(&mut columns, &mut rows, &mut truncated, next)?;
                }
                None => {
                    self.finished = true;
                    self.next_index += 1;
                    if rows.is_empty() && columns.is_empty() {
                        return Ok(None);
                    }
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
        }
    }

    pub fn take_captured_tx_id(&mut self) -> Option<String> {
        if let Some(id) = self.captured_tx_id.take() {
            return Some(id);
        }
        self.pending_part.as_ref().and_then(tx_id_from_part)
    }

    /// Drop the gRPC stream without draining unread parts (sends RST_STREAM).
    pub fn cancel(&mut self) {
        if let Some(part) = self.pending_part.take() {
            self.absorb_part_metadata(&part);
        }
        drop(self.grpc.take());
        self.finished = true;
    }

    pub async fn close(&mut self) -> RawResult<StreamCloseMeta> {
        if let Some(part) = self.pending_part.take() {
            self.absorb_part_metadata(&part);
        }
        drop(self.grpc.take());
        self.finished = true;
        Ok(StreamCloseMeta {
            tx_id: self.captured_tx_id.take(),
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
        ExecuteQueryResponsePart {
            status: StatusCode::Success as i32,
            issues: vec![],
            result_set_index: index,
            result_set: Some(ydb_grpc::ydb_proto::ResultSet {
                columns: vec![ydb_grpc::ydb_proto::Column {
                    name: column.to_string(),
                    r#type: Some(col_type),
                }],
                rows: vec![row],
                truncated: false,
                ..Default::default()
            }),
            exec_stats: None,
            tx_meta: None,
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
}
