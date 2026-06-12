use std::collections::HashMap;
use std::time::Duration;

use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_query_service::execute_query::{
    append_rows_from_part, check_part, merge_part, sets_to_vec, stats_from_part, tx_id_from_part,
};
use crate::grpc_wrapper::raw_table_service::value::RawResultSet;
use ydb_grpc::ydb_proto::query::ExecuteQueryResponsePart;

pub(crate) struct StreamCloseMeta {
    pub tx_id: Option<String>,
}

pub(crate) struct ExecuteQueryStream {
    grpc: Option<tonic::Streaming<ExecuteQueryResponsePart>>,
    next_index: i64,
    pending_part: Option<ExecuteQueryResponsePart>,
    finished: bool,
    stats: Option<Duration>,
}

impl ExecuteQueryStream {
    pub fn new(stream: tonic::Streaming<ExecuteQueryResponsePart>) -> Self {
        Self {
            grpc: Some(stream),
            next_index: 0,
            pending_part: None,
            finished: false,
            stats: None,
        }
    }

    pub fn stats(&self) -> Option<Duration> {
        self.stats
    }

    fn absorb_part_metadata(&mut self, part: &ExecuteQueryResponsePart) -> Option<String> {
        if let Some(duration) = stats_from_part(part) {
            self.stats = Some(duration);
        }
        tx_id_from_part(part)
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

            check_part(&part)?;
            if let Some(id) = self.absorb_part_metadata(&part) {
                tx_id = Some(id);
            }

            if part.result_set_index < target_index {
                continue;
            }

            if part.result_set_index > target_index {
                if rows.is_empty() && columns.is_empty() {
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
                    check_part(&next)?;
                    if let Some(id) = self.absorb_part_metadata(&next) {
                        tx_id = Some(id);
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

    #[allow(dead_code)]
    pub async fn drain_all(&mut self) -> RawResult<(Vec<RawResultSet>, Option<String>)> {
        if self.grpc.is_none() {
            self.finished = true;
            return Ok((Vec::new(), None));
        }

        let mut sets: HashMap<i64, RawResultSet> = HashMap::new();
        let mut tx_id = None;

        if let Some(part) = self.pending_part.take() {
            check_part(&part)?;
            if let Some(id) = self.absorb_part_metadata(&part) {
                tx_id = Some(id);
            }
            merge_part(&mut sets, part)?;
        }

        if let Some(stream) = self.grpc.as_mut() {
            let mut stats = self.stats;
            while let Some(part) = stream.message().await? {
                check_part(&part)?;
                if let Some(duration) = stats_from_part(&part) {
                    stats = Some(duration);
                }
                if let Some(id) = tx_id_from_part(&part) {
                    tx_id = Some(id);
                }
                merge_part(&mut sets, part)?;
            }
            self.stats = stats;
        }
        self.finished = true;
        Ok((sets_to_vec(sets), tx_id))
    }

    pub async fn close(mut self) -> RawResult<StreamCloseMeta> {
        let mut tx_id = None;

        if let Some(part) = self.pending_part.take() {
            check_part(&part)?;
            if let Some(id) = self.absorb_part_metadata(&part) {
                tx_id = Some(id);
            }
        }

        if let Some(mut stream) = self.grpc.take() {
            while let Some(part) = stream.message().await? {
                check_part(&part)?;
                if let Some(id) = self.absorb_part_metadata(&part) {
                    tx_id = Some(id);
                }
            }
        }

        Ok(StreamCloseMeta { tx_id })
    }
}
