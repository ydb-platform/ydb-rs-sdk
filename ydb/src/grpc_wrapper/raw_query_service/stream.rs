use std::time::Duration;

use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_query_service::execute_query::{
    append_rows_from_part, check_part, stats_from_part, tx_id_from_part,
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
    captured_tx_id: Option<String>,
    finished: bool,
    stats: Option<Duration>,
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
        }
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
