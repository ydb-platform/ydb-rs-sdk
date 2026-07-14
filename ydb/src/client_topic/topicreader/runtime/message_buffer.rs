use std::collections::{HashMap, VecDeque};

#[cfg(test)]
use crate::client_topic::topicreader::ids::PartitionId;
use crate::client_topic::topicreader::ids::PartitionSessionId;
use crate::client_topic::topicreader::messages::{TopicReaderBatch, TopicReaderMessage};
use crate::client_topic::topicreader::partition_state::PartitionSession;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawBatch;
use crate::{YdbError, YdbResult};

use super::round_robin::RoundRobin;

pub(super) struct BufferedBatch {
    pub(super) messages: Vec<TopicReaderMessage>,
    pub(super) bytes_to_release: i64,
    pub(super) epoch: usize,
}

struct PartitionEntry {
    session: PartitionSession,
    queue: VecDeque<TopicReaderMessage>,
}

impl PartitionEntry {
    fn new(session: PartitionSession) -> Self {
        Self {
            session,
            queue: VecDeque::new(),
        }
    }
}

#[derive(Default)]
pub(super) struct MessageBuffer {
    entries: HashMap<PartitionSessionId, PartitionEntry>,
    round_robin: RoundRobin,
}

impl MessageBuffer {
    pub(super) fn start(&mut self, session: PartitionSession) -> YdbResult<()> {
        let psid = session.partition_session_id;
        if self.entries.contains_key(&psid) {
            return Err(YdbError::custom(format!(
                "topic reader duplicate start partition session {psid}"
            )));
        }

        self.entries.insert(psid, PartitionEntry::new(session));
        self.round_robin.push(psid);
        Ok(())
    }

    pub(super) fn stop(&mut self, partition_session_id: PartitionSessionId) -> bool {
        self.round_robin.remove(partition_session_id);
        self.entries.remove(&partition_session_id).is_some()
    }

    pub(super) fn is_active_session(&self, partition_session_id: PartitionSessionId) -> bool {
        self.entries.contains_key(&partition_session_id)
    }

    pub(super) fn push_raw_batch(
        &mut self,
        batch: RawBatch,
        partition_session_id: PartitionSessionId,
        reader_id: usize,
        epoch: usize,
    ) -> YdbResult<bool> {
        if batch.message_data.is_empty() {
            return Ok(false);
        }

        let batch_bytes = batch.get_read_session_size();
        let Some(partition_entry) = self.entries.get_mut(&partition_session_id) else {
            return Err(YdbError::custom(format!(
                "topic reader received messages for unopened partition session {partition_session_id}"
            )));
        };

        let batch = TopicReaderBatch::new(batch, &mut partition_entry.session, reader_id, epoch);
        let mut messages = batch.messages;
        if let Some(last) = messages.last_mut() {
            last.bytes_to_release = batch_bytes;
        }
        partition_entry.queue.extend(messages);
        Ok(true)
    }

    #[cfg(test)]
    pub(super) fn push_batch(&mut self, messages: Vec<TopicReaderMessage>) {
        for message in messages {
            let partition_session_id = message.get_commit_marker().partition_session_id;
            let partition_entry = self
                .entries
                .entry(partition_session_id)
                .or_insert_with(|| PartitionEntry::new(PartitionSession::from_message(&message)));
            partition_entry.queue.push_back(message);
            self.round_robin.push(partition_session_id);
        }
    }

    pub(super) fn pop_batch(&mut self, cap: usize) -> YdbResult<Option<BufferedBatch>> {
        for _ in 0..self.round_robin.len() {
            let Some(psid) = self.round_robin.next() else {
                return Ok(None);
            };
            let Some(partition_entry) = self.entries.get_mut(&psid) else {
                return Err(YdbError::custom(format!(
                    "topic reader round robin contains unknown partition session {psid}"
                )));
            };

            if partition_entry.queue.is_empty() {
                continue;
            }

            let take = cap.min(partition_entry.queue.len());
            let mut out = Vec::with_capacity(take);
            let mut bytes = 0;
            for _ in 0..take {
                let Some(message) = partition_entry.queue.pop_front() else {
                    break;
                };
                bytes += message.bytes_to_release;
                out.push(message);
            }

            let epoch = out
                .first()
                .ok_or_else(|| YdbError::custom("topic reader produced empty buffered batch"))?
                .get_commit_marker()
                .epoch;

            return Ok(Some(BufferedBatch {
                messages: out,
                bytes_to_release: bytes,
                epoch,
            }));
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc_wrapper::raw_common_types::Timestamp;
    use crate::grpc_wrapper::raw_topic_service::common::codecs::RawCodec;
    use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawMessageData;
    use std::collections::HashMap;
    use std::time::UNIX_EPOCH;
    use ydb_grpc::ydb_proto::topic::Codec;

    fn session(partition_session_id: i64, partition_id: i64) -> PartitionSession {
        PartitionSession {
            partition_session_id: PartitionSessionId::from_raw(partition_session_id),
            partition_id: PartitionId::from_raw(partition_id),
            topic: String::new(),
            next_commit_offset_start: 0,
        }
    }

    fn raw_batch(messages: impl IntoIterator<Item = (i64, i64)>) -> RawBatch {
        RawBatch {
            producer_id: String::new(),
            write_session_meta: HashMap::new(),
            codec: RawCodec {
                code: i32::from(Codec::Raw),
            },
            written_at: Timestamp::from(UNIX_EPOCH),
            message_data: messages
                .into_iter()
                .map(|(offset, read_session_size_bytes)| RawMessageData {
                    offset,
                    seq_no: offset,
                    created_at: None,
                    uncompressed_size: 0,
                    data: Vec::new(),
                    read_session_size_bytes,
                })
                .collect(),
        }
    }

    #[test]
    fn pop_batch_round_robins_between_sessions() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 1)).unwrap();
        buffer.start(session(2, 2)).unwrap();
        buffer
            .push_raw_batch(
                raw_batch([(0, 1), (1, 1)]),
                PartitionSessionId::from_raw(1),
                0,
                0,
            )
            .unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), PartitionSessionId::from_raw(2), 0, 0)
            .unwrap();

        let first = buffer.pop_batch(1).unwrap().unwrap();
        let second = buffer.pop_batch(1).unwrap().unwrap();
        let third = buffer.pop_batch(1).unwrap().unwrap();

        assert_eq!(
            first.messages[0].get_commit_marker().partition_session_id,
            PartitionSessionId::from_raw(1)
        );
        assert_eq!(
            second.messages[0].get_commit_marker().partition_session_id,
            PartitionSessionId::from_raw(2)
        );
        assert_eq!(
            third.messages[0].get_commit_marker().partition_session_id,
            PartitionSessionId::from_raw(1)
        );
    }

    #[test]
    fn unopened_session_batch_returns_error() {
        let mut buffer = MessageBuffer::default();

        let err = buffer
            .push_raw_batch(raw_batch([(0, 1)]), PartitionSessionId::from_raw(1), 0, 0)
            .expect_err("batch for unopened partition session should fail");

        assert!(matches!(
            err,
            YdbError::Custom(message)
                if message
                    == "topic reader received messages for unopened partition session 1"
        ));
        assert!(buffer.pop_batch(1).unwrap().is_none());
    }

    #[test]
    fn empty_batch_is_not_added() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 1)).unwrap();

        assert!(
            !buffer
                .push_raw_batch(raw_batch([]), PartitionSessionId::from_raw(1), 0, 0)
                .unwrap()
        );
        assert!(buffer.pop_batch(1).unwrap().is_none());
    }
}
