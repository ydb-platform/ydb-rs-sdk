use crate::client_topic::topicreader::partition_state::PartitionSession;
use crate::client_topic::topicreader::reader::TopicReaderCommitMarker;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawBatch;
use crate::{Codec, YdbResult};
use std::time;
use std::time::SystemTime;

/// Internal pre-decompression batch carried from `grpc_stream` to `decompressor`.
/// Each `MessageBatch` is one `RawBatch`'s worth of messages plus the codec they
/// were compressed with.
pub(super) struct MessageBatch {
    pub(super) messages: Vec<TopicReaderMessage>,
    pub(super) codec: Codec,
}

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
#[derive(Debug)]
pub struct TopicReaderBatch {
    pub messages: Vec<TopicReaderMessage>,

    commit_marker: TopicReaderCommitMarker,
}

impl TopicReaderBatch {
    pub(crate) fn new(
        raw_batch: RawBatch,
        partition_session: &mut PartitionSession,
        epoch: usize,
    ) -> TopicReaderBatch {
        let written_at: SystemTime = raw_batch.written_at.into();

        let mut batch = Self {
            commit_marker: TopicReaderCommitMarker {
                partition_session_id: partition_session.partition_session_id,
                partition_id: partition_session.partition_id,
                start_offset: partition_session.next_commit_offset_start,
                end_offset: partition_session.next_commit_offset_start,
                topic: partition_session.topic.clone(),
                epoch,
            },

            messages: raw_batch
                .message_data
                .into_iter()
                .map(|message| {
                    let start_commit_offset = partition_session.next_commit_offset_start;
                    partition_session.next_commit_offset_start = message.offset + 1;

                    TopicReaderMessage {
                        seq_no: message.seq_no,
                        created_at: message.created_at.map(|x| x.into()),
                        offset: message.offset,
                        written_at,
                        uncompressed_size: message.uncompressed_size,
                        producer_id: raw_batch.producer_id.clone(),
                        raw_data: Some(message.data),

                        commit_marker: TopicReaderCommitMarker {
                            partition_session_id: partition_session.partition_session_id,
                            partition_id: partition_session.partition_id,
                            start_offset: start_commit_offset,
                            end_offset: message.offset + 1,
                            topic: partition_session.topic.clone(),
                            epoch,
                        },

                        bytes_to_release: 0,
                    }
                })
                .collect(),
        };

        if let Some(last) = batch.messages.last() {
            batch.commit_marker.end_offset = last.commit_marker.end_offset
        }

        batch
    }
}

impl TopicReaderBatch {
    pub fn get_commit_marker(&self) -> TopicReaderCommitMarker {
        self.commit_marker.clone()
    }

    pub(crate) fn from_messages(messages: Vec<TopicReaderMessage>) -> Self {
        let first = messages
            .first()
            .expect("TopicReaderBatch::from_messages called with empty vector");
        let last = messages
            .last()
            .expect("TopicReaderBatch::from_messages called with empty vector");
        let commit_marker = TopicReaderCommitMarker {
            partition_session_id: first.commit_marker.partition_session_id,
            partition_id: first.commit_marker.partition_id,
            start_offset: first.commit_marker.start_offset,
            end_offset: last.commit_marker.end_offset,
            topic: first.commit_marker.topic.clone(),
            epoch: first.commit_marker.epoch,
        };
        TopicReaderBatch {
            messages,
            commit_marker,
        }
    }
}

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
#[derive(Debug)]
pub struct TopicReaderMessage {
    pub seq_no: i64,
    pub created_at: Option<time::SystemTime>,
    pub offset: i64,
    pub written_at: time::SystemTime,
    pub uncompressed_size: i64, // as sent by sender, server/sdk doesn't check the field. It may be empty or wrong.

    producer_id: String,
    pub(crate) raw_data: Option<Vec<u8>>,
    pub(crate) commit_marker: TopicReaderCommitMarker,

    // Non-zero only on the last message of a server ReadResponse; carries the
    // response's bytes_size for flow-control (sent back as ReadRequest).
    pub(crate) bytes_to_release: i64,
}

impl TopicReaderMessage {
    pub async fn read_and_take(&mut self) -> YdbResult<Option<Vec<u8>>> {
        Ok(self.raw_data.take())
    }

    pub fn get_producer_id(&self) -> &str {
        self.producer_id.as_str()
    }

    pub fn get_commit_marker(&self) -> TopicReaderCommitMarker {
        self.commit_marker.clone()
    }

    pub fn get_topic(&self) -> &str {
        &self.commit_marker.topic
    }

    pub fn get_partition_id(&self) -> i64 {
        self.commit_marker.partition_id
    }

    #[cfg(test)]
    pub(crate) fn test_message(epoch: usize, bytes_to_release: i64) -> Self {
        Self {
            seq_no: 0,
            created_at: None,
            offset: 0,
            written_at: time::SystemTime::UNIX_EPOCH,
            uncompressed_size: 0,
            producer_id: String::new(),
            raw_data: Some(vec![]),
            commit_marker: TopicReaderCommitMarker {
                partition_session_id: 1,
                partition_id: 1,
                start_offset: 0,
                end_offset: 1,
                topic: "test".into(),
                epoch,
            },
            bytes_to_release,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_topic::topicreader::partition_state::PartitionSession;
    use crate::grpc_wrapper::raw_topic_service::stream_read::messages::{RawBatch, RawMessageData};
    use std::time::SystemTime;

    #[test]
    fn topic_reader_batch_new() {
        let mut partition_session = PartitionSession {
            partition_session_id: 123,
            partition_id: 456,
            topic: "test-topic".to_string(),
            next_commit_offset_start: 100,
        };

        let raw_batch = RawBatch {
            producer_id: "test-producer".to_string(),
            write_session_meta: std::collections::HashMap::new(),
            codec: crate::grpc_wrapper::raw_topic_service::common::codecs::RawCodec { code: 1 },
            written_at: SystemTime::now().into(),
            message_data: vec![RawMessageData {
                seq_no: 1,
                created_at: Some(SystemTime::now().into()),
                data: vec![1, 2, 3],
                uncompressed_size: 3,
                offset: 100,
                read_session_size_bytes: 0,
            }],
        };

        let batch = TopicReaderBatch::new(raw_batch, &mut partition_session, 0);

        let commit_marker = batch.get_commit_marker();
        assert_eq!(commit_marker.topic, "test-topic");
        assert_eq!(commit_marker.partition_session_id, 123);
        assert_eq!(commit_marker.partition_id, 456);
        assert_eq!(commit_marker.start_offset, 100);
        assert_eq!(commit_marker.end_offset, 101);

        assert_eq!(batch.messages.len(), 1);
        let message_commit_marker = batch.messages[0].get_commit_marker();
        assert_eq!(message_commit_marker.topic, "test-topic");
        assert_eq!(message_commit_marker.partition_session_id, 123);
        assert_eq!(message_commit_marker.partition_id, 456);
        assert_eq!(message_commit_marker.start_offset, 100);
        assert_eq!(message_commit_marker.end_offset, 101);
    }

    #[test]
    fn bytes_to_release_default_zero() {
        let mut partition_session = PartitionSession {
            partition_session_id: 1,
            partition_id: 2,
            topic: "t".to_string(),
            next_commit_offset_start: 0,
        };
        let raw_batch = RawBatch {
            producer_id: "p".to_string(),
            write_session_meta: std::collections::HashMap::new(),
            codec: crate::grpc_wrapper::raw_topic_service::common::codecs::RawCodec { code: 1 },
            written_at: SystemTime::UNIX_EPOCH.into(),
            message_data: vec![
                RawMessageData {
                    seq_no: 1,
                    created_at: None,
                    data: vec![],
                    uncompressed_size: 0,
                    offset: 0,
                    read_session_size_bytes: 0,
                },
                RawMessageData {
                    seq_no: 2,
                    created_at: None,
                    data: vec![],
                    uncompressed_size: 0,
                    offset: 1,
                    read_session_size_bytes: 0,
                },
            ],
        };
        let batch = TopicReaderBatch::new(raw_batch, &mut partition_session, 0);
        assert!(batch.messages.iter().all(|m| m.bytes_to_release == 0));
    }

    #[test]
    fn from_messages_commit_marker_spans_first_to_last() {
        let mut partition_session = PartitionSession {
            partition_session_id: 7,
            partition_id: 42,
            topic: "t-from-messages".to_string(),
            next_commit_offset_start: 100,
        };
        let raw_batch = RawBatch {
            producer_id: "p".to_string(),
            write_session_meta: std::collections::HashMap::new(),
            codec: crate::grpc_wrapper::raw_topic_service::common::codecs::RawCodec { code: 1 },
            written_at: SystemTime::UNIX_EPOCH.into(),
            message_data: (0..3)
                .map(|i| RawMessageData {
                    seq_no: i + 1,
                    created_at: None,
                    data: vec![],
                    uncompressed_size: 0,
                    offset: 100 + i,
                    read_session_size_bytes: 0,
                })
                .collect(),
        };
        let messages = TopicReaderBatch::new(raw_batch, &mut partition_session, 0).messages;

        let rebuilt = TopicReaderBatch::from_messages(messages);
        let m = rebuilt.get_commit_marker();
        assert_eq!(rebuilt.messages.len(), 3);
        assert_eq!(m.topic, "t-from-messages");
        assert_eq!(m.partition_session_id, 7);
        assert_eq!(m.partition_id, 42);
        assert_eq!(m.start_offset, 100);
        assert_eq!(m.end_offset, 103);

        let offsets: Vec<i64> = rebuilt.messages.iter().map(|x| x.offset).collect();
        assert_eq!(offsets, vec![100, 101, 102]);
    }
}
