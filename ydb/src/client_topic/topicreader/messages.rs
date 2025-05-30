use crate::client_topic::topicreader::partition_state::PartitionSession;
use crate::client_topic::topicreader::reader::TopicReaderCommitMarker;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawBatch;
use crate::YdbResult;
use std::collections::HashMap;
use std::time;
use std::time::SystemTime;

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct TopicReaderBatch {
    pub messages: Vec<TopicReaderMessage>,

    commit_marker: TopicReaderCommitMarker,
}

impl TopicReaderBatch {
    pub(crate) fn new(
        raw_batch: RawBatch,
        partition_session: &mut PartitionSession,
    ) -> TopicReaderBatch {
        let written_at: SystemTime = raw_batch.written_at.into();

        let mut batch = Self {
            commit_marker: TopicReaderCommitMarker {
                partition_session_id: partition_session.partition_session_id,
                partition_id: partition_session.partition_id,
                start_offset: partition_session.next_commit_offset_start,
                end_offset: partition_session.next_commit_offset_start,
                topic: partition_session.topic.clone(),
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
                        },
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
}

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct TopicReaderMessage {
    pub seq_no: i64,
    pub created_at: Option<time::SystemTime>,
    pub offset: i64,
    pub written_at: time::SystemTime,
    pub uncompressed_size: i64, // as sent by sender, server/sdk doesn't check the field. It may be empty or wrong.

    producer_id: String,
    raw_data: Option<Vec<u8>>,
    commit_marker: TopicReaderCommitMarker,
}

impl TopicReaderMessage {
    pub async fn read_and_take(&mut self) -> YdbResult<Option<Vec<u8>>> {
        Ok(self.raw_data.take())
    }

    pub fn get_producer_id(&self) -> &str {
        self.producer_id.as_str()
    }

    fn get_topic_path(&self) -> &str {
        todo!()
    }

    pub fn get_commit_marker(&self) -> TopicReaderCommitMarker {
        self.commit_marker.clone()
    }

    fn get_write_session_metadata(&self) -> HashMap<String, String> {
        unimplemented!();
    }

    fn get_message_metadata(&self) -> HashMap<String, String> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_topic::topicreader::partition_state::PartitionSession;
    use crate::grpc_wrapper::raw_topic_service::stream_read::messages::{RawBatch, RawMessageData};
    use std::time::SystemTime;

    #[test]
    fn test_topic_reader_batch_new() {
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

        let batch = TopicReaderBatch::new(raw_batch, &mut partition_session);

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
}
