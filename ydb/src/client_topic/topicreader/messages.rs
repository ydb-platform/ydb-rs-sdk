use crate::client_topic::topicreader::cancelation_token::YdbCancellationToken;
use crate::client_topic::topicreader::reader::TopicReaderCommitMarker;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawBatch;
use crate::YdbResult;
use std::collections::HashMap;
use std::time;
use std::time::SystemTime;

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct TopicReaderBatch {
    pub messages: Vec<TopicReaderMessage>,
}

impl TopicReaderBatch {
    pub(crate) fn new(raw_batch: RawBatch) -> TopicReaderBatch {
        let written_at: SystemTime = raw_batch.written_at.into();
        Self {
            messages: raw_batch
                .message_data
                .into_iter()
                .map(|message| TopicReaderMessage {
                    seq_no: message.seq_no,
                    created_at: message.created_at.map(|x| x.into()),
                    offset: message.offset,
                    written_at,
                    uncompressed_size: message.uncompressed_size,
                    producer_id: raw_batch.producer_id.clone(),
                    raw_data: Some(message.data),
                })
                .collect(),
        }
    }
}

impl TopicReaderBatch {
    pub fn get_commit_marker(&self) -> TopicReaderCommitMarker {
        unimplemented!();
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
        todo!()
    }

    fn get_write_session_metadata(&self) -> HashMap<String, String> {
        unimplemented!();
    }

    fn get_message_metadata(&self) -> HashMap<String, String> {
        unimplemented!()
    }
}
