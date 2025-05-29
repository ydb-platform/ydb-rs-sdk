use std::collections::HashMap;
use std::time;
use crate::client_topic::topicreader::cancelation_token::YdbCancellationToken;
use crate::client_topic::topicreader::reader::TopicReaderCommitMarker;
use crate::YdbResult;

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct TopicReaderBatch {
    pub messages: Vec<TopicReaderMessage>,

    commit_marker: TopicReaderCommitMarker,
}

impl TopicReaderBatch {
    pub fn get_commit_marker(&self) -> TopicReaderCommitMarker {
        return self.commit_marker.clone();
    }
}

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct TopicReaderMessage {
    pub seq_no: i64,
    pub created_at: time::SystemTime,
    pub offset: i64,
    pub written_at: time::SystemTime,
    pub uncompressed_size: i64, // as sent by sender, server/sdk doesn't check the field. It may be empty or wrong.
    pub cancellation_token: YdbCancellationToken,

    commit_marker: TopicReaderCommitMarker,
}

impl TopicReaderMessage {
    pub(crate) fn get_topic_path(&self) -> String {
        todo!()
    }
}

impl TopicReaderMessage{
    pub async fn read_data(&self) -> YdbResult<&Vec<u8>> {
        unimplemented!()
    }

    pub fn get_producer_id(&self)-> String {
        unimplemented!()
    }

    pub fn get_commit_marker(&self)-> TopicReaderCommitMarker{
        self.commit_marker.clone()
    }

    fn get_write_session_metadata(&self) -> HashMap<String, String> {
        unimplemented!();
    }

    fn get_message_metadata(&self) -> HashMap<String, String> {
        unimplemented!()
    }
}
