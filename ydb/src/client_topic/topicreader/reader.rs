use std::collections::HashMap;
use std::time;
use std::time::{Duration, SystemTime};
use crate::client_topic::topicreader::messages::{TopicReaderBatch};
use crate::YdbResult;

pub struct TopicReader{
}

impl TopicReader {
    pub async fn read_batch(&mut self) -> YdbResult<TopicReaderBatch> {
        todo!();
    }

    pub fn commit(&mut self, commit_marker: TopicReaderCommitMarker){
        todo!()
    }

    pub(crate) async fn new(selectors: TopicSelectors) -> Self {
        todo!()
    }

}

pub struct TopicSelectors(pub Vec<TopicSelector>);

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct TopicSelector {
    pub path: String,
    pub partition_ids: Option<Vec<i64>>,
    pub read_from: Option<SystemTime>,
    pub max_time_lag: Option<Duration>,
}

impl From<String> for TopicSelectors{
    fn from(path: String) -> Self {
        TopicSelectors(vec![TopicSelector{
            path,
            partition_ids: None,
            read_from: None,
            max_time_lag: None,
        }])
    }
}

impl From<&str> for TopicSelectors{
    fn from(path: &str)-> Self {
        path.to_owned().into()
    }
}

#[derive(Clone, Debug)]
pub struct TopicReaderCommitMarker {
    partition_session_id: i64,
    start_offset: i64,
    end_offset: i64,
}
