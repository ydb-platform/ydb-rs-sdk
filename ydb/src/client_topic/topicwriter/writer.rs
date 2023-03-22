// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.

use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::YdbResult;

#[allow(dead_code)]
pub struct TopicWriter {
    topic_path: String,
    writer_options: TopicWriterOptions,
}

impl TopicWriter {
    pub fn new(topic_path: String, writer_options: Option<TopicWriterOptions>) -> Self {
        Self {
            topic_path,
            writer_options: writer_options.unwrap_or_default(),
        }
    }

    pub async fn write_message(&self, _message: TopicWriterMessage) -> YdbResult<()> {
        unimplemented!("prototype")
    }

    pub async fn write_messages_bulk(&self, _messages: Vec<TopicWriterMessage>) -> YdbResult<()> {
        unimplemented!("prototype")
    }
}
