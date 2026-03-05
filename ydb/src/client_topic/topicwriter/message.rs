use std::time;

use derive_builder::Builder;
use tokio::sync::oneshot;

use crate::{
    client_topic::topicwriter::message_write_status::MessageWriteStatus, errors, YdbResult,
};

#[derive(Builder)]
#[builder(build_fn(error = "errors::YdbError", validate = "Self::validate"))]
#[allow(dead_code)]
pub struct TopicWriterMessage {
    #[builder(default = "None")]
    pub(crate) seq_no: Option<i64>,
    #[builder(default = "time::SystemTime::now()")]
    pub(crate) created_at: time::SystemTime,

    pub(crate) data: Vec<u8>,
}

impl TopicWriterMessageBuilder {
    fn validate(&self) -> YdbResult<()> {
        Ok(())
    }
}

pub(crate) struct TopicWriterMessageWithAck {
    pub(crate) message: TopicWriterMessage,
    pub(crate) ack: Option<oneshot::Sender<MessageWriteStatus>>,
}
