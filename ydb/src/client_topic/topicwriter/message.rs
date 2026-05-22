use crate::client_topic::list_types::Codec;
use crate::{errors, YdbResult};
use derive_builder::Builder;
use std::time;

#[derive(Builder, Debug)]
#[builder(build_fn(error = "errors::YdbError", validate = "Self::validate"))]
#[allow(dead_code)]
pub struct TopicWriterMessage {
    #[builder(default = "None")]
    pub(crate) seq_no: Option<i64>,
    #[builder(default = "time::SystemTime::now()")]
    pub(crate) created_at: time::SystemTime,

    pub(crate) data: Vec<u8>,

    #[builder(default = "None", setter(skip))]
    pub(crate) uncompressed_size: Option<i64>,
    #[builder(default = "None", setter(skip))]
    pub(crate) codec: Option<Codec>,
}

impl TopicWriterMessageBuilder {
    fn validate(&self) -> YdbResult<()> {
        Ok(())
    }
}
