use crate::client_topic::list_types::Codec;
use crate::errors;
use derive_builder::Builder;
use prost::bytes::Bytes;
use std::collections::HashMap;
use std::time::Duration;

type EncoderFunc = fn(Bytes) -> Bytes;

#[allow(dead_code)]
#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicWriterOptions {
    pub topic_path: String,

    #[builder(setter(strip_option), default)]
    pub(crate) producer_id: Option<String>,
    #[builder(setter(strip_option), default)]
    pub(crate) session_metadata: Option<HashMap<String, String>>,
    #[builder(default = "true")]
    pub(crate) auto_seq_no: bool,
    #[builder(default = "true")]
    pub(crate) auto_created_at: bool,
    #[builder(default = "10")]
    pub(crate) write_request_messages_chunk_size: usize,
    #[builder(default = "Duration::from_secs(1)")]
    pub(crate) write_request_send_messages_period: Duration,
    #[builder(setter(strip_option), default)]
    pub(crate) codec: Option<Codec>, // in case of no specified codec, codec is auto-selected
    #[builder(setter(strip_option), default)]
    pub(crate) custom_encoders: Option<HashMap<Codec, EncoderFunc>>,

    #[builder(default = "TopicWriterConnectionOptionsBuilder::default().build()?")]
    pub(crate) connection_options: TopicWriterConnectionOptions,
}

#[allow(dead_code)]
#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicWriterConnectionOptions {
    #[builder(setter(strip_option), default)]
    pub(crate) connection_timeout: Option<Duration>,
    #[builder(setter(strip_option), default)]
    pub(crate) max_message_size_bytes: Option<i32>,
    #[builder(setter(strip_option), default)]
    pub(crate) max_buffer_messages_count: Option<i32>,
    #[builder(setter(strip_option), default)]
    pub(crate) update_token_interval: Option<Duration>,

    #[builder(default = "TopicWriterRetrySettingsBuilder::default().build()?")]
    retry_settings: TopicWriterRetrySettings,
}

#[allow(dead_code)]
#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicWriterRetrySettings {
    #[builder(setter(strip_option), default)]
    start_timeout: Option<Duration>,
}
