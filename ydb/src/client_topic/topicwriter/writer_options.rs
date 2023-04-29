// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.

use crate::client_topic::list_types::Codec;
use crate::errors;
use derive_builder::{Builder};
use prost::bytes::Bytes;
use std::collections::HashMap;

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
    auto_seq_no: bool,
    #[builder(default = "true")]
    auto_created_at: bool,
    #[builder(setter(strip_option), default)]
    codec: Option<Codec>, // in case of no specified codec, codec is auto-selected
    #[builder(setter(strip_option), default)]
    custom_encoders: Option<HashMap<Codec, EncoderFunc>>,

    #[builder(default = "TopicWriterConnectionOptionsBuilder::default().build()?")]
    connection_options: TopicWriterConnectionOptions,
}

#[allow(dead_code)]
#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicWriterConnectionOptions {
    #[builder(setter(strip_option), default)]
    connection_timeout: Option<core::time::Duration>,
    #[builder(setter(strip_option), default)]
    max_message_size_bytes: Option<i32>,
    #[builder(setter(strip_option), default)]
    max_buffer_messages_count: Option<i32>,
    #[builder(setter(strip_option), default)]
    update_token_interval: Option<core::time::Duration>,

    #[builder(default = "false")]
    wait_server_ack: bool,
    #[builder(default = "TopicWriterRetrySettingsBuilder::default().build()?")]
    retry_settings: TopicWriterRetrySettings,
}

#[allow(dead_code)]
#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicWriterRetrySettings {
    #[builder(setter(strip_option), default)]
    start_timeout: Option<core::time::Duration>
}