// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.

use crate::client_topic::list_types::Codec;
use crate::errors;
use derive_builder::{Builder};
use prost::bytes::Bytes;
use std::collections::HashMap;

type EncoderFunc = &'static dyn Fn(Bytes) -> Bytes;

#[allow(dead_code)]
#[derive(Builder)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicWriterOptions {
    #[builder(setter(strip_option))]
    producer_id: Option<String>,
    #[builder(setter(strip_option))]
    partition_id: Option<i64>,
    #[builder(setter(strip_option))]
    session_metadata: Option<HashMap<String, String>>,
    auto_seq_no: bool,
    auto_created_at: bool,
    #[builder(setter(strip_option))]
    codec: Option<Codec>, // in case of no specified codec, codec is auto-selected
    #[builder(setter(strip_option))]
    custom_encoders: Option<HashMap<Codec, EncoderFunc>>,

    connection_options: TopicWriterConnectionOptions,
}

#[allow(dead_code)]
#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicWriterConnectionOptions {
    connection_timeout: Option<core::time::Duration>,
    max_message_size_bytes: Option<i32>,
    max_buffer_messages_count: Option<i32>,
    update_token_interval: Option<core::time::Duration>,

    wait_server_ack: bool,
    retry_settings: TopicWriterRetrySettings,
}

#[allow(dead_code)]
#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicWriterRetrySettings {
    start_timeout: Option<core::time::Duration>
}

impl Default for TopicWriterOptions{
    fn default() -> Self {
        todo!()
    }
}
