use crate::client_topic::compression::{CodecRegistry, ErrorHandlingStrategy};
use crate::client_topic::list_types::Codec;
use crate::client_topic::topicwriter::partitioning::PartitioningStrategy;
use crate::errors;
use derive_builder::Builder;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

fn default_codec_registry() -> Arc<CodecRegistry> {
    Arc::new(CodecRegistry::new())
}

#[allow(dead_code)]
#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicWriterOptions {
    pub topic_path: String,

    #[builder(setter(strip_option), default)]
    pub(crate) producer_id: Option<String>,
    #[builder(default)]
    pub(crate) partitioning: PartitioningStrategy,
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
    pub(crate) codec: Option<Codec>,
    #[builder(default = "default_codec_registry()")]
    pub(crate) codec_registry: Arc<CodecRegistry>,
    #[builder(default = "ErrorHandlingStrategy::FailFast")]
    pub(crate) compression_error_strategy: ErrorHandlingStrategy,

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
