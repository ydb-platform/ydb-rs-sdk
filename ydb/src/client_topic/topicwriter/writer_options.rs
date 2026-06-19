use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use derive_builder::Builder;

use crate::client_topic::compression::{CodecRegistry, CodecSelection, ErrorHandlingStrategy};
use crate::client_topic::topicwriter::partitioning::PartitioningStrategy;
use crate::errors;
use crate::retry::{IndefiniteRetrier, Retry};

fn default_codec_registry() -> Arc<CodecRegistry> {
    Arc::new(CodecRegistry::new())
}

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
    #[builder(default = "1000")]
    pub(crate) write_request_messages_chunk_size: usize,
    #[builder(default = "Duration::from_millis(1)")]
    pub(crate) write_request_send_messages_period: Duration,
    #[builder(default = "Duration::from_secs(3)")]
    pub(crate) flush_timeout: Duration,

    #[builder(default = "Arc::new(IndefiniteRetrier{ })")]
    pub(crate) retrier: Arc<dyn Retry>,

    #[builder(default)]
    pub(crate) codec: CodecSelection,
    #[builder(default = "default_codec_registry()")]
    pub(crate) codec_registry: Arc<CodecRegistry>,
    #[builder(default = "ErrorHandlingStrategy::FailFast")]
    pub(crate) compression_error_strategy: ErrorHandlingStrategy,
}
