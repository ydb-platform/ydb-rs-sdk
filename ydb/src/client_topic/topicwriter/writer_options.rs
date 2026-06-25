use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use derive_builder::Builder;

use crate::client_topic::compression::{CodecSelection, CompressionEncoder};
use crate::client_topic::topicwriter::partitioning::PartitioningStrategy;
use crate::errors;
use crate::retry::{IndefiniteRetrier, Retry};

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
    pub(crate) codec_selector: CodecSelection,
    #[builder(setter(custom), default)]
    pub(crate) extra_encoders: Vec<Arc<dyn CompressionEncoder>>,
}

impl TopicWriterOptionsBuilder {
    pub fn add_encoder<E>(&mut self, encoder: E) -> &mut Self
    where
        E: CompressionEncoder + 'static,
    {
        self.extra_encoders
            .get_or_insert_default()
            .push(Arc::new(encoder));
        self
    }
}
