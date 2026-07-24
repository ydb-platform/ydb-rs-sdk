use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::client_topic::compression::{CodecSelection, CompressionEncoder};
use crate::client_topic::topicwriter::partitioning::PartitioningStrategy;
use crate::retry_budget::{ArcRetrySettings, RetrySettings};

#[derive(bon::Builder, Clone)]
pub struct TopicWriterOptions {
    // `field` attrs must come first (bon constraint)
    #[builder(field)]
    pub(crate) extra_encoders: Vec<Arc<dyn CompressionEncoder>>,

    // required
    #[builder(into)]
    pub(crate) topic_path: String,

    // producer identity & routing
    pub(crate) producer_id: Option<String>,
    #[builder(default)]
    pub(crate) partitioning: PartitioningStrategy,
    pub(crate) session_metadata: Option<HashMap<String, String>>,

    // sequencing & codec
    #[builder(default = true)]
    pub(crate) auto_seq_no: bool,
    #[builder(default)]
    pub(crate) codec_selector: CodecSelection,

    // internal write-loop tuning
    #[builder(default = 1000)]
    pub(crate) write_request_messages_chunk_size: usize,
    #[builder(default = Duration::from_millis(1))]
    pub(crate) write_request_send_messages_period: Duration,
    #[builder(default = Duration::from_secs(3))]
    pub(crate) flush_timeout: Duration,

    #[builder(default = RetrySettings::default(), setters(vis = "pub(crate)"))]
    pub(crate) retry_settings: ArcRetrySettings,
}

impl<S: topic_writer_options_builder::State> TopicWriterOptionsBuilder<S> {
    pub fn add_encoder<E: CompressionEncoder + 'static>(mut self, encoder: E) -> Self {
        self.extra_encoders.push(Arc::new(encoder));
        self
    }
}
