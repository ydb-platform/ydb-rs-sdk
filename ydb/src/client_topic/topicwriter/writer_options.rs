use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use crate::client_topic::compression::{CodecSelection, CompressionEncoder};
use crate::client_topic::topicwriter::partitioning::PartitioningStrategy;
use crate::retry_settings::RetrySettings;
use crate::{YdbError, YdbResult};

pub(crate) const DEFAULT_MAX_INFLIGHT_MESSAGES: usize = 1000;
pub(crate) const DEFAULT_MAX_INFLIGHT_BYTES: usize = 20 * 1024 * 1024;

pub(crate) struct InflightLimits {
    pub(crate) messages: NonZeroUsize,
    pub(crate) bytes: NonZeroUsize,
}

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

    // write buffer limits
    /// Maximum number of messages accepted by the writer but not yet acknowledged by the
    /// server. Writes wait for capacity after reaching this limit. Must be greater than zero.
    /// The default is **1,000**.
    #[builder(default = DEFAULT_MAX_INFLIGHT_MESSAGES)]
    pub(crate) max_inflight_messages: usize,
    /// Maximum total payload size of messages accepted by the writer but not yet acknowledged by
    /// the server. Writes wait for capacity after reaching this limit. The default is **20 MiB**.
    /// A single larger message is allowed and consumes the entire byte capacity until it is
    /// acknowledged. Must be greater than zero.
    #[builder(default = DEFAULT_MAX_INFLIGHT_BYTES)]
    pub(crate) max_inflight_bytes: usize,

    #[builder(default = RetrySettings::with_default_backoff(), setters(vis = "pub(crate)"))]
    pub(crate) retry_settings: RetrySettings,
}

impl<S: topic_writer_options_builder::State> TopicWriterOptionsBuilder<S> {
    pub fn add_encoder<E: CompressionEncoder + 'static>(mut self, encoder: E) -> Self {
        self.extra_encoders.push(Arc::new(encoder));
        self
    }
}

impl TopicWriterOptions {
    pub(crate) fn inflight_limits(&self) -> YdbResult<InflightLimits> {
        let messages = NonZeroUsize::new(self.max_inflight_messages)
            .ok_or_else(|| YdbError::custom("max_inflight_messages must be greater than zero"))?;
        let bytes = NonZeroUsize::new(self.max_inflight_bytes)
            .ok_or_else(|| YdbError::custom("max_inflight_bytes must be greater than zero"))?;

        Ok(InflightLimits { messages, bytes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_max_inflight_messages() {
        let options = TopicWriterOptions::builder()
            .topic_path("topic")
            .max_inflight_messages(0)
            .build();

        let err = options.inflight_limits().err().unwrap();
        assert!(err.to_string().contains("max_inflight_messages"));
    }

    #[test]
    fn rejects_zero_max_inflight_bytes() {
        let options = TopicWriterOptions::builder()
            .topic_path("topic")
            .max_inflight_bytes(0)
            .build();

        let err = options.inflight_limits().err().unwrap();
        assert!(err.to_string().contains("max_inflight_bytes"));
    }
}
