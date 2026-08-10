use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use crate::YdbError;
use crate::byte_units::MiB;
use crate::client_topic::compression::{CodecSelection, CompressionEncoder};
use crate::client_topic::topicwriter::partitioning::PartitioningStrategy;
use crate::retry_settings::RetrySettings;

#[derive(Clone, Copy)]
pub(crate) struct InflightLimits {
    messages: NonZeroUsize,
    bytes: NonZeroUsize,
}

impl InflightLimits {
    pub(crate) fn messages(self) -> NonZeroUsize {
        self.messages
    }

    pub(crate) fn bytes(self) -> NonZeroUsize {
        self.bytes
    }
}

#[derive(Clone, Copy)]
pub(crate) struct AutoFlushSettings {
    messages: NonZeroUsize,
    bytes: NonZeroUsize,
    interval: Duration,
}

impl AutoFlushSettings {
    pub(crate) fn messages(self) -> usize {
        self.messages.get()
    }

    pub(crate) fn bytes(self) -> usize {
        self.bytes.get()
    }

    pub(crate) fn interval(self) -> Duration {
        self.interval
    }
}

#[derive(Clone, Copy)]
pub(crate) struct WriterFlowControl {
    inflight: InflightLimits,
    auto_flush: AutoFlushSettings,
}

impl WriterFlowControl {
    pub(crate) fn inflight(self) -> InflightLimits {
        self.inflight
    }

    pub(crate) fn auto_flush(self) -> AutoFlushSettings {
        self.auto_flush
    }
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

    // automatic flushing
    /// Number of buffered messages that triggers an automatic flush. Must be greater than zero
    /// and must not exceed `max_inflight_messages`. The default is **1,000**.
    #[builder(default = 1000)]
    pub(crate) auto_flush_messages: usize,
    /// Total payload size of buffered messages that triggers an automatic flush. Must be greater
    /// than zero and must not exceed `max_inflight_bytes`. The default is **20 MiB**.
    #[builder(default = 20 * MiB)]
    pub(crate) auto_flush_bytes: usize,
    /// Maximum interval before a partial batch is flushed automatically. The default is **1 ms**.
    #[builder(default = Duration::from_millis(1))]
    pub(crate) auto_flush_interval: Duration,

    // write buffer limits
    /// Maximum number of messages accepted by the writer but not yet acknowledged by the
    /// server. Writes wait for capacity after reaching this limit. Must be greater than zero.
    /// The default is **1,000**.
    #[builder(default = 1000)]
    pub(crate) max_inflight_messages: usize,
    /// Soft limit on the total payload size of messages accepted by the writer but not yet
    /// acknowledged by the server. When there is insufficient byte capacity for a new message,
    /// the write waits until capacity becomes available.
    ///
    /// A single message larger than this limit is still accepted to guarantee progress. As a
    /// result, the actual payload size in flight may temporarily exceed the configured limit.
    ///
    /// Must be greater than zero. The default is **20 MiB**.
    #[builder(default = 20 * MiB)]
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

impl TryFrom<&TopicWriterOptions> for WriterFlowControl {
    type Error = YdbError;

    fn try_from(options: &TopicWriterOptions) -> Result<Self, Self::Error> {
        let inflight_messages = NonZeroUsize::new(options.max_inflight_messages)
            .ok_or_else(|| YdbError::custom("max_inflight_messages must be greater than zero"))?;
        let inflight_bytes = NonZeroUsize::new(options.max_inflight_bytes)
            .ok_or_else(|| YdbError::custom("max_inflight_bytes must be greater than zero"))?;
        let auto_flush_messages = NonZeroUsize::new(options.auto_flush_messages)
            .ok_or_else(|| YdbError::custom("auto_flush_messages must be greater than zero"))?;
        let auto_flush_bytes = NonZeroUsize::new(options.auto_flush_bytes)
            .ok_or_else(|| YdbError::custom("auto_flush_bytes must be greater than zero"))?;

        if auto_flush_messages > inflight_messages {
            return Err(YdbError::custom(format!(
                "auto_flush_messages must not exceed max_inflight_messages: auto_flush_messages={}, max_inflight_messages={}",
                auto_flush_messages, inflight_messages,
            )));
        }
        if auto_flush_bytes > inflight_bytes {
            return Err(YdbError::custom(format!(
                "auto_flush_bytes must not exceed max_inflight_bytes: auto_flush_bytes={}, max_inflight_bytes={}",
                auto_flush_bytes, inflight_bytes,
            )));
        }

        Ok(Self {
            inflight: InflightLimits {
                messages: inflight_messages,
                bytes: inflight_bytes,
            },
            auto_flush: AutoFlushSettings {
                messages: auto_flush_messages,
                bytes: auto_flush_bytes,
                interval: options.auto_flush_interval,
            },
        })
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

        assert!(WriterFlowControl::try_from(&options).is_err());
    }

    #[test]
    fn rejects_zero_max_inflight_bytes() {
        let options = TopicWriterOptions::builder()
            .topic_path("topic")
            .max_inflight_bytes(0)
            .build();

        assert!(WriterFlowControl::try_from(&options).is_err());
    }

    #[test]
    fn rejects_zero_auto_flush_messages() {
        let options = TopicWriterOptions::builder()
            .topic_path("topic")
            .auto_flush_messages(0)
            .build();

        assert!(WriterFlowControl::try_from(&options).is_err());
    }

    #[test]
    fn rejects_zero_auto_flush_bytes() {
        let options = TopicWriterOptions::builder()
            .topic_path("topic")
            .auto_flush_bytes(0)
            .build();

        assert!(WriterFlowControl::try_from(&options).is_err());
    }

    #[test]
    fn rejects_auto_flush_messages_above_inflight_limit() {
        let options = TopicWriterOptions::builder()
            .topic_path("topic")
            .auto_flush_messages(11)
            .max_inflight_messages(10)
            .build();

        assert!(WriterFlowControl::try_from(&options).is_err());
    }

    #[test]
    fn rejects_auto_flush_bytes_above_inflight_limit() {
        let options = TopicWriterOptions::builder()
            .topic_path("topic")
            .auto_flush_bytes(11)
            .max_inflight_bytes(10)
            .build();

        assert!(WriterFlowControl::try_from(&options).is_err());
    }
}
