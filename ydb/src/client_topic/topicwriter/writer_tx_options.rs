use std::sync::Arc;

use derive_builder::Builder;

use crate::client_topic::compression::{CodecSelection, CompressionEncoder};
use crate::client_topic::topicwriter::partitioning::PartitioningStrategy;
use crate::retry::NoRetrier;
use crate::{TopicWriterOptions, errors};

#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicWriterTxOptions {
    pub topic_path: String,

    /// Producer identifier used for server-side ordering and deduplication.
    /// A random UUID is generated when this option is omitted.
    #[builder(setter(into, strip_option), default)]
    pub(crate) producer_id: Option<String>,

    /// Selects the topic partition for this transactional writer.
    #[builder(default)]
    pub(crate) partitioning: PartitioningStrategy,

    #[builder(default)]
    pub(crate) codec_selector: CodecSelection,
    #[builder(setter(custom), default)]
    pub(crate) extra_encoders: Vec<Arc<dyn CompressionEncoder>>,
}

impl TopicWriterTxOptionsBuilder {
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

impl TopicWriterTxOptions {
    pub(crate) fn into_non_tx_options(self) -> TopicWriterOptions {
        let mut options = TopicWriterOptions::builder()
            .topic_path(self.topic_path)
            .maybe_producer_id(self.producer_id)
            .partitioning(self.partitioning)
            // Current WriterTx should not reconnect
            .retrier(Arc::new(NoRetrier {}))
            .codec_selector(self.codec_selector)
            .build();

        options.extra_encoders = self.extra_encoders;

        options
    }
}
