use crate::client_topic::compression::CompressionDecoder;
use crate::client_topic::topicreader::reader::TopicSelectors;
use crate::retry::{IndefiniteRetrier, Retry};
use std::sync::Arc;

#[derive(bon::Builder, Clone)]
pub struct TopicReaderOptions {
    // `field` attrs must come first (bon constraint)
    #[builder(field)]
    pub(crate) extra_decoders: Vec<Arc<dyn CompressionDecoder>>,
    #[builder(field = true)]
    pub(crate) auto_partitioning_support: bool,

    // required
    #[builder(into)]
    pub(crate) consumer: String,
    #[builder(into)]
    pub(crate) topic: TopicSelectors,

    // internal tuning
    #[builder(default = 1000)]
    pub(crate) batch_size: usize,

    #[builder(default = Arc::new(IndefiniteRetrier {}), setters(vis = "pub(crate)"))]
    pub(crate) retrier: Arc<dyn Retry>,
}

impl<S: topic_reader_options_builder::State> TopicReaderOptionsBuilder<S> {
    pub fn add_decoder<D: CompressionDecoder + 'static>(mut self, decoder: D) -> Self {
        self.extra_decoders.push(Arc::new(decoder));
        self
    }

    /// Disables client-side handling of topic partition splits and merges.
    ///
    /// This only selects the reader's compatibility mode. It does not change
    /// the topic's auto-partitioning settings.
    pub fn disable_auto_partitioning(mut self) -> Self {
        self.auto_partitioning_support = false;
        self
    }
}
