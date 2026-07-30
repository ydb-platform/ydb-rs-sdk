use crate::client_topic::compression::CompressionDecoder;
use crate::client_topic::topicreader::reader::TopicSelectors;
use crate::retry_settings::RetrySettings;
use std::sync::Arc;

#[derive(bon::Builder, Clone)]
pub struct TopicReaderOptions {
    // `field` attrs must come first (bon constraint)
    #[builder(field)]
    pub(crate) extra_decoders: Vec<Arc<dyn CompressionDecoder>>,

    // required
    #[builder(into)]
    pub(crate) consumer: String,
    #[builder(into)]
    pub(crate) topic: TopicSelectors,

    // internal tuning
    #[builder(default = 1000)]
    pub(crate) batch_size: usize,

    #[builder(default = RetrySettings::with_default_backoff(), setters(vis = "pub(crate)"))]
    pub(crate) retry_settings: RetrySettings,
}

impl<S: topic_reader_options_builder::State> TopicReaderOptionsBuilder<S> {
    pub fn add_decoder<D: CompressionDecoder + 'static>(mut self, decoder: D) -> Self {
        self.extra_decoders.push(Arc::new(decoder));
        self
    }
}
