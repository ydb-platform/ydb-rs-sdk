use crate::client_topic::compression::CompressionDecoder;
use crate::client_topic::topicreader::reader::TopicSelectors;
use crate::retry::{IndefiniteRetrier, Retry};
use std::sync::Arc;

#[derive(bon::Builder)]
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

    #[builder(default = Box::new(IndefiniteRetrier {}), setters(vis = "pub(crate)"))]
    pub(crate) retrier: Box<dyn Retry>,
}

impl<S: topic_reader_options_builder::State> TopicReaderOptionsBuilder<S> {
    pub fn add_decoder<D: CompressionDecoder + 'static>(mut self, decoder: D) -> Self {
        self.extra_decoders.push(Arc::new(decoder));
        self
    }
}
