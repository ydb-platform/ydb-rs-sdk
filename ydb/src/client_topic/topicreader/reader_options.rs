use crate::client_topic::compression::{CompressionDecoder, ErrorHandlingStrategy};
use crate::client_topic::topicreader::reader::TopicSelectors;
use crate::errors;
use derive_builder::Builder;
use std::sync::Arc;

#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicReaderOptions {
    pub consumer: String,
    pub topic: TopicSelectors,

    #[builder(default = "1000")]
    pub(crate) batch_size: usize,
    #[builder(setter(custom), default)]
    pub(crate) extra_decoders: Vec<Arc<dyn CompressionDecoder>>,
    #[builder(default = "ErrorHandlingStrategy::FailFast")]
    pub(crate) compression_error_strategy: ErrorHandlingStrategy,
}

impl TopicReaderOptionsBuilder {
    pub fn add_decoder<D>(&mut self, decoder: D) -> &mut Self
    where
        D: CompressionDecoder + 'static,
    {
        self.extra_decoders
            .get_or_insert_default()
            .push(Arc::new(decoder));
        self
    }
}
