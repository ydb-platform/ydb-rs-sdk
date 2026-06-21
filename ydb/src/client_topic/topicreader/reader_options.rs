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
    #[builder(default)]
    pub(crate) custom_decoders: Vec<Arc<dyn CompressionDecoder>>,
    #[builder(default = "ErrorHandlingStrategy::FailFast")]
    pub(crate) compression_error_strategy: ErrorHandlingStrategy,
}
