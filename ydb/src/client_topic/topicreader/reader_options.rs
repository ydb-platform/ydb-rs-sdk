use crate::client_topic::compression::{
    default_executor, CodecRegistry, ErrorHandlingStrategy, Executor,
};
use crate::client_topic::topicreader::reader::TopicSelectors;
use crate::errors;
use derive_builder::Builder;
use std::sync::Arc;

fn default_codec_registry() -> Arc<CodecRegistry> {
    Arc::new(CodecRegistry::new())
}

#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicReaderOptions {
    pub consumer: String,
    pub topic: TopicSelectors,

    #[builder(default = "1000")]
    pub(crate) batch_size: usize,
    #[builder(default = "default_codec_registry()")]
    pub(crate) codec_registry: Arc<CodecRegistry>,
    #[builder(default = "ErrorHandlingStrategy::FailFast")]
    pub(crate) compression_error_strategy: ErrorHandlingStrategy,
    #[builder(default = "default_executor()")]
    pub(crate) compression_executor: Arc<dyn Executor>,
}
