use crate::client_topic::topicreader::reader::TopicSelectors;
use crate::errors;
use derive_builder::Builder;

#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicReaderOptions {
    pub consumer: String,
    pub topic: TopicSelectors,

    #[builder(default = "1000")]
    pub(crate) batch_size: usize,
}
