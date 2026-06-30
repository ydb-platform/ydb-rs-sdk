use std::sync::Arc;

use derive_builder::Builder;

use crate::client_topic::compression::{CodecSelection, CompressionEncoder};
use crate::retry::NoRetrier;
use crate::{errors, TopicWriterOptions, TopicWriterOptionsBuilder};

#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicWriterTxOptions {
    pub topic_path: String,

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

impl TryFrom<TopicWriterTxOptions> for TopicWriterOptions {
    type Error = errors::YdbError;

    fn try_from(value: TopicWriterTxOptions) -> Result<Self, Self::Error> {
        let mut options = TopicWriterOptionsBuilder::default()
            .topic_path(value.topic_path)
            // Writers in transaction should have empty producer_id!
            .producer_id("".to_string())
            // Current WriterTx should not reconnect
            .retrier(Arc::new(NoRetrier {}))
            .codec_selector(value.codec_selector)
            .build()?;

        options.extra_encoders = value.extra_encoders;

        Ok(options)
    }
}
