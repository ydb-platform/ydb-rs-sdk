use std::sync::Arc;

use derive_builder::Builder;

use crate::client_topic::compression::{CodecSelection, CompressionEncoder};
use crate::retry_budget::{DontRetry, RetryBudget};
use crate::{TopicWriterOptions, errors};

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

impl TopicWriterTxOptions {
    pub(crate) fn into_non_tx_options(self) -> TopicWriterOptions {
        let mut options = TopicWriterOptions::builder()
            .topic_path(self.topic_path)
            // Writers in transaction should have empty producer_id!
            .producer_id("".to_string())
            // Current WriterTx should not reconnect
            .retry_budget(RetryBudget::new(DontRetry).arc())
            .codec_selector(self.codec_selector)
            .build();

        options.extra_encoders = self.extra_encoders;

        options
    }
}
