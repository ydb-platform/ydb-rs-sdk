use super::ordered_task_queue::OrderedTaskQueue;
use crate::client_topic::compression::codec_registry::CodecRegistry;
use crate::client_topic::compression::error_strategy::ErrorHandlingStrategy;
use crate::client_topic::list_types::Codec;
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::YdbResult;
use prost::bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

pub struct CompressionWorker {
    codec: Option<Codec>,
    codec_registry: Arc<CodecRegistry>,
    error_strategy: ErrorHandlingStrategy,
    queue: OrderedTaskQueue<Vec<TopicWriterMessage>>,
}

impl CompressionWorker {
    pub fn new(
        codec: Option<Codec>,
        codec_registry: Arc<CodecRegistry>,
        error_strategy: ErrorHandlingStrategy,
    ) -> (
        Self,
        mpsc::UnboundedReceiver<YdbResult<Vec<TopicWriterMessage>>>,
    ) {
        let (queue, receiver) = OrderedTaskQueue::new();
        (
            Self {
                codec,
                codec_registry,
                error_strategy,
                queue,
            },
            receiver,
        )
    }

    pub async fn process_batch(&self, batch: Vec<TopicWriterMessage>) -> YdbResult<()> {
        let registry = self.codec_registry.clone();
        let strategy = self.error_strategy.clone();
        let codec = self.codec;

        self.queue
            .submit(Box::new(move || {
                compress_batch(batch, &registry, &codec, &strategy)
            }))
            .await
    }
}

fn compress_batch(
    mut batch: Vec<TopicWriterMessage>,
    registry: &CodecRegistry,
    codec: &Option<Codec>,
    strategy: &ErrorHandlingStrategy,
) -> YdbResult<Vec<TopicWriterMessage>> {
    let codec = match codec {
        None => return Ok(batch),
        Some(c) => c,
    };

    for message in batch.iter_mut() {
        message
            .uncompressed_size
            .get_or_insert(message.data.len() as i64);

        let data_bytes = Bytes::from(std::mem::take(&mut message.data));
        match registry.compress(&data_bytes, codec) {
            Ok(compressed) => {
                message.data = compressed.to_vec();
                message.codec = Some(*codec);
            }
            Err(err) => match strategy {
                ErrorHandlingStrategy::FailFast => {
                    return Err(err);
                }
                ErrorHandlingStrategy::Skip => {
                    warn!(
                        "compression failed for message (seq_no: {:?}), sending as RAW: {}",
                        message.seq_no, err
                    );
                    message.data = data_bytes.to_vec();
                }
            },
        }
    }

    Ok(batch)
}
