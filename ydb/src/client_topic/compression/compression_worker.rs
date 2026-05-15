use super::ordered_task_queue::OrderedTaskQueue;
use crate::client_topic::compression::codec_registry::CodecRegistry;
use crate::client_topic::compression::codec_selector::CodecSelector;
use crate::client_topic::compression::error_strategy::ErrorHandlingStrategy;
use crate::client_topic::compression::executor::Executor;
use crate::client_topic::list_types::Codec;
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::YdbResult;
use prost::bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

pub struct CompressionWorker {
    codec_selector: CodecSelector,
    codec_registry: Arc<CodecRegistry>,
    error_strategy: ErrorHandlingStrategy,
    parallelism: usize,
    queue: OrderedTaskQueue<Vec<TopicWriterMessage>>,
}

impl CompressionWorker {
    pub fn new(
        codec: Option<Codec>,
        codec_registry: Arc<CodecRegistry>,
        error_strategy: ErrorHandlingStrategy,
        executor: Arc<dyn Executor>,
        server_codecs: Vec<Codec>,
    ) -> YdbResult<(
        Self,
        mpsc::UnboundedReceiver<YdbResult<Vec<TopicWriterMessage>>>,
    )> {
        let codec_selector = CodecSelector::new(codec, server_codecs, codec_registry.clone())?;
        let parallelism = executor.available_parallelism();
        let (queue, receiver) = OrderedTaskQueue::new(executor);
        Ok((
            Self {
                codec_selector,
                codec_registry,
                error_strategy,
                parallelism,
                queue,
            },
            receiver,
        ))
    }

    pub fn process_batch(&mut self, mut batch: Vec<TopicWriterMessage>) {
        self.codec_selector.step(&batch);
        let codec = self.codec_selector.codec();

        let chunk_size = (batch.len() / self.parallelism).max(1);
        while !batch.is_empty() {
            let chunk: Vec<TopicWriterMessage> =
                batch.drain(..chunk_size.min(batch.len())).collect();
            let registry = self.codec_registry.clone();
            let strategy = self.error_strategy.clone();

            self.queue.submit(Box::new(move || {
                compress_batch(chunk, &registry, codec, &strategy)
            }));
        }
    }
}

fn compress_batch(
    mut batch: Vec<TopicWriterMessage>,
    registry: &CodecRegistry,
    codec: Codec,
    strategy: &ErrorHandlingStrategy,
) -> YdbResult<Vec<TopicWriterMessage>> {
    if codec == Codec::RAW {
        return Ok(batch);
    }

    for message in batch.iter_mut() {
        message
            .uncompressed_size
            .get_or_insert(message.data.len() as i64);

        let data_bytes = Bytes::from(std::mem::take(&mut message.data));
        match registry.compress(&data_bytes, &codec) {
            Ok(compressed) => {
                message.data = compressed.to_vec();
                message.codec = Some(codec);
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
                    message.codec = Some(Codec::RAW);
                }
            },
        }
    }

    Ok(batch)
}
