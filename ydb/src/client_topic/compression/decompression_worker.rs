use super::ordered_task_queue::OrderedTaskQueue;
use crate::client_topic::compression::codec_registry::CodecRegistry;
use crate::client_topic::compression::error_strategy::ErrorHandlingStrategy;
use crate::client_topic::compression::executor::Executor;
use crate::client_topic::list_types::Codec;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::{
    RawBatch, RawBatchWithId, RawMessageData,
};
use crate::YdbResult;
use prost::bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

pub struct DecompressionWorker {
    codec_registry: Arc<CodecRegistry>,
    error_strategy: ErrorHandlingStrategy,
    parallelism: usize,
    queue: OrderedTaskQueue<RawBatchWithId>,
}

impl DecompressionWorker {
    pub fn new(
        codec_registry: Arc<CodecRegistry>,
        error_strategy: ErrorHandlingStrategy,
        executor: Arc<dyn Executor>,
    ) -> (Self, mpsc::UnboundedReceiver<YdbResult<RawBatchWithId>>) {
        let parallelism = executor.available_parallelism();
        let (queue, receiver) = OrderedTaskQueue::new(executor);
        (
            Self {
                codec_registry,
                error_strategy,
                parallelism,
                queue,
            },
            receiver,
        )
    }

    pub fn process_batch(&self, mut batch: RawBatchWithId) {
        let chunk_size = (batch.batch.message_data.len() / self.parallelism).max(1);
        let total_bytes = batch.read_session_size_bytes;

        while !batch.batch.message_data.is_empty() {
            let chunk: Vec<RawMessageData> = batch
                .batch
                .message_data
                .drain(..chunk_size.min(batch.batch.message_data.len()))
                .collect();
            let is_last = batch.batch.message_data.is_empty();

            let chunk_batch = RawBatchWithId {
                partition_session_id: batch.partition_session_id,
                read_session_size_bytes: if is_last { total_bytes } else { 0 },
                batch: RawBatch {
                    producer_id: batch.batch.producer_id.clone(),
                    write_session_meta: batch.batch.write_session_meta.clone(),
                    codec: batch.batch.codec.clone(),
                    written_at: batch.batch.written_at.clone(),
                    message_data: chunk,
                },
            };
            let registry = self.codec_registry.clone();
            let strategy = self.error_strategy.clone();

            self.queue.submit(Box::new(move || {
                decompress_batch(chunk_batch, &registry, &strategy)
            }));
        }
    }
}

fn decompress_batch(
    mut batch: RawBatchWithId,
    registry: &CodecRegistry,
    strategy: &ErrorHandlingStrategy,
) -> YdbResult<RawBatchWithId> {
    let codec = Codec {
        code: batch.batch.codec.code,
    };
    if codec == Codec::RAW {
        return Ok(batch);
    }

    let mut failed_offsets = Vec::new();

    for message in batch.batch.message_data.iter_mut() {
        let data_bytes = Bytes::from(std::mem::take(&mut message.data));
        match registry.decompress(&data_bytes, &codec) {
            Ok(decompressed) => {
                message.data = decompressed.to_vec();
            }
            Err(err) => match strategy {
                ErrorHandlingStrategy::FailFast => {
                    return Err(err);
                }
                ErrorHandlingStrategy::Skip => {
                    warn!(
                        "decompression failed for message (offset: {}, seq_no: {}), \
                         dropping message: {}",
                        message.offset, message.seq_no, err
                    );
                    failed_offsets.push(message.offset);
                }
            },
        }
    }

    if !failed_offsets.is_empty() {
        batch
            .batch
            .message_data
            .retain(|m| !failed_offsets.contains(&m.offset));
    }

    Ok(batch)
}
