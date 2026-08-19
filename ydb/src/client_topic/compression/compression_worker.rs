use super::ordered_task_queue::{self, OrderedTaskQueue};
use crate::client_topic::compression::codec_registry::CodecRegistry;
use crate::client_topic::compression::codec_selector::{CodecSelection, CodecSelector};
use crate::client_topic::compression::executor::Executor;
use crate::client_topic::list_types::Codec;
use crate::{YdbError, YdbResult};
use std::{num::NonZeroUsize, sync::Arc};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

type ChunkResult = YdbResult<CompressedChunk>;
type InputRx = mpsc::UnboundedReceiver<Vec<MessageData>>;
type OutputTx = mpsc::UnboundedSender<ChunkResult>;

pub(crate) struct CompressedChunk {
    pub(crate) messages: Vec<MessageData>,
    pub(crate) codec: Codec,
    pub(crate) ends_batch: bool,
}

pub(crate) struct CompressionWorker {
    codec_selector: CodecSelector,
    codec_registry: Arc<CodecRegistry>,
    queue: OrderedTaskQueue<CompressedChunk>,
    results_rx: ordered_task_queue::TaskResultRx<CompressedChunk>,
    parallelism: NonZeroUsize,
}

impl CompressionWorker {
    pub(crate) fn new(
        selection: CodecSelection,
        codec_registry: Arc<CodecRegistry>,
        executor: Arc<dyn Executor>,
        server_codecs: Vec<Codec>,
    ) -> YdbResult<Self> {
        let codec_selector = CodecSelector::new(
            selection,
            server_codecs,
            codec_registry.clone(),
            executor.clone(),
        )?;
        let parallelism = executor.available_parallelism();
        let output_backlog = parallelism.saturating_mul(super::OUTPUT_BACKLOG_PER_TASK);
        let (queue, results_rx) = OrderedTaskQueue::new(executor, parallelism, output_backlog);

        Ok(Self {
            codec_selector,
            codec_registry,
            queue,
            results_rx,
            parallelism,
        })
    }

    pub(crate) fn spawn_into(
        self,
        tasks: &mut JoinSet<YdbResult<()>>,
        mut rx: InputRx,
        tx: OutputTx,
    ) {
        let CompressionWorker {
            mut codec_selector,
            codec_registry,
            queue,
            mut results_rx,
            parallelism,
        } = self;

        tasks.spawn(async move {
            while let Some(mut batch) = rx.recv().await {
                codec_selector.step(&batch).await;
                let codec = codec_selector.codec();
                let chunk_size =
                    (batch.len() / parallelism).clamp(1, super::MAX_MESSAGES_PER_CHUNK);

                while !batch.is_empty() {
                    let chunk: Vec<MessageData> =
                        batch.drain(..chunk_size.min(batch.len())).collect();
                    let ends_batch = batch.is_empty();

                    let registry = codec_registry.clone();

                    queue
                        .submit(Box::new(move || {
                            compress_chunk(chunk, codec, ends_batch, registry)
                        }))
                        .await;
                }
            }
            Ok(())
        });

        tasks.spawn(async move {
            while let Some(result_tx) = results_rx.recv().await {
                let result = result_tx
                    .await
                    .unwrap_or(Err(YdbError::custom("executor compression task panicked")));

                if tx.send(result).is_err() {
                    return Ok(());
                }
            }
            Ok(())
        });
    }
}

fn compress_chunk(
    mut messages: Vec<MessageData>,
    codec: Codec,
    ends_batch: bool,
    registry: Arc<CodecRegistry>,
) -> ChunkResult {
    if codec != Codec::RAW {
        let Some(encoder) = registry.get_encoder(codec) else {
            return Err(YdbError::custom(format!(
                "no encoder found for codec {}",
                codec.code
            )));
        };

        for message in messages.iter_mut() {
            message.data = encoder.encode(message.data.as_slice()).map_err(|err| {
                YdbError::custom(format!(
                    "{encoder:?} failed to encode: {err}, message seq_no: {}",
                    message.seq_no,
                ))
            })?;
        }
    }

    Ok(CompressedChunk {
        messages,
        codec,
        ends_batch,
    })
}
