use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::client_topic::compression::{
    CodecRegistry, CompressionDecoder, Executor, MAX_MESSAGES_PER_CHUNK, OUTPUT_BACKLOG_PER_TASK,
    OrderedTaskQueue, TaskResultRx,
};
use crate::client_topic::list_types::Codec;
use crate::{TopicReaderMessage, YdbError, YdbResult};

use super::messages::MessageBatch;
use super::reconnector;
use super::runtime::RuntimeHandle;
use super::task_supervisor::wait_child_tasks;

type BatchRx = mpsc::UnboundedReceiver<MessageBatch>;

pub(super) struct Decompressor {
    codec_registry: Arc<CodecRegistry>,
    executor: Arc<dyn Executor>,
    rx: BatchRx,
    runtime: RuntimeHandle,
    cancellation: CancellationToken,
}

impl Decompressor {
    pub(super) fn new(
        attempt: &reconnector::ConnectionAttempt,
        rx: BatchRx,
        runtime: RuntimeHandle,
    ) -> Self {
        let mut codec_registry = CodecRegistry::new();
        for dec in &attempt.options.extra_decoders {
            codec_registry.register_decoder(dec.clone());
        }

        Self {
            codec_registry: Arc::new(codec_registry),
            executor: attempt.compression_executor.clone(),
            rx,
            runtime,
            cancellation: attempt.cancellation_token.clone(),
        }
    }

    pub(super) async fn run(self) -> YdbResult<()> {
        let Self {
            codec_registry,
            executor,
            rx,
            runtime,
            cancellation,
        } = self;

        let parallelism = executor.available_parallelism();
        let output_backlog = parallelism.saturating_mul(OUTPUT_BACKLOG_PER_TASK);
        let (queue, results_rx) = OrderedTaskQueue::new(executor, parallelism, output_backlog);
        let decompressor_cancellation = cancellation.child_token();

        let schedule = schedule_loop(
            rx,
            queue,
            codec_registry,
            parallelism,
            decompressor_cancellation.clone(),
        );
        let forward = forward_loop(results_rx, runtime, decompressor_cancellation.clone());

        let mut tasks: JoinSet<YdbResult<()>> = JoinSet::new();
        tasks.spawn(schedule);
        tasks.spawn(forward);

        wait_child_tasks(&decompressor_cancellation, tasks, "decompressor").await
    }
}

async fn schedule_loop(
    rx: BatchRx,
    queue: OrderedTaskQueue<Vec<TopicReaderMessage>>,
    codec_registry: Arc<CodecRegistry>,
    parallelism: NonZeroUsize,
    cancellation: CancellationToken,
) -> YdbResult<()> {
    select! {
        _ = cancellation.cancelled() => {
            debug!("decompressor schedule cancelled, stopping");
            Ok(())
        }
        result = schedule_messages(rx, queue, codec_registry, parallelism) => {
            let Err(e) = result;
            Err(e)
        }
    }
}

async fn schedule_messages(
    mut rx: BatchRx,
    queue: OrderedTaskQueue<Vec<TopicReaderMessage>>,
    codec_registry: Arc<CodecRegistry>,
    parallelism: NonZeroUsize,
) -> YdbResult<Infallible> {
    loop {
        let Some(MessageBatch { messages, codec }) = rx.recv().await else {
            return Err(YdbError::Transport(
                "decompressor input channel closed".into(),
            ));
        };

        let decoder: Option<Arc<dyn CompressionDecoder>> = if codec == Codec::RAW {
            None
        } else {
            Some(codec_registry.get_decoder(codec).ok_or_else(|| {
                YdbError::custom(format!("no decoder found for codec {}", codec.code))
            })?)
        };

        let chunk_size = (messages.len() / parallelism.get()).clamp(1, MAX_MESSAGES_PER_CHUNK);
        let mut iter = messages.into_iter();
        loop {
            let chunk: Vec<TopicReaderMessage> = iter.by_ref().take(chunk_size).collect();
            if chunk.is_empty() {
                break;
            }
            let dec = decoder.clone();
            queue
                .submit(Box::new(move || decompress_batch(chunk, dec)))
                .await;
        }
    }
}

async fn forward_loop(
    results_rx: TaskResultRx<Vec<TopicReaderMessage>>,
    runtime: RuntimeHandle,
    cancellation: CancellationToken,
) -> YdbResult<()> {
    select! {
        _ = cancellation.cancelled() => {
            debug!("decompressor forward cancelled, stopping");
            Ok(())
        }
        result = forward_messages(results_rx, runtime) => {
            let Err(e) = result;
            Err(e)
        }
    }
}

async fn forward_messages(
    mut results_rx: TaskResultRx<Vec<TopicReaderMessage>>,
    runtime: RuntimeHandle,
) -> YdbResult<Infallible> {
    loop {
        let Some(result_rx) = results_rx.recv().await else {
            return Err(YdbError::Transport(
                "decompressor results channel closed".into(),
            ));
        };
        let messages = result_rx
            .await
            .unwrap_or_else(|_| Err(YdbError::custom("executor decompression task panicked")))?;
        runtime.push_batch(messages)?;
    }
}

fn decompress_batch(
    mut batch: Vec<TopicReaderMessage>,
    decoder: Option<Arc<dyn CompressionDecoder>>,
) -> YdbResult<Vec<TopicReaderMessage>> {
    let Some(decoder) = decoder else {
        return Ok(batch);
    };

    for message in batch.iter_mut() {
        let Some(raw_data) = message.raw_data.as_ref() else {
            continue;
        };

        message.raw_data = Some(decoder.decode(raw_data.as_slice()).map_err(|err| {
            YdbError::custom(format!(
                "{decoder:?} failed to decode: {err}, message seq_no: {}, message offset: {}",
                message.seq_no, message.offset,
            ))
        })?);
    }

    Ok(batch)
}
