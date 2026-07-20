mod builtin_codecs;
mod codec_registry;
mod codec_selector;
mod compression_worker;
mod executor;
mod ordered_task_queue;

pub(crate) const MAX_MESSAGES_PER_CHUNK: usize = 100;
pub(crate) const OUTPUT_BACKLOG_PER_TASK: std::num::NonZeroUsize =
    const { std::num::NonZeroUsize::new(4).unwrap() };

pub(crate) use codec_registry::CodecRegistry;
pub use codec_registry::{CompressionDecoder, CompressionEncoder};
pub use codec_selector::CodecSelection;
pub(crate) use compression_worker::CompressionWorker;
pub use executor::Executor;
#[cfg(test)]
pub(crate) use executor::RayonExecutor;
pub(crate) use executor::default_executor;
pub(crate) use ordered_task_queue::{OrderedTaskQueue, TaskResultRx};
