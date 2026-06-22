mod builtin_codecs;
mod codec_registry;
mod codec_selector;
mod compression_worker;
mod decompression_worker;
mod executor;
mod ordered_task_queue;

const MAX_MESSAGES_PER_CHUNK: usize = 100;
const OUTPUT_BACKLOG_PER_TASK: std::num::NonZeroUsize =
    const { std::num::NonZeroUsize::new(4).unwrap() };

pub(crate) use codec_registry::CodecRegistry;
pub use codec_registry::{CompressionDecoder, CompressionEncoder};
pub use codec_selector::CodecSelection;
pub(crate) use compression_worker::CompressionWorker;
pub(crate) use decompression_worker::DecompressionWorker;
pub use executor::{default_executor, Executor, InplaceExecutor, RayonExecutor, TokioExecutor};
