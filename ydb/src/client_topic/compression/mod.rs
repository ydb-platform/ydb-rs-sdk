mod builtin_codecs;
mod codec_registry;
mod codec_selector;
mod compression_worker;
mod decompression_worker;
mod error_strategy;
mod executor;
mod ordered_task_queue;

pub(crate) use codec_registry::CodecRegistry;
pub use codec_registry::{CompressionDecoder, CompressionEncoder};
pub use codec_selector::CodecSelection;
pub(crate) use compression_worker::{CompressedGroups, CompressionWorker};
pub(crate) use decompression_worker::DecompressionWorker;
pub use error_strategy::ErrorHandlingStrategy;
pub use executor::{default_executor, Executor, InplaceExecutor, RayonExecutor, TokioExecutor};
