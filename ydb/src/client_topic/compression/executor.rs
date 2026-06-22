use std::{num::NonZeroUsize, sync::Arc};

/// Describes, how reader/writer handle compression/decompression errors.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ErrorHandlingStrategy {
    /// On any compression/decompression error propagates the error to the reader/writer.
    FailFast,

    /// Reader: on decompression failure returns the raw compressed bytes with `decompression_failed` set.
    /// Writer: on compression failure sends the message as RAW instead of compressed.
    Skip,
}

pub trait Executor: Send + Sync {
    /// Returns a concurrency hint for blocking compression work.
    ///
    /// Used for task and buffer sizing; not a global task limit.
    fn available_parallelism(&self) -> NonZeroUsize;

    /// Schedules a blocking task.
    ///
    /// Implementations should not run CPU-heavy codec work on a Tokio worker thread.
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>);
}

/// Dedicated Rayon-backed executor for compression work.
pub struct RayonExecutor {
    pool: rayon::ThreadPool,
}

impl RayonExecutor {
    pub fn new(num_threads: usize) -> Self {
        Self {
            pool: rayon::ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .build()
                .expect("failed to create rayon thread pool"),
        }
    }
}

const DEFAULT_THREAD_COUNT: usize = 4;

impl Default for RayonExecutor {
    fn default() -> Self {
        Self::new(DEFAULT_THREAD_COUNT)
    }
}

impl Executor for RayonExecutor {
    fn available_parallelism(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.pool.current_num_threads())
            .unwrap_or(const { NonZeroUsize::new(1).unwrap() })
    }

    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        self.pool.spawn(task);
    }
}

pub fn default_executor() -> Arc<dyn Executor> {
    Arc::new(RayonExecutor::default())
}

/// Executor that runs tasks immediately on the caller thread.
///
/// Intended for tests and controlled environments.
#[derive(Default)]
pub struct InplaceExecutor {}

impl InplaceExecutor {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Executor for InplaceExecutor {
    fn available_parallelism(&self) -> NonZeroUsize {
        const { NonZeroUsize::new(1).unwrap() }
    }

    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        task();
    }
}

/// Executor that uses `tokio::task::spawn_blocking`.
///
/// Requires an active Tokio runtime.
#[derive(Default)]
pub struct TokioExecutor {}

impl TokioExecutor {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Executor for TokioExecutor {
    fn available_parallelism(&self) -> NonZeroUsize {
        std::thread::available_parallelism().unwrap_or(const { NonZeroUsize::new(1).unwrap() })
    }

    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        tokio::task::spawn_blocking(task);
    }
}
