use std::{num::NonZeroUsize, sync::Arc};

pub trait Executor: Send + Sync {
    /// Returns an estimate amount of parallelism an executor should use.
    fn available_parallelism(&self) -> NonZeroUsize;

    /// Submits a task for execution. Fire-and-forget.
    fn execute(&self, task: Box<dyn FnOnce() + Send + 'static>);
}

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

    fn execute(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        self.pool.spawn(task);
    }
}

pub fn default_executor() -> Arc<dyn Executor> {
    Arc::new(RayonExecutor::default())
}

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

    fn execute(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        task();
    }
}

/// Runs each task on `tokio::task::spawn_blocking`. Requires a tokio runtime.
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

    fn execute(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        tokio::task::spawn_blocking(task);
    }
}
