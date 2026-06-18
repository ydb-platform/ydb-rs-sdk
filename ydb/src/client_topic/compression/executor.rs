use std::sync::{Arc, LazyLock};

pub trait Executor: Send + Sync {
    /// Returns the number of threads available for parallel processing.
    fn available_parallelism(&self) -> usize;

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
    fn available_parallelism(&self) -> usize {
        self.pool.current_num_threads()
    }

    fn execute(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        self.pool.spawn(move || task());
    }
}

static DEFAULT_EXECUTOR: LazyLock<Arc<dyn Executor>> =
    LazyLock::new(|| Arc::new(RayonExecutor::default()));

pub fn default_executor() -> Arc<dyn Executor> {
    DEFAULT_EXECUTOR.clone()
}
