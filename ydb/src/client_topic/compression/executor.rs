use std::{num::NonZeroUsize, sync::Arc};

use crate::{YdbError, YdbResult};

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
pub(crate) struct RayonExecutor {
    pool: rayon::ThreadPool,
}

impl RayonExecutor {
    pub(crate) fn new(num_threads: usize) -> YdbResult<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .map_err(|err| YdbError::custom(format!("thread pool did not build: {err}")))?;

        Ok(Self { pool })
    }
}

const DEFAULT_THREAD_COUNT: usize = 4;

impl Executor for RayonExecutor {
    fn available_parallelism(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.pool.current_num_threads())
            .unwrap_or(const { NonZeroUsize::new(1).unwrap() })
    }

    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        self.pool.spawn(task);
    }
}

pub(crate) fn default_executor() -> YdbResult<Arc<dyn Executor>> {
    let executor = RayonExecutor::new(DEFAULT_THREAD_COUNT)?;

    Ok(Arc::new(executor))
}

/// Executor that runs tasks immediately on the caller thread.
///
/// Intended for tests and controlled environments.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct InplaceExecutor {}

#[cfg(test)]
impl InplaceExecutor {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

#[cfg(test)]
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
#[cfg(test)]
#[derive(Default)]
pub(crate) struct TokioExecutor {}

#[cfg(test)]
impl TokioExecutor {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

#[cfg(test)]
impl Executor for TokioExecutor {
    fn available_parallelism(&self) -> NonZeroUsize {
        std::thread::available_parallelism().unwrap_or(const { NonZeroUsize::new(1).unwrap() })
    }

    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        tokio::task::spawn_blocking(task);
    }
}
