use crate::client_topic::compression::executor::Executor;
use crate::YdbResult;
use std::{num::NonZeroUsize, sync::Arc};
use tokio::sync::{mpsc, oneshot};
use tracing::error;

type WorkerTask<T> = Box<dyn FnOnce() -> YdbResult<T> + Send + 'static>;

type TaskResultTx<T> = mpsc::Sender<oneshot::Receiver<YdbResult<T>>>;
pub(crate) type TaskResultRx<T> = mpsc::Receiver<oneshot::Receiver<YdbResult<T>>>;

pub(crate) struct OrderedTaskQueue<T: Send + 'static> {
    executor: Arc<dyn Executor>,
    results_tx: TaskResultTx<T>,
    running_task_slots: Arc<tokio::sync::Semaphore>,
}

impl<T: Send + 'static> OrderedTaskQueue<T> {
    pub(crate) fn new(
        executor: Arc<dyn Executor>,
        max_running_tasks: NonZeroUsize,
        output_backlog: NonZeroUsize,
    ) -> (Self, TaskResultRx<T>) {
        let max_running_tasks = max_running_tasks
            .min(executor.available_parallelism())
            .get();

        let (results_tx, results_rx) = mpsc::channel(output_backlog.get());

        let queue = Self {
            executor,
            results_tx,
            running_task_slots: Arc::new(tokio::sync::Semaphore::new(max_running_tasks)),
        };

        (queue, results_rx)
    }

    pub(crate) async fn submit(&self, task: WorkerTask<T>) {
        let Ok(task_permit) = self.running_task_slots.clone().acquire_owned().await else {
            error!("running task semaphore was closed");
            return;
        };

        let (tx, rx) = oneshot::channel();

        if self.results_tx.send(rx).await.is_err() {
            return;
        }

        self.executor.spawn(Box::new(move || {
            let result = task();
            drop(task_permit);
            let _ = tx.send(result);
        }));
    }
}
