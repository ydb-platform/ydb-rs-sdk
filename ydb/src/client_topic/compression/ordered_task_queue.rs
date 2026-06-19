use crate::client_topic::compression::executor::Executor;
use crate::YdbResult;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

type WorkerTask<T> = Box<dyn FnOnce() -> YdbResult<T> + Send + 'static>;

type TaskResultTx<T> = mpsc::Sender<oneshot::Receiver<YdbResult<T>>>;
pub(super) type TaskResultRx<T> = mpsc::Receiver<oneshot::Receiver<YdbResult<T>>>;

pub(super) struct OrderedTaskQueue<T: Send + 'static> {
    executor: Arc<dyn Executor>,
    results_tx: TaskResultTx<T>,
}

impl<T: Send + 'static> OrderedTaskQueue<T> {
    pub(super) fn new(executor: Arc<dyn Executor>, queue_size: usize) -> (Self, TaskResultRx<T>) {
        let (results_tx, results_rx) = mpsc::channel(queue_size);

        let query = Self {
            executor,
            results_tx,
        };

        (query, results_rx)
    }

    pub(super) async fn submit(&self, task: WorkerTask<T>) {
        let (tx, rx) = oneshot::channel();

        // NOTE: Channel fixed size guarantees that simultaneously we will not be able to run more
        // than `queue_size` tasks.
        if self.results_tx.send(rx).await.is_err() {
            return;
        }

        self.executor.execute(Box::new(move || {
            let _ = tx.send(task());
        }));
    }
}
