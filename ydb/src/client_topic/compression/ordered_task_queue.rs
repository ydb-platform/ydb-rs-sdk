use crate::{YdbError, YdbResult};
use tokio::sync::mpsc;

type WorkerTask<T> = Box<dyn FnOnce() -> YdbResult<T> + Send + 'static>;

const DEFAULT_TASK_CHANNEL_CAPACITY: usize = 32;

pub struct OrderedTaskQueue<T> {
    task_sender: mpsc::Sender<WorkerTask<T>>,
}

impl<T: Send + 'static> OrderedTaskQueue<T> {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<YdbResult<T>>) {
        Self::with_capacity(DEFAULT_TASK_CHANNEL_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> (Self, mpsc::UnboundedReceiver<YdbResult<T>>) {
        let (task_sender, mut task_receiver) = mpsc::channel::<WorkerTask<T>>(capacity);
        let (result_sender, result_receiver) = mpsc::unbounded_channel::<YdbResult<T>>();

        std::thread::spawn(move || {
            while let Some(task) = task_receiver.blocking_recv() {
                let result = task();
                if result_sender.send(result).is_err() {
                    break;
                }
            }
        });

        (Self { task_sender }, result_receiver)
    }

    pub async fn submit(&self, task: WorkerTask<T>) -> YdbResult<()> {
        self.task_sender
            .send(task)
            .await
            .map_err(|err| YdbError::custom(format!("ordered queue is closed: {err}")))
    }
}
