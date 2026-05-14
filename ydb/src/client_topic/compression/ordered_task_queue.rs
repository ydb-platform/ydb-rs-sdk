use crate::client_topic::compression::executor::Executor;
use crate::YdbResult;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

type WorkerTask<T> = Box<dyn FnOnce() -> YdbResult<T> + Send + 'static>;

pub struct OrderedTaskQueue<T: Send + 'static> {
    executor: Arc<dyn Executor>,
    state: Arc<Mutex<ReorderState<T>>>,
    next_seq: AtomicU64,
}

struct ReorderState<T> {
    next_expected: u64,
    pending: BTreeMap<u64, YdbResult<T>>,
    result_sender: mpsc::UnboundedSender<YdbResult<T>>,
}

impl<T: Send + 'static> ReorderState<T> {
    fn insert_and_drain(&mut self, seq: u64, result: YdbResult<T>) {
        self.pending.insert(seq, result);

        while let Some(result) = self.pending.remove(&self.next_expected) {
            if self.result_sender.send(result).is_err() {
                break; // consumer dropped
            }
            self.next_expected += 1;
        }
    }
}

impl<T: Send + 'static> OrderedTaskQueue<T> {
    pub fn new(executor: Arc<dyn Executor>) -> (Self, mpsc::UnboundedReceiver<YdbResult<T>>) {
        let (result_sender, result_receiver) = mpsc::unbounded_channel::<YdbResult<T>>();
        (
            Self {
                executor,
                state: Arc::new(Mutex::new(ReorderState {
                    next_expected: 0,
                    pending: BTreeMap::new(),
                    result_sender,
                })),
                next_seq: AtomicU64::new(0),
            },
            result_receiver,
        )
    }

    pub fn submit(&self, task: WorkerTask<T>) {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let state = self.state.clone();

        self.executor.execute(Box::new(move || {
            let result = task();
            state.lock().unwrap().insert_and_drain(seq, result);
        }));
    }
}
