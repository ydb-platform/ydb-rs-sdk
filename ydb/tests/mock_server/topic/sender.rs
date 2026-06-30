use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, watch};
use ydb_grpc::ydb_proto::topic::{stream_read_message, stream_write_message};

pub enum StreamCommand<T> {
    Reply(T),
    Close,
    Fail(tonic::Status),
}

pub type ReadStreamCommand = StreamCommand<stream_read_message::FromServer>;
pub type WriteStreamCommand = StreamCommand<stream_write_message::FromServer>;

#[derive(Clone)]
pub struct StreamSender<T> {
    streams: Arc<Mutex<BTreeMap<u64, mpsc::UnboundedSender<StreamCommand<T>>>>>,
    latest_stream_id_tx: watch::Sender<Option<u64>>,
}

pub type ReadStreamSender = StreamSender<stream_read_message::FromServer>;
pub type WriteStreamSender = StreamSender<stream_write_message::FromServer>;

impl<T> Default for StreamSender<T> {
    fn default() -> Self {
        let (latest_stream_id_tx, _) = watch::channel(None);
        Self {
            streams: Arc::new(Mutex::new(BTreeMap::new())),
            latest_stream_id_tx,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamSenderError {
    MissingStream { stream_id: u64 },
    ClosedStream { stream_id: u64 },
}

impl fmt::Display for StreamSenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamSenderError::MissingStream { stream_id } => {
                write!(f, "stream {stream_id} is not registered")
            }
            StreamSenderError::ClosedStream { stream_id } => {
                write!(f, "stream {stream_id} is closed")
            }
        }
    }
}

impl std::error::Error for StreamSenderError {}

impl<T> StreamSender<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register_stream(
        &self,
        stream_id: u64,
    ) -> mpsc::UnboundedReceiver<StreamCommand<T>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.streams
            .lock()
            .expect("stream sender mutex poisoned")
            .insert(stream_id, tx);
        let _ = self.latest_stream_id_tx.send(Some(stream_id));
        rx
    }

    pub(crate) fn unregister_stream(&self, stream_id: u64) {
        let latest_stream_id = {
            let mut streams = self.streams.lock().expect("stream sender mutex poisoned");
            streams.remove(&stream_id);
            streams.keys().next_back().copied()
        };
        let _ = self.latest_stream_id_tx.send(latest_stream_id);
    }

    pub fn send_to(&self, stream_id: u64, msg: T) -> Result<(), StreamSenderError> {
        self.dispatch(stream_id, StreamCommand::Reply(msg))
    }

    pub fn close(&self, stream_id: u64) -> Result<(), StreamSenderError> {
        self.dispatch(stream_id, StreamCommand::Close)
    }

    pub fn fail(&self, stream_id: u64, status: tonic::Status) -> Result<(), StreamSenderError> {
        self.dispatch(stream_id, StreamCommand::Fail(status))
    }

    pub fn latest_stream_id(&self) -> Option<u64> {
        self.streams
            .lock()
            .expect("stream sender mutex poisoned")
            .keys()
            .next_back()
            .copied()
    }

    pub fn subscribe_latest_stream_id(&self) -> watch::Receiver<Option<u64>> {
        self.latest_stream_id_tx.subscribe()
    }

    fn dispatch(&self, stream_id: u64, cmd: StreamCommand<T>) -> Result<(), StreamSenderError> {
        let sink = self
            .streams
            .lock()
            .expect("stream sender mutex poisoned")
            .get(&stream_id)
            .cloned()
            .ok_or(StreamSenderError::MissingStream { stream_id })?;

        sink.send(cmd)
            .map_err(|_| StreamSenderError::ClosedStream { stream_id })
    }
}
