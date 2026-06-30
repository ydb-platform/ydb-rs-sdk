use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use ydb_grpc::ydb_proto::topic::{stream_read_message, stream_write_message};

type Stream = mpsc::UnboundedSender<StreamReadCommand>;

#[derive(Clone, Default)]
pub struct TopicSender {
    streams: Arc<Mutex<BTreeMap<u64, Stream>>>,
}

pub enum StreamReadCommand {
    Reply(stream_read_message::FromServer),
    Close,
    Fail(tonic::Status),
}

impl TopicSender {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register_stream(
        &self,
        stream_id: u64,
    ) -> mpsc::UnboundedReceiver<StreamReadCommand> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.streams
            .lock()
            .expect("topic sender mutex poisoned")
            .insert(stream_id, tx);
        rx
    }

    pub(crate) fn unregister_stream(&self, stream_id: u64) {
        self.streams
            .lock()
            .expect("topic sender mutex poisoned")
            .remove(&stream_id);
    }

    pub fn send_to(&self, stream_id: u64, msg: stream_read_message::FromServer) {
        self.dispatch(stream_id, StreamReadCommand::Reply(msg));
    }

    pub fn close(&self, stream_id: u64) {
        self.dispatch(stream_id, StreamReadCommand::Close);
    }

    pub fn fail(&self, stream_id: u64, status: tonic::Status) {
        self.dispatch(stream_id, StreamReadCommand::Fail(status));
    }

    pub fn latest_stream_id(&self) -> Option<u64> {
        self.streams
            .lock()
            .expect("topic sender mutex poisoned")
            .keys()
            .next_back()
            .copied()
    }

    fn dispatch(&self, stream_id: u64, cmd: StreamReadCommand) {
        let sink = self
            .streams
            .lock()
            .expect("topic sender mutex poisoned")
            .get(&stream_id)
            .cloned();
        if let Some(sink) = sink {
            let _ = sink.send(cmd);
        }
    }
}

pub enum StreamWriteCommand {
    Reply(stream_write_message::FromServer),
    Close,
    Fail(tonic::Status),
}

#[derive(Clone, Default)]
pub struct WriteStreamSender {
    streams: Arc<Mutex<BTreeMap<u64, mpsc::UnboundedSender<StreamWriteCommand>>>>,
}

impl WriteStreamSender {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register_stream(
        &self,
        stream_id: u64,
    ) -> mpsc::UnboundedReceiver<StreamWriteCommand> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.streams
            .lock()
            .expect("write stream sender mutex poisoned")
            .insert(stream_id, tx);
        rx
    }

    pub(crate) fn unregister_stream(&self, stream_id: u64) {
        self.streams
            .lock()
            .expect("write stream sender mutex poisoned")
            .remove(&stream_id);
    }

    pub fn send_to(&self, stream_id: u64, msg: stream_write_message::FromServer) {
        self.dispatch(stream_id, StreamWriteCommand::Reply(msg));
    }

    pub fn close(&self, stream_id: u64) {
        self.dispatch(stream_id, StreamWriteCommand::Close);
    }

    pub fn fail(&self, stream_id: u64, status: tonic::Status) {
        self.dispatch(stream_id, StreamWriteCommand::Fail(status));
    }

    pub fn latest_stream_id(&self) -> Option<u64> {
        self.streams
            .lock()
            .expect("write stream sender mutex poisoned")
            .keys()
            .next_back()
            .copied()
    }

    fn dispatch(&self, stream_id: u64, cmd: StreamWriteCommand) {
        let sink = self
            .streams
            .lock()
            .expect("write stream sender mutex poisoned")
            .get(&stream_id)
            .cloned();
        if let Some(sink) = sink {
            let _ = sink.send(cmd);
        }
    }
}
