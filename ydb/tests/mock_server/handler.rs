use std::sync::Mutex;

use super::{
    query::{QueryIncoming, QueryReply},
    scheme::{SchemeIncoming, SchemeReply},
    topic::{TopicIncoming, TopicReply},
};

pub type FromServiceToServerTx = tokio::sync::mpsc::UnboundedSender<Incoming>;
pub type FromServiceToServerRx = tokio::sync::mpsc::UnboundedReceiver<Incoming>;

pub type FromServerToServiceTx = tokio::sync::mpsc::UnboundedSender<Reply>;
pub type FromServerToServiceRx = tokio::sync::mpsc::UnboundedReceiver<Reply>;

pub type FromHandlerToService = FromServerToServiceTx;

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Incoming {
    Topic(TopicIncoming),
    Scheme(SchemeIncoming),
    Query(QueryIncoming),
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Reply {
    Topic(TopicReply),
    Scheme(SchemeReply),
    Query(QueryReply),
}

impl From<QueryReply> for Reply {
    fn from(reply: QueryReply) -> Self {
        Self::Query(reply)
    }
}

impl From<TopicReply> for Reply {
    fn from(reply: TopicReply) -> Self {
        Self::Topic(reply)
    }
}

impl From<SchemeReply> for Reply {
    fn from(reply: SchemeReply) -> Self {
        Self::Scheme(reply)
    }
}

#[derive(Default)]
pub struct ReplySink {
    tx: Mutex<Option<FromHandlerToService>>,
}

impl ReplySink {
    pub fn set_channel(&self, tx: FromHandlerToService) {
        *self
            .tx
            .lock()
            .expect("poisoning shouldn't happen in reply channel mock (set_channel)") = Some(tx);
    }

    pub fn send(&self, reply: impl Into<Reply>) {
        self.tx
            .lock()
            .expect("poisoning shouldn't happen in reply channel mock (send)")
            .as_ref()
            .expect("mock reply channel must be set before replies are sent")
            .send(reply.into())
            .expect("mock server failed to send reply");
    }
}

pub trait Handler: Send + 'static {
    fn set_channel(&mut self, _tx: FromHandlerToService) {}

    /// Default behavior: let every request through to the service's default
    /// reply policy. Override to absorb (`None`) or rewrite specific messages.
    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        Some(incoming)
    }
}
