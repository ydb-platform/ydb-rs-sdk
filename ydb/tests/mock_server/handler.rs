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

pub trait Handler: Send + 'static {
    fn set_channel(&mut self, _tx: FromHandlerToService) {}

    /// Default behavior: let every request through to the service's default
    /// reply policy. Override to absorb (`None`) or rewrite specific messages.
    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        Some(incoming)
    }
}
