// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::YdbResult;

#[allow(dead_code)]
pub struct TopicWriter {
    pub topic_path: String,
    pub writer_options: TopicWriterOptions,

    pub(crate) connection_manager: GrpcConnectionManager,
}

#[allow(dead_code)]
pub enum AckInfo {
    SuccessfullySent,
    Error,
}

#[allow(dead_code)]
pub struct AckFuture {}

impl Future for AckFuture{
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        unimplemented!("prototype")
    }
}

impl TopicWriter {
    pub async fn write(&self, _message: TopicWriterMessage) -> YdbResult<()> {
        unimplemented!("prototype")
    }

    pub async fn write_with_ack(&self, _message: TopicWriterMessage) -> YdbResult<AckInfo> {
        unimplemented!("prototype")
    }

    pub async fn write_with_ack_future(
        &self,
        _message: TopicWriterMessage,
    ) -> YdbResult<AckFuture> {
        unimplemented!("prototype")
    }

    pub async fn flush(&self) -> YdbResult<()> {
        unimplemented!("prototype")
    }
}
