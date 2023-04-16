// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::{grpc_wrapper, YdbResult};

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

impl Future for AckFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        unimplemented!("prototype")
    }
}

impl TopicWriter {
    pub(crate) async fn new(topic_path: String, writer_options: TopicWriterOptions, connection_manager: GrpcConnectionManager) -> Self {
        Self {
            topic_path,
            writer_options,
            connection_manager,
        }
    }

    pub async fn write(&self, message: TopicWriterMessage) -> YdbResult<()> {
        let mut service = self.connection().await?;
        service.do_single_write_request(message, &self.topic_path, &self.writer_options).await?;

        Ok(())
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

    async fn connection(
        &self,
    ) -> YdbResult<grpc_wrapper::raw_topic_service::client::RawTopicClient> {
        self.connection_manager
            .get_auth_service(grpc_wrapper::raw_topic_service::client::RawTopicClient::new)
            .await
    }
}
