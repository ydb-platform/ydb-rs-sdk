use crate::grpc_wrapper::raw_errors::{RawError, RawResult};

use futures_util::StreamExt;
use tokio::sync::mpsc;

pub(crate) struct AsyncGrpcStreamWrapper<RequestT, ResponseT> {
    from_client_grpc: mpsc::UnboundedSender<RequestT>,
    from_server_grpc: tonic::Streaming<ResponseT>,
}

impl<RequestT, ResponseT> AsyncGrpcStreamWrapper<RequestT, ResponseT> {
    pub(crate) fn new(
        request_stream: mpsc::UnboundedSender<RequestT>,
        response_stream: tonic::Streaming<ResponseT>,
    ) -> Self {
        Self {
            from_client_grpc: request_stream,
            from_server_grpc: response_stream,
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn send<Message>(&mut self, message: Message) -> RawResult<()>
    where
        Message: Into<RequestT>,
    {
        self.send_nowait(message)
    }

    pub(crate) fn send_nowait<Message>(&mut self, message: Message) -> RawResult<()>
    where
        Message: Into<RequestT>,
    {
        Ok(self.from_client_grpc.send(message.into())?)
    }

    pub(crate) fn clone_sender(&mut self) -> mpsc::UnboundedSender<RequestT> {
        self.from_client_grpc.clone()
    }

    pub(crate) async fn receive<Message>(&mut self) -> RawResult<Message>
    where
        Message: TryFrom<ResponseT, Error = RawError>,
    {
        let maybe_ydb_response = self
            .from_server_grpc
            .next()
            .await
            .ok_or(RawError::Custom("Stream seems to be empty".to_string()))?;
        let message = Message::try_from(maybe_ydb_response?)?;
        Ok(message)
    }
}
