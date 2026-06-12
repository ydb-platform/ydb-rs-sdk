use crate::grpc_wrapper::raw_errors::{RawError, RawResult};

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
    pub(crate) async fn send<Message>(&self, message: Message) -> RawResult<()>
    where
        Message: Into<RequestT>,
    {
        self.send_nowait(message)
    }

    pub(crate) fn send_nowait<Message>(&self, message: Message) -> RawResult<()>
    where
        Message: Into<RequestT>,
    {
        Ok(self.from_client_grpc.send(message.into())?)
    }

    pub(crate) fn clone_sender(&self) -> mpsc::UnboundedSender<RequestT> {
        self.from_client_grpc.clone()
    }

    pub(crate) async fn receive<Message>(&mut self) -> RawResult<Message>
    where
        Message: TryFrom<ResponseT, Error = RawError>,
    {
        let message = self
            .from_server_grpc
            .message()
            .await
            .map_err(RawError::from)?;

        message
            .ok_or_else(|| {
                RawError::from(tonic::Status::unavailable(
                    "Grpc stream was closed by sender",
                ))
            })?
            .try_into()
    }
}
