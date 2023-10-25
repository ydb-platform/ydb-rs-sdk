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
    pub(crate) async fn send(&mut self, message: RequestT) -> RawResult<()> {
        Ok(self.from_client_grpc.send(message)?)
    }

    pub(crate) fn clone_sender(&mut self) -> mpsc::UnboundedSender<RequestT> {
        self.from_client_grpc.clone()
    }

    pub(crate) async fn receive(&mut self) -> RawResult<ResponseT> {
        let maybe_ydb_response = self
            .from_server_grpc
            .next()
            .await
            .ok_or(RawError::Custom("Stream seems to be empty".to_string()))?;
        Ok(maybe_ydb_response?)
    }
}
