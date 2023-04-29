use crate::grpc_wrapper::raw_errors::RawResult;
use futures_util::StreamExt;
use tokio::sync::mpsc;

pub(crate) struct AsyncGrpcStreamWrapper<RequestT, ResponseT> {
    from_client_grpc: mpsc::UnboundedSender<RequestT>,
    from_server_grpc: tonic::Streaming<ResponseT>,
}

impl <RequestT, ResponseT> AsyncGrpcStreamWrapper<RequestT, ResponseT> {
    pub(crate) fn new(request_stream: mpsc::UnboundedSender<RequestT>, response_stream: tonic::Streaming<ResponseT>) -> Self {
        Self{
            from_client_grpc: request_stream,
            from_server_grpc: response_stream
        }
    }

    pub(crate) async fn send(
        &mut self,
        message: RequestT,
    ) -> RawResult<()> {
        self.from_client_grpc.send(message).map_err(|e| e.into())
    }

    pub(crate) async fn receive(&mut self) -> Option<RawResult<ResponseT>> {
        if let Some(receive_result) = self.from_server_grpc.next().await {
            return Some(receive_result.map_err(|e| e.into()));
        }
        return None;
    }
}
