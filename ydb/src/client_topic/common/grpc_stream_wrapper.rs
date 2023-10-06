use crate::client_topic::common::stream_response_wrapper::StreamingResponseTrait;

use crate::grpc_wrapper::raw_errors::{RawError, RawResult};

use futures_util::StreamExt;
use tokio::sync::mpsc;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_server;

pub(crate) struct AsyncGrpcStreamWrapper<RequestT, ResponseT>
where
    ResponseT: StreamingResponseTrait<from_server::ServerMessage>,
{
    from_client_grpc: mpsc::UnboundedSender<RequestT>,
    from_server_grpc: tonic::Streaming<ResponseT>,
}

impl<RequestT, ResponseT: StreamingResponseTrait<from_server::ServerMessage>>
    AsyncGrpcStreamWrapper<RequestT, ResponseT>
{
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

    pub(crate) async fn receive(&mut self) -> RawResult<from_server::ServerMessage> {
        let maybe_ydb_response = self
            .from_server_grpc
            .next()
            .await
            .ok_or(RawError::Custom("Stream seems to be empty".to_string()))?;

        let ydb_response = maybe_ydb_response?;
        let response_body = ydb_response.extract_response_body()?;

        Ok(response_body)
    }
}
