use tracing::{instrument, trace};
use ydb_grpc::ydb_proto::coordination::session_request::{self, SessionStart};
use ydb_grpc::ydb_proto::coordination::v1::coordination_service_client::CoordinationServiceClient;
use ydb_grpc::ydb_proto::coordination::{SessionRequest, SessionResponse};

use crate::grpc_wrapper::grpc_limits::WithGrpcMaxMessageSize;
use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;

use super::alter_node::RawAlterNodeRequest;
use super::create_node::RawCreateNodeRequest;
use super::describe_node::{RawDescribeNodeRequest, RawDescribeNodeResult};
use super::drop_node::RawDropNodeRequest;

pub(crate) struct RawCoordinationClient {
    service: CoordinationServiceClient<InterceptedChannel>,
}

impl WithGrpcMaxMessageSize for RawCoordinationClient {
    fn with_grpc_max_message_size(mut self, bytes: usize) -> Self {
        self.service = self
            .service
            .max_decoding_message_size(bytes)
            .max_encoding_message_size(bytes);
        self
    }
}

impl RawCoordinationClient {
    pub fn new(service: InterceptedChannel) -> Self {
        Self {
            service: CoordinationServiceClient::new(service),
        }
    }

    #[instrument(name = "ydb.grpc.CoordinationSession", skip_all, fields(ydb.coordination.path = %req.path), err)]
    pub async fn session(
        &mut self,
        req: SessionStart,
    ) -> RawResult<AsyncGrpcStreamWrapper<SessionRequest, SessionResponse>> {
        let (tx, rx): (
            tokio::sync::mpsc::UnboundedSender<SessionRequest>,
            tokio::sync::mpsc::UnboundedReceiver<SessionRequest>,
        ) = tokio::sync::mpsc::unbounded_channel();

        let mess = SessionRequest {
            request: Some(session_request::Request::SessionStart(req)),
        };
        tx.send(mess)?;

        let request_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        let stream_writer_result = self.service.session(request_stream).await;
        let response_stream = stream_writer_result?.into_inner();

        Ok(AsyncGrpcStreamWrapper::<SessionRequest, SessionResponse>::new(tx, response_stream))
    }

    #[instrument(name = "ydb.grpc.CreateNode", skip_all, fields(ydb.coordination.path = %req.path), err)]
    pub async fn create_node(&mut self, req: RawCreateNodeRequest) -> RawResult<()> {
        request_without_result!(
            self.service.create_node,
            req => ydb_grpc::ydb_proto::coordination::CreateNodeRequest
        );
    }

    #[instrument(name = "ydb.grpc.AlterNode", skip_all, fields(ydb.coordination.path = %req.path), err)]
    pub async fn alter_node(&mut self, req: RawAlterNodeRequest) -> RawResult<()> {
        request_without_result!(
            self.service.alter_node,
            req => ydb_grpc::ydb_proto::coordination::AlterNodeRequest
        );
    }

    #[instrument(name = "ydb.grpc.DropNode", skip_all, fields(ydb.coordination.path = %req.path), err)]
    pub async fn drop_node(&mut self, req: RawDropNodeRequest) -> RawResult<()> {
        request_without_result!(
            self.service.drop_node,
            req => ydb_grpc::ydb_proto::coordination::DropNodeRequest
        );
    }

    #[instrument(name = "ydb.grpc.DescribeNode", skip_all, fields(ydb.coordination.path = %req.path), err)]
    pub async fn describe_node(
        &mut self,
        req: RawDescribeNodeRequest,
    ) -> RawResult<RawDescribeNodeResult> {
        request_with_result!(
            self.service.describe_node,
            req => ydb_grpc::ydb_proto::coordination::DescribeNodeRequest,
            ydb_grpc::ydb_proto::coordination::DescribeNodeResult => RawDescribeNodeResult
        );
    }

    // use for tests only, while reader not ready
    pub(crate) fn get_grpc_service(&self) -> CoordinationServiceClient<InterceptedChannel> {
        self.service.clone()
    }
}

impl GrpcServiceForDiscovery for RawCoordinationClient {
    fn get_grpc_discovery_service() -> Service {
        Service::Coordination
    }
}
