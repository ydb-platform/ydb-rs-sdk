use crate::grpc_wrapper::grpc_limits::WithGrpcMaxMessageSize;
use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_query_service::execute_query::RawExecuteQueryRequest;
use crate::grpc_wrapper::raw_query_service::status::check_status;
use crate::grpc_wrapper::raw_query_service::transaction_control::{
    tx_settings_for_mode, RawQueryTxMode,
};
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use ydb_grpc::ydb_proto::query::v1::query_service_client::QueryServiceClient;
use ydb_grpc::ydb_proto::query::{
    AttachSessionRequest, BeginTransactionRequest, CommitTransactionRequest, CreateSessionRequest,
    DeleteSessionRequest, ExecuteQueryResponsePart, RollbackTransactionRequest, SessionState,
};

pub(crate) struct RawQueryClient {
    service: QueryServiceClient<InterceptedChannel>,
}

impl WithGrpcMaxMessageSize for RawQueryClient {
    fn with_grpc_max_message_size(mut self, bytes: usize) -> Self {
        self.service = self
            .service
            .max_decoding_message_size(bytes)
            .max_encoding_message_size(bytes);
        self
    }
}

impl GrpcServiceForDiscovery for RawQueryClient {
    fn get_grpc_discovery_service() -> Service {
        Service::Query
    }
}

impl RawQueryClient {
    pub fn new(service: InterceptedChannel) -> Self {
        Self {
            service: QueryServiceClient::new(service),
        }
    }

    pub async fn execute_query(
        &mut self,
        req: RawExecuteQueryRequest,
    ) -> RawResult<tonic::Streaming<ExecuteQueryResponsePart>> {
        let proto = req.into_proto()?;
        let response = self.service.execute_query(proto).await?;
        Ok(response.into_inner())
    }

    pub async fn create_session(&mut self) -> RawResult<String> {
        let response = self.service.create_session(CreateSessionRequest {}).await?;
        let inner = response.into_inner();
        check_status(inner.status, &inner.issues)?;
        Ok(inner.session_id)
    }

    pub async fn delete_session(&mut self, session_id: &str) -> RawResult<()> {
        let response = self
            .service
            .delete_session(DeleteSessionRequest {
                session_id: session_id.to_string(),
            })
            .await?;
        let inner = response.into_inner();
        check_status(inner.status, &inner.issues)
    }

    pub async fn attach_session(
        &mut self,
        session_id: &str,
    ) -> RawResult<tonic::Streaming<SessionState>> {
        let response = self
            .service
            .attach_session(AttachSessionRequest {
                session_id: session_id.to_string(),
            })
            .await?;
        Ok(response.into_inner())
    }

    pub async fn begin_transaction(
        &mut self,
        session_id: &str,
        mode: RawQueryTxMode,
    ) -> RawResult<String> {
        let response = self
            .service
            .begin_transaction(BeginTransactionRequest {
                session_id: session_id.to_string(),
                tx_settings: Some(tx_settings_for_mode(mode)),
            })
            .await?;
        let inner = response.into_inner();
        check_status(inner.status, &inner.issues)?;
        let tx_id = inner
            .tx_meta
            .map(|meta| meta.id)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                crate::grpc_wrapper::raw_errors::RawError::custom(
                    "begin transaction returned empty tx id",
                )
            })?;
        Ok(tx_id)
    }

    pub async fn commit_transaction(&mut self, session_id: &str, tx_id: &str) -> RawResult<()> {
        let response = self
            .service
            .commit_transaction(CommitTransactionRequest {
                session_id: session_id.to_string(),
                tx_id: tx_id.to_string(),
            })
            .await?;
        let inner = response.into_inner();
        check_status(inner.status, &inner.issues)
    }

    pub async fn rollback_transaction(&mut self, session_id: &str, tx_id: &str) -> RawResult<()> {
        let response = self
            .service
            .rollback_transaction(RollbackTransactionRequest {
                session_id: session_id.to_string(),
                tx_id: tx_id.to_string(),
            })
            .await?;
        let inner = response.into_inner();
        check_status(inner.status, &inner.issues)
    }
}
