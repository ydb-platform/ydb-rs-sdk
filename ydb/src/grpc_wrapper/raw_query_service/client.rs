use tracing::instrument;

use crate::grpc_wrapper::grpc_limits::WithGrpcMaxMessageSize;
use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_query_service::execute_query::RawExecuteQueryRequest;
use crate::grpc_wrapper::raw_query_service::execute_script::{
    RawExecuteScriptRequest, parse_execute_script_operation,
};
use crate::grpc_wrapper::raw_query_service::fetch_script_results::{
    RawFetchScriptResultsRequest, parse_response,
};
use crate::grpc_wrapper::raw_query_service::status::check_status;
use crate::grpc_wrapper::raw_query_service::transaction_control::{
    RawTxMode, tx_settings_for_mode,
};
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use ydb_grpc::ydb_proto::query::v1::query_service_client::QueryServiceClient;
use ydb_grpc::ydb_proto::query::{
    AttachSessionRequest, BeginTransactionRequest, CommitTransactionRequest, CreateSessionRequest,
    DeleteSessionRequest, ExecuteQueryResponsePart, RollbackTransactionRequest, SessionState,
};

/// gRPC metadata: enable server-side session balancing on CreateSession.
pub(crate) const HEADER_CLIENT_CAPABILITIES: &str = "x-ydb-client-capabilities";
pub(crate) const CLIENT_CAPABILITY_SESSION_BALANCER: &str = "session-balancer";

pub(crate) struct CreateSessionResult {
    pub session_id: String,
}

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

    #[instrument(name = "ydb.grpc.ExecuteQuery", skip_all, fields(db.system.name = "ydb", ydb.session.id = %req.session_id), err)]
    pub async fn execute_query(
        &mut self,
        req: RawExecuteQueryRequest,
    ) -> RawResult<tonic::Streaming<ExecuteQueryResponsePart>> {
        let proto = req.into_proto()?;
        let response = self.service.execute_query(proto).await?;
        Ok(response.into_inner())
    }

    #[instrument(name = "ydb.grpc.ExecuteScript", skip_all, fields(db.system.name = "ydb"), err)]
    pub async fn execute_script(
        &mut self,
        req: RawExecuteScriptRequest,
    ) -> RawResult<(String, Option<f64>)> {
        let proto = req.into_proto()?;
        let response = self.service.execute_script(proto).await?;
        parse_execute_script_operation(response.into_inner())
    }

    #[instrument(name = "ydb.grpc.FetchScriptResults", skip_all, fields(db.system.name = "ydb"), err)]
    pub async fn fetch_script_results(
        &mut self,
        req: RawFetchScriptResultsRequest,
    ) -> RawResult<(
        i64,
        crate::grpc_wrapper::raw_table_service::value::RawResultSet,
        String,
    )> {
        let proto = req.into_proto();
        let response = self.service.fetch_script_results(proto).await?;
        parse_response(response.into_inner())
    }

    #[instrument(name = "ydb.grpc.CreateSession", skip_all, fields(db.system.name = "ydb"), err)]
    pub async fn create_session(&mut self) -> RawResult<CreateSessionResult> {
        let mut request = tonic::Request::new(CreateSessionRequest {});
        request.metadata_mut().append(
            HEADER_CLIENT_CAPABILITIES,
            tonic::metadata::MetadataValue::from_static(CLIENT_CAPABILITY_SESSION_BALANCER),
        );
        let response = self.service.create_session(request).await?;
        let inner = response.into_inner();
        check_status(inner.status, &inner.issues)?;
        Ok(CreateSessionResult {
            session_id: inner.session_id,
        })
    }

    #[instrument(name = "ydb.grpc.DeleteSession", skip_all, fields(db.system.name = "ydb", ydb.session.id = %session_id), err)]
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

    #[instrument(name = "ydb.grpc.AttachSession", skip_all, fields(db.system.name = "ydb", ydb.session.id = %session_id), err)]
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

    #[instrument(name = "ydb.grpc.BeginTransaction", skip_all, fields(db.system.name = "ydb", ydb.session.id = %session_id, ydb.tx.mode = ?mode), err)]
    pub async fn begin_transaction(
        &mut self,
        session_id: &str,
        mode: RawTxMode,
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
        inner
            .tx_meta
            .map(|meta| meta.id)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| RawError::custom("BeginTransaction response missing tx_meta.id"))
    }

    #[instrument(name = "ydb.grpc.CommitTransaction", skip_all, fields(db.system.name = "ydb", ydb.session.id = %session_id, ydb.tx.id = %tx_id), err)]
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

    #[instrument(name = "ydb.grpc.RollbackTransaction", skip_all, fields(db.system.name = "ydb", ydb.session.id = %session_id, ydb.tx.id = %tx_id), err)]
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
