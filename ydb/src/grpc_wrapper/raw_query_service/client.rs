use std::collections::HashMap;

use crate::client::TimeoutSettings;
use crate::grpc_wrapper::grpc_limits::WithGrpcMaxMessageSize;
use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_query_service::execute_query::{
    check_part, merge_part, sets_to_vec, tx_id_from_part, RawExecuteQueryRequest,
    RawExecuteQueryResult,
};
use crate::grpc_wrapper::raw_query_service::status::check_status;
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use ydb_grpc::ydb_proto::query::v1::query_service_client::QueryServiceClient;
use ydb_grpc::ydb_proto::query::{
    AttachSessionRequest, CommitTransactionRequest, CreateSessionRequest,
    DeleteSessionRequest, ExecuteQueryResponsePart, RollbackTransactionRequest, SessionState,
};

pub(crate) struct RawQueryClient {
    service: QueryServiceClient<InterceptedChannel>,
    #[allow(dead_code)]
    timeouts: TimeoutSettings,
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
        Service::Table
    }
}

impl RawQueryClient {
    pub fn new(service: InterceptedChannel) -> Self {
        Self {
            service: QueryServiceClient::new(service),
            timeouts: TimeoutSettings::default(),
        }
    }

    #[allow(dead_code)]
    pub fn with_timeout(mut self, timeouts: TimeoutSettings) -> Self {
        self.timeouts = timeouts;
        self
    }

    pub async fn execute_query(
        &mut self,
        req: RawExecuteQueryRequest,
    ) -> RawResult<tonic::Streaming<ExecuteQueryResponsePart>> {
        let proto = req.into_proto()?;
        let response = self.service.execute_query(proto).await?;
        Ok(response.into_inner())
    }

    pub async fn execute_query_collect(
        &mut self,
        req: RawExecuteQueryRequest,
    ) -> RawResult<RawExecuteQueryResult> {
        let mut stream = self.execute_query(req).await?;
        let mut sets: HashMap<i64, crate::grpc_wrapper::raw_table_service::value::RawResultSet> =
            HashMap::new();
        let mut tx_id = None;

        while let Some(part) = stream.message().await? {
            check_part(&part)?;
            if let Some(id) = tx_id_from_part(&part) {
                tx_id = Some(id);
            }
            merge_part(&mut sets, part)?;
        }

        Ok(RawExecuteQueryResult {
            result_sets: sets_to_vec(sets),
            tx_id,
        })
    }

    pub async fn create_session(&mut self) -> RawResult<String> {
        let response = self
            .service
            .create_session(CreateSessionRequest {})
            .await?;
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
