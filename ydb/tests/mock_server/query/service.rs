use std::pin::Pin;

use futures_util::{stream, Stream, StreamExt};
use tokio_stream::iter;
use ydb_grpc::ydb_proto::operations::Operation;
use ydb_grpc::ydb_proto::query::v1::query_service_server::QueryService;
use ydb_grpc::ydb_proto::query::{
    AttachSessionRequest, BeginTransactionRequest, BeginTransactionResponse,
    CommitTransactionRequest, CommitTransactionResponse, CreateSessionRequest,
    CreateSessionResponse, DeleteSessionRequest, DeleteSessionResponse, ExecuteQueryRequest,
    ExecuteQueryResponsePart, ExecuteScriptRequest, FetchScriptResultsRequest,
    FetchScriptResultsResponse, RollbackTransactionRequest, RollbackTransactionResponse,
    SessionState, TransactionMeta,
};
use ydb_grpc::ydb_proto::status_ids::StatusCode;

pub const QUERY_SESSION_ID: &str = "session-id-xyz";
pub const QUERY_TX_ID: &str = "tx-id-abc";

type AttachSessionStream =
    Pin<Box<dyn Stream<Item = Result<SessionState, tonic::Status>> + Send + 'static>>;

type ExecuteQueryStream =
    Pin<Box<dyn Stream<Item = Result<ExecuteQueryResponsePart, tonic::Status>> + Send + 'static>>;

#[derive(Default)]
pub struct MockQueryService;

#[tonic::async_trait]
impl QueryService for MockQueryService {
    type AttachSessionStream = AttachSessionStream;
    type ExecuteQueryStream = ExecuteQueryStream;

    async fn create_session(
        &self,
        _request: tonic::Request<CreateSessionRequest>,
    ) -> Result<tonic::Response<CreateSessionResponse>, tonic::Status> {
        Ok(tonic::Response::new(CreateSessionResponse {
            status: StatusCode::Success as i32,
            issues: Vec::new(),
            session_id: QUERY_SESSION_ID.to_string(),
            node_id: 0,
        }))
    }

    async fn delete_session(
        &self,
        _request: tonic::Request<DeleteSessionRequest>,
    ) -> Result<tonic::Response<DeleteSessionResponse>, tonic::Status> {
        Ok(tonic::Response::new(DeleteSessionResponse {
            status: StatusCode::Success as i32,
            issues: Vec::new(),
        }))
    }

    async fn attach_session(
        &self,
        _request: tonic::Request<AttachSessionRequest>,
    ) -> Result<tonic::Response<Self::AttachSessionStream>, tonic::Status> {
        let states = stream::once(async {
            Ok(SessionState {
                status: StatusCode::Success as i32,
                issues: Vec::new(),
                session_hint: None,
            })
        })
        .chain(stream::pending());
        Ok(tonic::Response::new(Box::pin(states)))
    }

    async fn begin_transaction(
        &self,
        _request: tonic::Request<BeginTransactionRequest>,
    ) -> Result<tonic::Response<BeginTransactionResponse>, tonic::Status> {
        Ok(tonic::Response::new(BeginTransactionResponse {
            status: StatusCode::Success as i32,
            issues: Vec::new(),
            tx_meta: Some(TransactionMeta {
                id: QUERY_TX_ID.to_string(),
            }),
        }))
    }

    async fn commit_transaction(
        &self,
        _request: tonic::Request<CommitTransactionRequest>,
    ) -> Result<tonic::Response<CommitTransactionResponse>, tonic::Status> {
        Ok(tonic::Response::new(CommitTransactionResponse {
            status: StatusCode::Success as i32,
            issues: Vec::new(),
        }))
    }

    async fn rollback_transaction(
        &self,
        _request: tonic::Request<RollbackTransactionRequest>,
    ) -> Result<tonic::Response<RollbackTransactionResponse>, tonic::Status> {
        Ok(tonic::Response::new(RollbackTransactionResponse {
            status: StatusCode::Success as i32,
            issues: Vec::new(),
        }))
    }

    async fn execute_query(
        &self,
        _request: tonic::Request<ExecuteQueryRequest>,
    ) -> Result<tonic::Response<Self::ExecuteQueryStream>, tonic::Status> {
        Ok(tonic::Response::new(Box::pin(iter([]))))
    }

    async fn execute_script(
        &self,
        _request: tonic::Request<ExecuteScriptRequest>,
    ) -> Result<tonic::Response<Operation>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "mock query ExecuteScript is not implemented",
        ))
    }

    async fn fetch_script_results(
        &self,
        _request: tonic::Request<FetchScriptResultsRequest>,
    ) -> Result<tonic::Response<FetchScriptResultsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "mock query FetchScriptResults is not implemented",
        ))
    }
}
