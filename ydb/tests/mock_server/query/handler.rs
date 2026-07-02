use std::fmt;

use tokio::sync::{mpsc, oneshot};
use ydb_grpc::ydb_proto::query::{
    AttachSessionRequest, BeginTransactionRequest, BeginTransactionResponse,
    CommitTransactionRequest, CommitTransactionResponse, CreateSessionRequest,
    CreateSessionResponse, DeleteSessionRequest, DeleteSessionResponse, ExecuteQueryRequest,
    ExecuteQueryResponsePart, RollbackTransactionRequest, RollbackTransactionResponse,
    SessionState,
};

type OneshotTx<T> = oneshot::Sender<Result<tonic::Response<T>, tonic::Status>>;

pub enum QueryIncoming {
    CreateSession(CreateSessionRequest, OneshotTx<CreateSessionResponse>),
    DeleteSession(DeleteSessionRequest, OneshotTx<DeleteSessionResponse>),
    BeginTransaction(BeginTransactionRequest, OneshotTx<BeginTransactionResponse>),
    CommitTransaction(
        CommitTransactionRequest,
        OneshotTx<CommitTransactionResponse>,
    ),
    RollbackTransaction(
        RollbackTransactionRequest,
        OneshotTx<RollbackTransactionResponse>,
    ),
    AttachSession(AttachSessionRequest, u64),
    ExecuteQuery(ExecuteQueryRequest, u64),
}

impl fmt::Debug for QueryIncoming {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateSession(req, _) => f.debug_tuple("CreateSession").field(req).finish(),
            Self::DeleteSession(req, _) => f.debug_tuple("DeleteSession").field(req).finish(),
            Self::BeginTransaction(req, _) => f.debug_tuple("BeginTransaction").field(req).finish(),
            Self::CommitTransaction(req, _) => {
                f.debug_tuple("CommitTransaction").field(req).finish()
            }
            Self::RollbackTransaction(req, _) => {
                f.debug_tuple("RollbackTransaction").field(req).finish()
            }
            Self::AttachSession(req, id) => {
                f.debug_tuple("AttachSession").field(req).field(id).finish()
            }
            Self::ExecuteQuery(req, id) => {
                f.debug_tuple("ExecuteQuery").field(req).field(id).finish()
            }
        }
    }
}

#[derive(Debug)]
pub enum QueryReply {
    AttachSession {
        stream_id: u64,
        state: SessionState,
    },
    AttachSessionFail {
        stream_id: u64,
        status: tonic::Status,
    },
    AttachSessionClose {
        stream_id: u64,
    },
    ExecuteQuery {
        stream_id: u64,
        part: ExecuteQueryResponsePart,
    },
    ExecuteQueryFail {
        stream_id: u64,
        status: tonic::Status,
    },
    ExecuteQueryClose {
        stream_id: u64,
    },
}

pub type QueryTx = mpsc::UnboundedSender<QueryReply>;
pub type QueryRx = mpsc::UnboundedReceiver<QueryReply>;
