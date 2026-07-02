use tracing::error;
use ydb_grpc::ydb_proto::query::{
    BeginTransactionResponse, CommitTransactionResponse, CreateSessionResponse,
    DeleteSessionResponse, RollbackTransactionResponse, SessionState, TransactionMeta,
};
use ydb_grpc::ydb_proto::status_ids::StatusCode;

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, Reply};

use super::handler::{QueryIncoming, QueryReply};

pub const QUERY_SESSION_ID: &str = "session-id-xyz";
pub const QUERY_TX_ID: &str = "tx-id-abc";

pub struct QueryDefaultHandler {
    tx: FromHandlerToService,
}

impl QueryDefaultHandler {
    pub fn with_tx(tx: FromHandlerToService) -> Self {
        Self { tx }
    }
}

impl Handler for QueryDefaultHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.tx = tx;
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        let Incoming::Query(incoming) = incoming else {
            error!(?incoming, "query default handler got non-query message");
            return Some(incoming);
        };

        match incoming {
            QueryIncoming::CreateSession(_, reply_tx) => {
                let _ = reply_tx.send(Ok(tonic::Response::new(CreateSessionResponse {
                    status: StatusCode::Success as i32,
                    issues: Vec::new(),
                    session_id: QUERY_SESSION_ID.to_string(),
                    node_id: 0,
                })));
            }
            QueryIncoming::DeleteSession(_, reply_tx) => {
                let _ = reply_tx.send(Ok(tonic::Response::new(DeleteSessionResponse {
                    status: StatusCode::Success as i32,
                    issues: Vec::new(),
                })));
            }
            QueryIncoming::BeginTransaction(_, reply_tx) => {
                let _ = reply_tx.send(Ok(tonic::Response::new(BeginTransactionResponse {
                    status: StatusCode::Success as i32,
                    issues: Vec::new(),
                    tx_meta: Some(TransactionMeta {
                        id: QUERY_TX_ID.to_string(),
                    }),
                })));
            }
            QueryIncoming::CommitTransaction(_, reply_tx) => {
                let _ = reply_tx.send(Ok(tonic::Response::new(CommitTransactionResponse {
                    status: StatusCode::Success as i32,
                    issues: Vec::new(),
                })));
            }
            QueryIncoming::RollbackTransaction(_, reply_tx) => {
                let _ = reply_tx.send(Ok(tonic::Response::new(RollbackTransactionResponse {
                    status: StatusCode::Success as i32,
                    issues: Vec::new(),
                })));
            }
            QueryIncoming::AttachSession(_, stream_id) => {
                let _ = self.tx.send(Reply::Query(QueryReply::AttachSession {
                    stream_id,
                    state: SessionState {
                        status: StatusCode::Success as i32,
                        issues: Vec::new(),
                        session_hint: None,
                    },
                }));
                // stream stays open — no close sent, mirrors stream::pending() behavior
            }
            QueryIncoming::ExecuteQuery(_, stream_id) => {
                let _ = self
                    .tx
                    .send(Reply::Query(QueryReply::ExecuteQueryClose { stream_id }));
            }
        }

        None
    }
}
