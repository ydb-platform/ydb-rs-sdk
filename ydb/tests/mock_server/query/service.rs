use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::{stream, Stream, StreamExt};
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnboundedReceiverStream;
use ydb_grpc::ydb_proto::operations::Operation;
use ydb_grpc::ydb_proto::query::v1::query_service_server::QueryService;
use ydb_grpc::ydb_proto::query::{
    AttachSessionRequest, BeginTransactionRequest, BeginTransactionResponse,
    CommitTransactionRequest, CommitTransactionResponse, CreateSessionRequest,
    CreateSessionResponse, DeleteSessionRequest, DeleteSessionResponse, ExecuteQueryRequest,
    ExecuteQueryResponsePart, ExecuteScriptRequest, FetchScriptResultsRequest,
    FetchScriptResultsResponse, RollbackTransactionRequest, RollbackTransactionResponse,
    SessionState,
};

use crate::mock_server::handler::{FromServiceToServerTx, Incoming};
use crate::mock_server::topic::sender::{StreamCommand, StreamSender};

use super::handler::{QueryIncoming, QueryReply, QueryRx};

type AttachSessionStream =
    Pin<Box<dyn Stream<Item = Result<SessionState, tonic::Status>> + Send + 'static>>;

type ExecuteQueryStream =
    Pin<Box<dyn Stream<Item = Result<ExecuteQueryResponsePart, tonic::Status>> + Send + 'static>>;

pub struct MockQueryService {
    to_server: FromServiceToServerTx,
    next_stream_id: AtomicU64,
    pub(crate) attach_sender: StreamSender<SessionState>,
    pub(crate) execute_query_sender: StreamSender<ExecuteQueryResponsePart>,
}

impl MockQueryService {
    pub fn new(to_server: FromServiceToServerTx, rx: QueryRx) -> Self {
        let attach_sender = StreamSender::new();
        let execute_query_sender = StreamSender::new();
        tokio::spawn(Self::handle_messages(
            attach_sender.clone(),
            execute_query_sender.clone(),
            rx,
        ));
        Self {
            to_server,
            next_stream_id: AtomicU64::new(0),
            attach_sender,
            execute_query_sender,
        }
    }

    async fn handle_messages(
        attach_sender: StreamSender<SessionState>,
        execute_query_sender: StreamSender<ExecuteQueryResponsePart>,
        mut rx: QueryRx,
    ) {
        while let Some(msg) = rx.recv().await {
            match msg {
                QueryReply::AttachSession { stream_id, state } => {
                    let _ = attach_sender.send_to(stream_id, state);
                }
                QueryReply::AttachSessionFail { stream_id, status } => {
                    let _ = attach_sender.fail(stream_id, status);
                }
                QueryReply::AttachSessionClose { stream_id } => {
                    let _ = attach_sender.close(stream_id);
                }
                QueryReply::ExecuteQuery { stream_id, part } => {
                    let _ = execute_query_sender.send_to(stream_id, part);
                }
                QueryReply::ExecuteQueryFail { stream_id, status } => {
                    let _ = execute_query_sender.fail(stream_id, status);
                }
                QueryReply::ExecuteQueryClose { stream_id } => {
                    let _ = execute_query_sender.close(stream_id);
                }
            }
        }
    }

    fn send_unary<T: Send + 'static>(
        &self,
        incoming: QueryIncoming,
        rx: oneshot::Receiver<Result<tonic::Response<T>, tonic::Status>>,
    ) -> impl std::future::Future<Output = Result<tonic::Response<T>, tonic::Status>> {
        let _ = self.to_server.send(Incoming::Query(incoming));
        async move {
            rx.await
                .unwrap_or_else(|_| Err(tonic::Status::internal("mock handler dropped oneshot")))
        }
    }
}

#[tonic::async_trait]
impl QueryService for MockQueryService {
    type AttachSessionStream = AttachSessionStream;
    type ExecuteQueryStream = ExecuteQueryStream;

    async fn create_session(
        &self,
        request: tonic::Request<CreateSessionRequest>,
    ) -> Result<tonic::Response<CreateSessionResponse>, tonic::Status> {
        let (tx, rx) = oneshot::channel();
        self.send_unary(QueryIncoming::CreateSession(request.into_inner(), tx), rx)
            .await
    }

    async fn delete_session(
        &self,
        request: tonic::Request<DeleteSessionRequest>,
    ) -> Result<tonic::Response<DeleteSessionResponse>, tonic::Status> {
        let (tx, rx) = oneshot::channel();
        self.send_unary(QueryIncoming::DeleteSession(request.into_inner(), tx), rx)
            .await
    }

    async fn attach_session(
        &self,
        request: tonic::Request<AttachSessionRequest>,
    ) -> Result<tonic::Response<Self::AttachSessionStream>, tonic::Status> {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let rx = self.attach_sender.register_stream(stream_id);
        let sender = self.attach_sender.clone();
        let _ = self
            .to_server
            .send(Incoming::Query(QueryIncoming::AttachSession(
                request.into_inner(),
                stream_id,
            )));

        let responses = UnboundedReceiverStream::new(rx);
        let responses = stream::unfold(
            (responses, sender, stream_id),
            |(mut responses, sender, stream_id)| async move {
                match responses.next().await {
                    Some(StreamCommand::Reply(state)) => {
                        Some((Ok(state), (responses, sender, stream_id)))
                    }
                    Some(StreamCommand::Fail(status)) => {
                        sender.unregister_stream(stream_id);
                        Some((Err(status), (responses, sender, stream_id)))
                    }
                    Some(StreamCommand::Close) | None => {
                        sender.unregister_stream(stream_id);
                        None
                    }
                }
            },
        );
        Ok(tonic::Response::new(Box::pin(responses)))
    }

    async fn begin_transaction(
        &self,
        request: tonic::Request<BeginTransactionRequest>,
    ) -> Result<tonic::Response<BeginTransactionResponse>, tonic::Status> {
        let (tx, rx) = oneshot::channel();
        self.send_unary(
            QueryIncoming::BeginTransaction(request.into_inner(), tx),
            rx,
        )
        .await
    }

    async fn commit_transaction(
        &self,
        request: tonic::Request<CommitTransactionRequest>,
    ) -> Result<tonic::Response<CommitTransactionResponse>, tonic::Status> {
        let (tx, rx) = oneshot::channel();
        self.send_unary(
            QueryIncoming::CommitTransaction(request.into_inner(), tx),
            rx,
        )
        .await
    }

    async fn rollback_transaction(
        &self,
        request: tonic::Request<RollbackTransactionRequest>,
    ) -> Result<tonic::Response<RollbackTransactionResponse>, tonic::Status> {
        let (tx, rx) = oneshot::channel();
        self.send_unary(
            QueryIncoming::RollbackTransaction(request.into_inner(), tx),
            rx,
        )
        .await
    }

    async fn execute_query(
        &self,
        request: tonic::Request<ExecuteQueryRequest>,
    ) -> Result<tonic::Response<Self::ExecuteQueryStream>, tonic::Status> {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let rx = self.execute_query_sender.register_stream(stream_id);
        let sender = self.execute_query_sender.clone();
        let _ = self
            .to_server
            .send(Incoming::Query(QueryIncoming::ExecuteQuery(
                request.into_inner(),
                stream_id,
            )));

        let responses = UnboundedReceiverStream::new(rx);
        let responses = stream::unfold(
            (responses, sender, stream_id),
            |(mut responses, sender, stream_id)| async move {
                match responses.next().await {
                    Some(StreamCommand::Reply(part)) => {
                        Some((Ok(part), (responses, sender, stream_id)))
                    }
                    Some(StreamCommand::Fail(status)) => {
                        sender.unregister_stream(stream_id);
                        Some((Err(status), (responses, sender, stream_id)))
                    }
                    Some(StreamCommand::Close) | None => {
                        sender.unregister_stream(stream_id);
                        None
                    }
                }
            },
        );
        Ok(tonic::Response::new(Box::pin(responses)))
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
