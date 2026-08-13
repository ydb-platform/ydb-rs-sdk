//! Mock-server tests for `QueryClient::explain`.
//!
//! These assert the wire contract that a real server cannot: that `EXEC_MODE_EXPLAIN` is sent,
//! that the request stays minimal (implicit session, no tx control, no parameters, no pool), and
//! how the SDK folds the response — plan and AST out of `exec_stats`.
mod mock_server;

use std::sync::{Arc, Mutex};

use ydb::{Client, ClientBuilder, YdbError, YdbResult};
use ydb_grpc::ydb_proto::query::{ExecMode, ExecuteQueryRequest, ExecuteQueryResponsePart};
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::table_stats::QueryStats;

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, Reply};
use crate::mock_server::query::{QueryIncoming, QueryReply};
use crate::mock_server::server::MockServer;

const DATABASE: &str = "/local";
const QUERY: &str = "SELECT 1";
const PLAN: &str = r#"{"Plan":{"Node Type":"ResultSet"}}"#;
const AST: &str = "(return (Just (AsList (AsStruct))))";

async fn make_client(server: &MockServer) -> YdbResult<Client> {
    ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .build()
    .await
}

/// What the mock answers an `ExecuteQuery` with.
#[derive(Clone)]
enum Response {
    /// Success carrying `exec_stats` with a plan and AST.
    WithStats,
    /// Success with no `exec_stats` at all.
    StatusOnly,
    /// A non-success status in the response part.
    Failing(StatusCode),
}

fn part(response: &Response) -> ExecuteQueryResponsePart {
    let (status, exec_stats) = match response {
        Response::WithStats => (
            StatusCode::Success,
            Some(QueryStats {
                query_plan: PLAN.to_string(),
                query_ast: AST.to_string(),
                ..Default::default()
            }),
        ),
        Response::StatusOnly => (StatusCode::Success, None),
        Response::Failing(status) => (*status, None),
    };
    ExecuteQueryResponsePart {
        status: status as i32,
        issues: vec![],
        result_set_index: 0,
        result_set: None,
        exec_stats,
        tx_meta: None,
    }
}

/// Records every `ExecuteQuery` request and answers it with the configured response.
/// Everything else falls through to the mock's default handler.
struct RecordingHandler {
    tx: Mutex<Option<FromHandlerToService>>,
    requests: Arc<Mutex<Vec<ExecuteQueryRequest>>>,
    response: Response,
}

impl RecordingHandler {
    fn new(response: Response) -> (Self, Arc<Mutex<Vec<ExecuteQueryRequest>>>) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let handler = Self {
            tx: Mutex::new(None),
            requests: requests.clone(),
            response,
        };
        (handler, requests)
    }

    fn send(&self, reply: QueryReply) {
        self.tx
            .lock()
            .unwrap()
            .as_ref()
            .expect("mock query reply channel must be set before replies are sent")
            .send(Reply::Query(reply))
            .expect("mock server failed to send query reply");
    }
}

impl Handler for RecordingHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        *self.tx.lock().unwrap() = Some(tx);
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        let Incoming::Query(QueryIncoming::ExecuteQuery(req, stream_id)) = incoming else {
            return Some(incoming);
        };
        self.requests.lock().unwrap().push(req);
        self.send(QueryReply::ExecuteQuery {
            stream_id,
            part: part(&self.response),
        });
        self.send(QueryReply::ExecuteQueryClose { stream_id });
        None
    }
}

async fn start(response: Response) -> (MockServer, Client, Arc<Mutex<Vec<ExecuteQueryRequest>>>) {
    let (handler, requests) = RecordingHandler::new(response);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await.expect("build client");
    (server, client, requests)
}

/// Every analysis request must be minimal: implicit session, no transaction, no parameters,
/// no workload-manager pool.
fn assert_minimal_request(req: &ExecuteQueryRequest, expected_mode: ExecMode) {
    assert_eq!(req.exec_mode, expected_mode as i32, "exec_mode");
    assert!(req.session_id.is_empty(), "session_id must be empty");
    assert!(req.tx_control.is_none(), "tx_control must be absent");
    assert!(req.parameters.is_empty(), "parameters must be empty");
    assert!(req.pool_id.is_empty(), "pool_id must be empty");
}

fn only_request(requests: &Arc<Mutex<Vec<ExecuteQueryRequest>>>) -> ExecuteQueryRequest {
    let recorded = requests.lock().unwrap();
    assert_eq!(recorded.len(), 1, "expected exactly one ExecuteQuery");
    recorded[0].clone()
}

#[tokio::test]
async fn explain_sends_explain_mode_and_returns_plan_and_ast() {
    let (_server, client, requests) = start(Response::WithStats).await;

    let result = client
        .query_client()
        .explain(QUERY)
        .await
        .expect("explain must succeed");

    assert_eq!(result.query_plan, PLAN);
    assert_eq!(result.query_ast, AST);
    assert_minimal_request(&only_request(&requests), ExecMode::Explain);
}

#[tokio::test]
async fn explain_without_stats_is_an_error() {
    let (_server, client, _requests) = start(Response::StatusOnly).await;

    let err = client
        .query_client()
        .explain(QUERY)
        .await
        .expect_err("explain without exec_stats must fail");

    match err {
        YdbError::Custom(message) => {
            assert!(
                message.contains("no query plan"),
                "error must explain the missing plan, got: {message}"
            );
        }
        other => panic!("expected a Custom error, got {other:?}"),
    }
}

#[tokio::test]
async fn non_success_status_is_propagated() {
    let (_server, client, _requests) = start(Response::Failing(StatusCode::BadRequest)).await;

    let err = client
        .query_client()
        .explain(QUERY)
        .await
        .expect_err("explain must propagate the status");

    assert!(
        matches!(err, YdbError::YdbStatusError(_)),
        "expected a server status error, got {err:?}"
    );
}
