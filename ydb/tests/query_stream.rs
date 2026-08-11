#![recursion_limit = "256"]
mod mock_server;

use std::collections::VecDeque;
use std::sync::Mutex;

use futures_util::TryStreamExt;
use futures_util::stream::FusedStream;
use ydb::{Client, ClientBuilder, SessionPoolSettings, Value, YdbResult};
use ydb_grpc::ydb_proto::query::ExecuteQueryResponsePart;
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::{Column, ResultSet, Type, Value as ProtoValue, r#type, value};

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, ReplySink};
use crate::mock_server::query::{QueryIncoming, QueryReply};
use crate::mock_server::server::MockServer;

const DATABASE: &str = "/local";

enum StreamEnd {
    Close,
    Fail(tonic::Status),
    Pending,
}

struct StreamScript {
    parts: Vec<ExecuteQueryResponsePart>,
    end: StreamEnd,
}

impl StreamScript {
    fn closed(parts: Vec<ExecuteQueryResponsePart>) -> Self {
        Self {
            parts,
            end: StreamEnd::Close,
        }
    }

    fn failed(parts: Vec<ExecuteQueryResponsePart>, status: tonic::Status) -> Self {
        Self {
            parts,
            end: StreamEnd::Fail(status),
        }
    }

    fn pending(parts: Vec<ExecuteQueryResponsePart>) -> Self {
        Self {
            parts,
            end: StreamEnd::Pending,
        }
    }
}

struct ScriptedStreamHandler {
    replies: ReplySink,
    scripts: Mutex<VecDeque<StreamScript>>,
}

impl ScriptedStreamHandler {
    fn new(scripts: impl IntoIterator<Item = StreamScript>) -> Self {
        Self {
            replies: ReplySink::default(),
            scripts: Mutex::new(scripts.into_iter().collect()),
        }
    }
}

impl Handler for ScriptedStreamHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies.set_channel(tx);
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        let Incoming::Query(QueryIncoming::ExecuteQuery(_, stream_id)) = incoming else {
            return Some(incoming);
        };
        let script = self
            .scripts
            .lock()
            .expect("query stream script lock")
            .pop_front()
            .expect("one script per ExecuteQuery call");
        for part in script.parts {
            self.replies
                .send(QueryReply::ExecuteQuery { stream_id, part });
        }
        match script.end {
            StreamEnd::Close => self
                .replies
                .send(QueryReply::ExecuteQueryClose { stream_id }),
            StreamEnd::Fail(status) => self
                .replies
                .send(QueryReply::ExecuteQueryFail { stream_id, status }),
            StreamEnd::Pending => {}
        }
        None
    }
}

async fn make_client(
    scripts: impl IntoIterator<Item = StreamScript>,
) -> YdbResult<(Client, MockServer)> {
    let (server, _replies) = MockServer::start(ScriptedStreamHandler::new(scripts)).await;
    let client = ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .build()
    .await?
    .with_session_pool(SessionPoolSettings::new().with_limit(1))
    .await?;
    Ok((client, server))
}

fn result_part(index: i64, column: Option<&str>, values: &[i64]) -> ExecuteQueryResponsePart {
    let columns = column.map(|name| {
        vec![Column {
            name: name.to_string(),
            r#type: Some(Type {
                r#type: Some(r#type::Type::TypeId(r#type::PrimitiveTypeId::Int64 as i32)),
            }),
        }]
    });
    ExecuteQueryResponsePart {
        status: StatusCode::Success as i32,
        issues: Vec::new(),
        result_set_index: index,
        result_set: columns.map(|columns| ResultSet {
            columns,
            rows: values
                .iter()
                .map(|value| ProtoValue {
                    items: vec![ProtoValue {
                        value: Some(value::Value::Int64Value(*value)),
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }),
        exec_stats: None,
        tx_meta: None,
    }
}

fn failed_part(status: StatusCode) -> ExecuteQueryResponsePart {
    ExecuteQueryResponsePart {
        status: status as i32,
        issues: Vec::new(),
        result_set_index: 0,
        result_set: None,
        exec_stats: None,
        tx_meta: None,
    }
}

fn result_values(result_set: ydb::ResultSet, column: &str) -> YdbResult<Vec<i64>> {
    result_set
        .rows()
        .map(|mut row| match row.remove_field_by_name(column)? {
            Value::Int64(value) => Ok(value),
            value => Err(ydb::YdbError::Custom(format!(
                "expected Int64 result value, got {value:?}"
            ))),
        })
        .collect()
}

#[tokio::test]
async fn final_result_set_returns_the_pooled_session() -> YdbResult<()> {
    let (client, _server) = make_client([StreamScript::closed(vec![result_part(
        0,
        Some("value"),
        &[1],
    )])])
    .await?;
    let mut query = client.query_client();
    let mut stream = query.query("SELECT 1").await?;
    assert_eq!(client.session_pool_stats().in_use, 1);

    let part = stream
        .try_next()
        .await?
        .expect("query must return one result part");
    assert_eq!(part.result_set_index(), 0);
    assert_eq!(result_values(part.into_result_set(), "value")?, vec![1]);
    assert_eq!(client.session_pool_stats().in_use, 1);
    assert!(stream.try_next().await?.is_none());
    assert!(stream.is_terminated());
    assert_eq!(client.session_pool_stats().in_use, 0);
    assert_eq!(client.session_pool_stats().idle, 1);
    Ok(())
}

#[tokio::test]
async fn dropping_before_eof_discards_the_pooled_session() -> YdbResult<()> {
    let (client, _server) = make_client([
        StreamScript::pending(Vec::new()),
        StreamScript::closed(Vec::new()),
    ])
    .await?;
    let mut query = client.query_client();
    let stream = query.query("SELECT 1").await?;
    assert_eq!(client.session_pool_stats().in_use, 1);

    drop(stream);
    let stats = client.session_pool_stats();
    assert_eq!(stats.in_use, 0);
    assert_eq!(stats.idle, 0);
    assert_eq!(stats.sessions_created, 1);

    query.exec("SELECT 2").await?;
    let stats = client.session_pool_stats();
    assert_eq!(stats.sessions_created, 2);
    assert_eq!(stats.idle, 1);
    Ok(())
}

#[tokio::test]
async fn response_status_error_discards_the_pooled_session() -> YdbResult<()> {
    let (client, _server) = make_client([
        StreamScript::closed(vec![failed_part(StatusCode::BadRequest)]),
        StreamScript::closed(Vec::new()),
    ])
    .await?;
    let mut query = client.query_client();
    {
        let mut stream = query.query("SELECT broken").await?;
        assert!(stream.try_next().await.is_err());
        assert!(stream.is_terminated());
        assert!(stream.try_next().await?.is_none());
    }
    assert_eq!(client.session_pool_stats().idle, 0);

    query.exec("SELECT 2").await?;
    assert_eq!(client.session_pool_stats().sessions_created, 2);
    Ok(())
}

#[tokio::test]
async fn finish_drains_unread_parts_and_returns_the_pooled_session() -> YdbResult<()> {
    let (client, _server) = make_client([StreamScript::closed(vec![result_part(
        0,
        Some("value"),
        &[1],
    )])])
    .await?;
    let mut query = client.query_client();
    let stream = query.query("SELECT 1").await?;

    stream.finish().await?;
    assert_eq!(client.session_pool_stats().in_use, 0);
    assert_eq!(client.session_pool_stats().idle, 1);
    Ok(())
}

#[tokio::test]
async fn finish_propagates_unread_status_error_and_discards_session() -> YdbResult<()> {
    let (client, _server) = make_client([StreamScript::closed(vec![
        result_part(0, Some("value"), &[1]),
        failed_part(StatusCode::BadRequest),
    ])])
    .await?;
    let mut query = client.query_client();
    let stream = query.query("SELECT broken").await?;

    assert!(stream.finish().await.is_err());
    assert_eq!(client.session_pool_stats().in_use, 0);
    assert_eq!(client.session_pool_stats().idle, 0);
    Ok(())
}

#[tokio::test]
async fn transport_error_discards_the_pooled_session() -> YdbResult<()> {
    let (client, _server) = make_client([StreamScript::failed(
        Vec::new(),
        tonic::Status::unavailable("response stream failed"),
    )])
    .await?;
    let mut query = client.query_client();
    let mut stream = query.query("SELECT broken").await?;

    assert!(stream.try_next().await.is_err());
    assert_eq!(client.session_pool_stats().in_use, 0);
    assert_eq!(client.session_pool_stats().idle, 0);
    Ok(())
}

#[tokio::test]
async fn stream_yields_result_parts_without_materializing() -> YdbResult<()> {
    let (client, _server) = make_client([StreamScript::closed(vec![
        result_part(0, Some("first"), &[10]),
        result_part(0, None, &[]),
        result_part(0, Some("first"), &[11]),
        result_part(1, Some("second"), &[20, 21]),
    ])])
    .await?;
    let mut query = client.query_client();
    let mut stream = query.query("SELECT 1; SELECT 2").await?;

    let first = stream.try_next().await?.expect("first result part");
    let continuation = stream.try_next().await?.expect("continuation part");
    let second = stream.try_next().await?.expect("second result part");
    assert_eq!(first.result_set_index(), 0);
    assert_eq!(continuation.result_set_index(), 0);
    assert_eq!(second.result_set_index(), 1);
    assert_eq!(result_values(first.into_result_set(), "first")?, vec![10]);
    assert_eq!(
        result_values(continuation.into_result_set(), "first")?,
        vec![11]
    );
    assert_eq!(
        result_values(second.into_result_set(), "second")?,
        vec![20, 21]
    );
    assert!(stream.try_next().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn materialized_result_set_combines_streamed_parts() -> YdbResult<()> {
    let (client, _server) = make_client([StreamScript::closed(vec![
        result_part(0, Some("value"), &[10]),
        result_part(0, Some("value"), &[11, 12]),
    ])])
    .await?;
    let mut query = client.query_client();

    let result_set = query.query_result_set("SELECT 1").await?;

    assert_eq!(result_values(result_set, "value")?, vec![10, 11, 12]);
    Ok(())
}
