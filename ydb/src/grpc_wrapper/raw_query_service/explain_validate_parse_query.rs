//! `ExecuteQuery` in the non-executing modes: `PARSE`, `VALIDATE`, `EXPLAIN`.
//!
//! These modes analyze a query instead of running it, so the request is a stripped-down
//! [`ExecuteQueryRequest`]: implicit session, no transaction control, no parameters, no
//! workload-manager pool. Responses carry no rows — only a status, and for `EXPLAIN` the
//! plan and AST inside `exec_stats`.

use std::collections::HashMap;

use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_query_service::status::check_status;
use ydb_grpc::ydb_proto::query::{
    ExecMode, ExecuteQueryRequest, ExecuteQueryResponsePart, QueryContent, SchemaInclusionMode,
    StatsMode, Syntax, execute_query_request,
};
use ydb_grpc::ydb_proto::result_set::Format;

/// Non-executing `Ydb.Query.ExecMode` values.
///
/// Only [`Explain`](Self::Explain) has a public entry point today. The Query Service rejects the
/// other two on every server we can test against — `rpc_execute_query.cpp`'s `ParseQueryAction`
/// has no `PARSE` branch, and `ValidateQuery` accepts only script query types, which the Query
/// Service never uses. They are kept here so the decision can be made on PR review.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "Parse/Validate kept pending PR review — see type docs"
)]
pub(crate) enum RawExecMode {
    /// Syntax check only; no schema or type resolution.
    Parse,
    /// Syntax, types and schema; no execution.
    Validate,
    /// Full compilation producing a query plan and MiniKQL AST.
    Explain,
}

impl RawExecMode {
    fn proto_exec_mode(self) -> ExecMode {
        match self {
            Self::Parse => ExecMode::Parse,
            Self::Validate => ExecMode::Validate,
            Self::Explain => ExecMode::Explain,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RawExplainValidateParseRequest {
    pub yql_text: String,
    pub mode: RawExecMode,
}

impl RawExplainValidateParseRequest {
    pub(crate) fn new(yql_text: impl Into<String>, mode: RawExecMode) -> Self {
        Self {
            yql_text: yql_text.into(),
            mode,
        }
    }

    pub(crate) fn into_proto(self) -> ExecuteQueryRequest {
        ExecuteQueryRequest {
            // Analysis modes hold no server-side state: always an implicit session.
            session_id: String::new(),
            exec_mode: self.mode.proto_exec_mode() as i32,
            tx_control: None,
            query: Some(execute_query_request::Query::QueryContent(QueryContent {
                syntax: Syntax::YqlV1 as i32,
                text: self.yql_text,
            })),
            parameters: HashMap::new(),
            // Never requested: the server reports stats for EXPLAIN regardless of this field
            // (`NeedReportStats` short-circuits on the exec mode), and the other modes have no
            // stats to read.
            stats_mode: StatsMode::Unspecified as i32,
            concurrent_result_sets: false,
            response_part_limit_bytes: 0,
            pool_id: String::new(),
            stats_period_ms: 0,
            schema_inclusion_mode: SchemaInclusionMode::Unspecified as i32,
            result_set_format: Format::Unspecified as i32,
            arrow_format_settings: None,
        }
    }
}

/// Plan and AST strings carried by `QueryStats` on an `EXPLAIN` response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RawQueryStatsPlan {
    pub query_plan: String,
    pub query_ast: String,
}

/// Folds an analysis-mode response stream: checks every part's status and keeps the last
/// non-empty plan/AST. Result sets and `tx_meta` never appear in these modes and are ignored.
#[derive(Default)]
pub(crate) struct QueryStatsPlanCollector {
    plan: Option<RawQueryStatsPlan>,
}

impl QueryStatsPlanCollector {
    pub(crate) fn ingest(&mut self, part: &ExecuteQueryResponsePart) -> RawResult<()> {
        check_status(part.status, &part.issues)?;
        let Some(stats) = &part.exec_stats else {
            return Ok(());
        };
        if stats.query_plan.is_empty() && stats.query_ast.is_empty() {
            return Ok(());
        }
        self.plan = Some(RawQueryStatsPlan {
            query_plan: stats.query_plan.clone(),
            query_ast: stats.query_ast.clone(),
        });
        Ok(())
    }

    pub(crate) fn finish(self) -> Option<RawQueryStatsPlan> {
        self.plan
    }
}

/// Drain the whole stream: analysis responses are short, and the plan typically arrives last.
pub(crate) async fn collect_stats_plan(
    stream: &mut tonic::Streaming<ExecuteQueryResponsePart>,
) -> RawResult<Option<RawQueryStatsPlan>> {
    let mut collector = QueryStatsPlanCollector::default();
    while let Some(part) = stream.message().await? {
        collector.ingest(&part)?;
    }
    Ok(collector.finish())
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::grpc_wrapper::raw_errors::RawError;
    use ydb_grpc::ydb_proto::status_ids::StatusCode;
    use ydb_grpc::ydb_proto::table_stats::QueryStats;

    fn part(status: StatusCode, stats: Option<QueryStats>) -> ExecuteQueryResponsePart {
        ExecuteQueryResponsePart {
            status: status as i32,
            issues: vec![],
            result_set_index: 0,
            result_set: None,
            exec_stats: stats,
            tx_meta: None,
        }
    }

    fn stats(plan: &str, ast: &str) -> QueryStats {
        QueryStats {
            query_plan: plan.to_string(),
            query_ast: ast.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn each_mode_maps_to_its_proto_exec_mode() {
        for (mode, expected) in [
            (RawExecMode::Parse, ExecMode::Parse),
            (RawExecMode::Validate, ExecMode::Validate),
            (RawExecMode::Explain, ExecMode::Explain),
        ] {
            let proto = RawExplainValidateParseRequest::new("SELECT 1", mode).into_proto();
            assert_eq!(proto.exec_mode, expected as i32, "exec_mode for {mode:?}");
        }
    }

    /// The server decides on exec mode alone (`NeedReportStats` returns true for EXPLAIN whatever
    /// `stats_mode` says), so no analysis request asks for statistics.
    #[test]
    fn no_mode_requests_statistics() {
        for mode in [
            RawExecMode::Parse,
            RawExecMode::Validate,
            RawExecMode::Explain,
        ] {
            let proto = RawExplainValidateParseRequest::new("SELECT 1", mode).into_proto();
            assert_eq!(
                proto.stats_mode,
                StatsMode::Unspecified as i32,
                "stats for {mode:?}"
            );
        }
    }

    #[test]
    fn request_is_minimal_and_uses_implicit_session() {
        let proto =
            RawExplainValidateParseRequest::new("SELECT 1", RawExecMode::Validate).into_proto();

        assert!(proto.session_id.is_empty());
        assert!(proto.tx_control.is_none());
        assert!(proto.parameters.is_empty());
        assert!(proto.pool_id.is_empty());
        assert!(!proto.concurrent_result_sets);
        assert_eq!(
            proto.query,
            Some(execute_query_request::Query::QueryContent(QueryContent {
                syntax: Syntax::YqlV1 as i32,
                text: "SELECT 1".to_string(),
            }))
        );
    }

    #[test]
    fn last_non_empty_stats_wins() {
        let mut collector = QueryStatsPlanCollector::default();
        collector
            .ingest(&part(StatusCode::Success, Some(stats("plan-1", "ast-1"))))
            .expect("first part");
        collector
            .ingest(&part(StatusCode::Success, Some(stats("plan-2", "ast-2"))))
            .expect("second part");

        assert_eq!(
            collector.finish(),
            Some(RawQueryStatsPlan {
                query_plan: "plan-2".to_string(),
                query_ast: "ast-2".to_string(),
            })
        );
    }

    #[test]
    fn empty_stats_do_not_overwrite_a_collected_plan() {
        let mut collector = QueryStatsPlanCollector::default();
        collector
            .ingest(&part(StatusCode::Success, Some(stats("plan", "ast"))))
            .expect("plan part");
        collector
            .ingest(&part(StatusCode::Success, Some(stats("", ""))))
            .expect("stats-only part");
        collector
            .ingest(&part(StatusCode::Success, None))
            .expect("trailing part");

        assert_eq!(
            collector.finish(),
            Some(RawQueryStatsPlan {
                query_plan: "plan".to_string(),
                query_ast: "ast".to_string(),
            })
        );
    }

    #[test]
    fn absent_stats_yield_none() {
        let mut collector = QueryStatsPlanCollector::default();
        collector
            .ingest(&part(StatusCode::Success, None))
            .expect("status-only part");

        assert_eq!(collector.finish(), None);
    }

    #[test]
    fn error_status_propagates() {
        let mut collector = QueryStatsPlanCollector::default();
        let err = collector
            .ingest(&part(StatusCode::BadRequest, None))
            .expect_err("bad request must fail");

        assert!(matches!(err, RawError::YdbStatus(_)));
    }
}
