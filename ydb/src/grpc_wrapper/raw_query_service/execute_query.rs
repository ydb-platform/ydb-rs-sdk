use std::collections::HashMap;
use std::time::Duration;

use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_query_service::status::check_status;
use crate::grpc_wrapper::raw_query_service::transaction_control::TransactionId;
use crate::grpc_wrapper::raw_table_service::value::{
    RawColumn, RawResultSet, RawTypedValue, RawValue,
};
use crate::types::Value;
use ydb_grpc::ydb_proto::query::{
    ExecMode, ExecuteQueryRequest, ExecuteQueryResponsePart, QueryContent, SchemaInclusionMode,
    StatsMode, Syntax, execute_query_request,
};
use ydb_grpc::ydb_proto::result_set::Format;

/// `Ydb.Query.ExecMode` values the SDK sends.
///
/// The proto also defines `PARSE` and `VALIDATE`, which the Query Service does not accept:
/// `ParseQueryAction` has no `PARSE` branch, and `VALIDATE` reaches `ValidateQuery`, which only
/// handles script query types. Neither is represented here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum RawExecMode {
    /// Run the query.
    #[default]
    Execute,
    /// Compile the query and report its plan and AST without running it.
    Explain,
}

impl From<RawExecMode> for ExecMode {
    fn from(mode: RawExecMode) -> Self {
        match mode {
            RawExecMode::Execute => Self::Execute,
            RawExecMode::Explain => Self::Explain,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RawExecuteQueryRequest {
    pub session_id: String,
    pub yql_text: String,
    pub parameters: HashMap<String, Value>,
    pub tx_control: Option<ydb_grpc::ydb_proto::query::TransactionControl>,
    pub collect_stats: bool,
    pub concurrent_result_sets: bool,
    pub exec_mode: RawExecMode,
}

impl RawExecuteQueryRequest {
    pub(crate) fn new(
        session_id: impl Into<String>,
        yql_text: impl Into<String>,
        parameters: HashMap<String, Value>,
        tx_control: Option<ydb_grpc::ydb_proto::query::TransactionControl>,
        collect_stats: bool,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            yql_text: yql_text.into(),
            parameters,
            tx_control,
            collect_stats,
            concurrent_result_sets: false,
            exec_mode: RawExecMode::Execute,
        }
    }

    pub fn into_proto(self) -> RawResult<ExecuteQueryRequest> {
        let mut parameters = HashMap::with_capacity(self.parameters.len());
        for (name, val) in self.parameters {
            let raw: RawTypedValue = val.try_into()?;
            parameters.insert(name, raw.into());
        }

        Ok(ExecuteQueryRequest {
            session_id: self.session_id,
            exec_mode: ExecMode::from(self.exec_mode) as i32,
            tx_control: self.tx_control,
            query: Some(execute_query_request::Query::QueryContent(QueryContent {
                syntax: Syntax::YqlV1 as i32,
                text: self.yql_text,
            })),
            parameters,
            stats_mode: if self.collect_stats {
                StatsMode::Basic as i32
            } else {
                StatsMode::None as i32
            },
            concurrent_result_sets: self.concurrent_result_sets,
            response_part_limit_bytes: 0,
            pool_id: String::new(),
            stats_period_ms: 0,
            schema_inclusion_mode: SchemaInclusionMode::Unspecified as i32,
            result_set_format: Format::Unspecified as i32,
            arrow_format_settings: None,
        })
    }
}

pub(crate) fn check_part(part: &ExecuteQueryResponsePart) -> RawResult<()> {
    check_status(part.status, &part.issues)
}

pub(crate) fn tx_id_from_part(part: &ExecuteQueryResponsePart) -> Option<TransactionId> {
    part.tx_meta
        .as_ref()
        .map(|m| m.id.clone())
        .and_then(TransactionId::from_server)
}

pub(crate) fn stats_from_part(part: &ExecuteQueryResponsePart) -> Option<Duration> {
    part.exec_stats
        .as_ref()
        .map(|stats| Duration::from_micros(stats.total_duration_us))
}

/// Plan and AST strings carried by `QueryStats`.
///
/// Which of the two the server fills in depends on the request. Measured against YDB 26.1.1:
///
/// - `EXPLAIN`, any `stats_mode` — both. The plan describes how the query *would* run.
/// - `EXECUTE` with `STATS_MODE_FULL` — both. The plan is larger than the `EXPLAIN` one because
///   actual execution statistics are merged into it.
/// - `EXECUTE` with `STATS_MODE_BASIC` — neither. This is what [`RawExecuteQueryRequest`]'s
///   `collect_stats` sends, so no `EXECUTE` the SDK issues today yields a plan.
/// - `EXECUTE` with `STATS_MODE_NONE` — no `exec_stats` on the part at all.
///
/// The server populates the two fields at different points — the AST is gated behind
/// `STATS_COLLECTION_FULL` in `kqp_session_actor.cpp` while the plan is not — so treat them as
/// independent and expect either to be empty.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RawQueryStatsPlan {
    pub query_plan: String,
    pub query_ast: String,
}

/// `None` when the part carries no `exec_stats`, or carries stats with neither field set — which
/// covers every `EXECUTE` the SDK currently issues (see [`RawQueryStatsPlan`]).
///
/// A `Some` is not a promise that both fields are filled in; callers that need both must check.
pub(crate) fn plan_from_part(part: &ExecuteQueryResponsePart) -> Option<RawQueryStatsPlan> {
    let stats = part.exec_stats.as_ref()?;
    if stats.query_plan.is_empty() && stats.query_ast.is_empty() {
        return None;
    }
    Some(RawQueryStatsPlan {
        query_plan: stats.query_plan.clone(),
        query_ast: stats.query_ast.clone(),
    })
}

pub(crate) fn append_result_set_part(
    columns: &mut Vec<RawColumn>,
    rows: &mut Vec<Vec<RawValue>>,
    truncated: &mut bool,
    part_set: RawResultSet,
) -> RawResult<()> {
    *truncated |= part_set.truncated;
    if !columns.is_empty()
        && !part_set.columns.is_empty()
        && !columns_compatible(columns, &part_set.columns)
    {
        return Err(crate::grpc_wrapper::raw_errors::RawError::custom(
            "column metadata mismatch between stream parts".to_string(),
        ));
    }
    if columns.is_empty() {
        *columns = part_set.columns;
    }
    rows.extend(part_set.rows);
    Ok(())
}

pub(super) fn columns_compatible(existing: &[RawColumn], new_cols: &[RawColumn]) -> bool {
    existing.len() == new_cols.len()
        && existing
            .iter()
            .zip(new_cols.iter())
            .all(|(left, right)| left.name == right.name && left.column_type == right.column_type)
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use ydb_grpc::ydb_proto::status_ids::StatusCode;
    use ydb_grpc::ydb_proto::table_stats::QueryStats;

    fn request(mode: RawExecMode) -> ExecuteQueryRequest {
        let mut req = RawExecuteQueryRequest::new("", "SELECT 1", HashMap::new(), None, false);
        req.exec_mode = mode;
        req.into_proto().expect("no parameters to convert")
    }

    fn part(stats: Option<QueryStats>) -> ExecuteQueryResponsePart {
        ExecuteQueryResponsePart {
            status: StatusCode::Success as i32,
            issues: vec![],
            result_set_index: 0,
            result_set: None,
            exec_stats: stats,
            tx_meta: None,
        }
    }

    #[test]
    fn exec_mode_defaults_to_execute() {
        let req = RawExecuteQueryRequest::new("s", "SELECT 1", HashMap::new(), None, false);
        assert_eq!(req.exec_mode, RawExecMode::Execute);
        assert_eq!(
            req.into_proto().expect("into_proto").exec_mode,
            ExecMode::Execute as i32
        );
    }

    #[test]
    fn each_mode_maps_to_its_proto_exec_mode() {
        assert_eq!(
            request(RawExecMode::Execute).exec_mode,
            ExecMode::Execute as i32
        );
        assert_eq!(
            request(RawExecMode::Explain).exec_mode,
            ExecMode::Explain as i32
        );
    }

    /// The server reports EXPLAIN statistics on exec mode alone (`NeedReportStats` never consults
    /// `stats_mode` for it), so an explain request asks for nothing extra.
    #[test]
    fn explain_does_not_request_statistics() {
        assert_eq!(
            request(RawExecMode::Explain).stats_mode,
            StatsMode::None as i32
        );
    }

    #[test]
    fn explain_request_is_minimal_and_uses_implicit_session() {
        let proto = request(RawExecMode::Explain);

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
    fn plan_is_read_from_stats() {
        let plan = plan_from_part(&part(Some(QueryStats {
            query_plan: "plan".to_string(),
            query_ast: "ast".to_string(),
            ..Default::default()
        })));
        assert_eq!(
            plan,
            Some(RawQueryStatsPlan {
                query_plan: "plan".to_string(),
                query_ast: "ast".to_string(),
            })
        );
    }

    #[test]
    fn stats_without_plan_or_ast_yield_none() {
        assert_eq!(
            plan_from_part(&part(Some(QueryStats {
                total_duration_us: 42,
                ..Default::default()
            }))),
            None
        );
        assert_eq!(plan_from_part(&part(None)), None);
    }

    /// The server sets plan and AST at different points, so the helper must not require both.
    #[test]
    fn a_plan_without_an_ast_is_still_returned() {
        assert_eq!(
            plan_from_part(&part(Some(QueryStats {
                query_plan: "plan".to_string(),
                total_duration_us: 42,
                ..Default::default()
            }))),
            Some(RawQueryStatsPlan {
                query_plan: "plan".to_string(),
                query_ast: String::new(),
            })
        );
    }
}
