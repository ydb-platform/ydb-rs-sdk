use std::collections::HashMap;
use std::time::Duration;

use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_query_service::status::check_status;
use crate::grpc_wrapper::raw_table_service::value::{
    RawColumn, RawResultSet, RawTypedValue, RawValue,
};
use crate::types::Value;
use ydb_grpc::ydb_proto::query::{
    execute_query_request, ExecMode, ExecuteQueryRequest, ExecuteQueryResponsePart, QueryContent,
    SchemaInclusionMode, StatsMode, Syntax,
};
use ydb_grpc::ydb_proto::result_set::Format;

#[derive(Clone, Debug)]
pub(crate) struct RawExecuteQueryRequest {
    pub session_id: String,
    pub yql_text: String,
    pub parameters: HashMap<String, Value>,
    pub tx_control: Option<ydb_grpc::ydb_proto::query::TransactionControl>,
    pub collect_stats: bool,
}

#[derive(Debug)]
pub(crate) struct RawExecuteQueryResult {
    pub result_sets: Vec<RawResultSet>,
    pub tx_id: Option<String>,
}

#[derive(Debug)]
pub(crate) struct RawExecuteQueryCollectError {
    pub err: crate::grpc_wrapper::raw_errors::RawError,
    pub tx_id: Option<String>,
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
            exec_mode: ExecMode::Execute as i32,
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
            concurrent_result_sets: false,
            response_part_limit_bytes: 0,
            pool_id: String::new(),
            stats_period_ms: 0,
            schema_inclusion_mode: SchemaInclusionMode::Unspecified as i32,
            result_set_format: Format::Unspecified as i32,
            arrow_format_settings: None,
        })
    }
}

pub(crate) fn merge_part(
    sets: &mut HashMap<i64, RawResultSet>,
    part: ExecuteQueryResponsePart,
) -> RawResult<()> {
    let index = part.result_set_index;
    let Some(proto_set) = part.result_set else {
        return Ok(());
    };

    let part_set = RawResultSet::try_from(proto_set)?;
    let entry = sets.entry(index).or_insert_with(|| RawResultSet {
        columns: Vec::new(),
        rows: Vec::new(),
        truncated: false,
    });
    entry.truncated |= part_set.truncated;
    if !entry.columns.is_empty()
        && !part_set.columns.is_empty()
        && !columns_compatible(&entry.columns, &part_set.columns)
    {
        return Err(crate::grpc_wrapper::raw_errors::RawError::custom(format!(
            "result set {index}: column metadata mismatch between stream parts"
        )));
    }
    if entry.columns.is_empty() {
        entry.columns = part_set.columns;
    }
    entry.rows.extend(part_set.rows);
    Ok(())
}

fn columns_compatible(existing: &[RawColumn], new_cols: &[RawColumn]) -> bool {
    existing.len() == new_cols.len()
        && existing
            .iter()
            .zip(new_cols.iter())
            .all(|(left, right)| left.name == right.name && left.column_type == right.column_type)
}

pub(crate) fn sets_to_vec(mut sets: HashMap<i64, RawResultSet>) -> Vec<RawResultSet> {
    let mut keys: Vec<_> = sets.keys().copied().collect();
    keys.sort_unstable();
    keys.into_iter().filter_map(|k| sets.remove(&k)).collect()
}

pub(crate) fn check_part(part: &ExecuteQueryResponsePart) -> RawResult<()> {
    check_status(part.status, &part.issues)
}

pub(crate) fn tx_id_from_part(part: &ExecuteQueryResponsePart) -> Option<String> {
    part.tx_meta
        .as_ref()
        .map(|m| m.id.clone())
        .filter(|id| !id.is_empty())
}

pub(crate) fn stats_from_part(part: &ExecuteQueryResponsePart) -> Option<Duration> {
    part.exec_stats
        .as_ref()
        .map(|stats| Duration::from_micros(stats.total_duration_us))
}

pub(crate) fn append_rows_from_part(
    columns: &mut Vec<RawColumn>,
    rows: &mut Vec<Vec<RawValue>>,
    truncated: &mut bool,
    part: ExecuteQueryResponsePart,
) -> RawResult<()> {
    let Some(proto_set) = part.result_set else {
        return Ok(());
    };
    let part_set = RawResultSet::try_from(proto_set)?;
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
