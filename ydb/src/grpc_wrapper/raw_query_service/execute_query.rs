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
    StatsMode, Syntax,
};

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

impl RawExecuteQueryRequest {
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
            schema_inclusion_mode: 0,
            result_set_format: 0,
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
        columns: part_set.columns.clone(),
        rows: Vec::new(),
        truncated: part_set.truncated,
    });
    entry.truncated |= part_set.truncated;
    if entry.columns.is_empty() {
        entry.columns = part_set.columns;
    }
    entry.rows.extend(part_set.rows);
    Ok(())
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
    part: &ExecuteQueryResponsePart,
) -> RawResult<()> {
    let Some(proto_set) = &part.result_set else {
        return Ok(());
    };
    let part_set = RawResultSet::try_from(proto_set.clone())?;
    *truncated |= part_set.truncated;
    if columns.is_empty() {
        *columns = part_set.columns;
    }
    rows.extend(part_set.rows);
    Ok(())
}
