use crate::errors;
use crate::errors::{YdbError, YdbResult, YdbStatusError};
use crate::grpc::proto_issues_to_ydb_issues;
use crate::grpc_wrapper::raw_table_service::execute_data_query::{RawQueryStats,RawExecuteDataQueryResult, RawOperationStats, RawQueryPhaseStats, RawTableAccessStats};
use crate::grpc_wrapper::raw_table_service::value::{RawResultSet, RawTypedValue, RawValue};
use crate::trace_helpers::ensure_len_string;
use crate::types::Value;
use itertools::Itertools;
use std::collections::HashMap;
use std::sync::Arc;
use std::vec::IntoIter;
use tracing::trace;
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::table::ExecuteScanQueryPartialResponse;

#[derive(Debug)]
pub struct QueryResult {
    pub(crate) results: Vec<ResultSet>,
    pub(crate) tx_id: String,
    
    pub stats: Option<QueryStats>,
}

#[derive(Debug)]
pub struct QueryStats {
    pub process_cpu_time: std::time::Duration,
    pub total_duration: std::time::Duration,
    pub total_cpu_time: std::time::Duration,
    pub query_plan: String,
    pub query_ast: String,

    pub query_phases: Vec<QueryPhaseStats>,
}

impl QueryResult {
    pub(crate) fn from_raw_result(
        error_on_truncate: bool,
        raw_res: RawExecuteDataQueryResult,
    ) -> YdbResult<Self> {
        trace!(
            "raw_res: {}",
            ensure_len_string(serde_json::to_string(&raw_res)?)
        );
        let mut results = Vec::with_capacity(raw_res.result_sets.len());
        for current_set in raw_res.result_sets.into_iter() {
            if error_on_truncate && current_set.truncated {
                return Err(
                    format!("got truncated result. result set index: {}", results.len())
                        .as_str()
                        .into(),
                );
            }
            let result_set = ResultSet::try_from(current_set)?;

            results.push(result_set);
        }

        Ok(QueryResult {
            results,
            tx_id: raw_res.tx_meta.id,
            stats: raw_res.query_stats.map(QueryStats::from),
        })
    }

    pub fn into_only_result(self) -> YdbResult<ResultSet> {
        let mut iter = self.results.into_iter();
        match iter.next() {
            Some(result_set) => {
                if iter.next().is_none() {
                    Ok(result_set)
                } else {
                    Err(YdbError::from_str("more then one result set"))
                }
            }
            None => Err(YdbError::from_str("no result set")),
        }
    }

    pub fn into_only_row(self) -> YdbResult<Row> {
        let result_set = self.into_only_result()?;
        let mut rows = result_set.rows();
        match rows.next() {
            Some(first_row) => {
                if rows.next().is_none() {
                    Ok(first_row)
                } else {
                    Err(YdbError::from_str("result set has more then one row"))
                }
            }
            None => Err(YdbError::NoRows),
        }
    }
}

#[derive(Debug)]
pub struct ResultSet {
    columns: Vec<crate::types::Column>,
    columns_by_name: HashMap<String, usize>,
    raw_result_set: RawResultSet,
}

impl ResultSet {
    #[allow(dead_code)]
    pub(crate) fn columns(&self) -> &Vec<crate::types::Column> {
        &self.columns
    }

    pub fn rows(self) -> ResultSetRowsIter {
        ResultSetRowsIter {
            columns: Arc::new(self.columns),
            columns_by_name: Arc::new(self.columns_by_name),
            row_iter: self.raw_result_set.rows.into_iter(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn truncated(&self) -> bool {
        self.raw_result_set.truncated
    }
}

impl TryFrom<RawResultSet> for ResultSet {
    type Error = YdbError;

    fn try_from(value: RawResultSet) -> Result<Self, Self::Error> {
        let columns_by_name: HashMap<String, usize> = value
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| (column.name.clone(), index))
            .collect();
        Ok(Self {
            columns: value
                .columns
                .iter()
                .map(|item| item.clone().try_into())
                .try_collect()?,
            columns_by_name,
            raw_result_set: value,
        })
    }
}

impl IntoIterator for ResultSet {
    type Item = Row;
    type IntoIter = ResultSetRowsIter;

    fn into_iter(self) -> Self::IntoIter {
        self.rows()
    }
}

#[derive(Debug)]
pub struct Row {
    columns: Arc<Vec<crate::types::Column>>,
    columns_by_name: Arc<HashMap<String, usize>>,
    raw_values: HashMap<usize, RawValue>,
}

impl Row {
    pub fn remove_field_by_name(&mut self, name: &str) -> errors::YdbResult<Value> {
        if let Some(&index) = self.columns_by_name.get(name) {
            return self.remove_field(index);
        }
        Err(YdbError::Custom("field not found".into()))
    }

    pub fn remove_field(&mut self, index: usize) -> errors::YdbResult<Value> {
        match self.raw_values.remove(&index) {
            Some(val) => Ok(Value::try_from(RawTypedValue {
                r#type: self.columns[index].v_type.clone(),
                value: val,
            })?),
            None => Err(YdbError::Custom("it has no the field".into())),
        }
    }
}

pub struct ResultSetRowsIter {
    columns: Arc<Vec<crate::types::Column>>,
    columns_by_name: Arc<HashMap<String, usize>>,
    row_iter: IntoIter<Vec<RawValue>>,
}

impl Iterator for ResultSetRowsIter {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        match self.row_iter.next() {
            None => None,
            Some(row) => Some(Row {
                columns: self.columns.clone(),
                columns_by_name: self.columns_by_name.clone(),
                raw_values: row.into_iter().enumerate().collect(),
            }),
        }
    }
}

pub struct StreamResult {
    pub(crate) results: tonic::codec::Streaming<ExecuteScanQueryPartialResponse>,
}

impl StreamResult {
    pub async fn next(&mut self) -> YdbResult<Option<ResultSet>> {
        let partial_response = if let Some(partial_response) = self.results.message().await? {
            partial_response
        } else {
            return Ok(None);
        };
        if partial_response.status() != StatusCode::Success {
            return Err(YdbError::YdbStatusError(YdbStatusError {
                message: format!("{:?}", partial_response.issues),
                operation_status: partial_response.status,
                issues: proto_issues_to_ydb_issues(partial_response.issues),
            }));
        };
        let proto_result_set = if let Some(partial_result) = partial_response.result {
            if let Some(proto_result_set) = partial_result.result_set {
                proto_result_set
            } else {
                return Ok(None);
            }
        } else {
            return Err(YdbError::InternalError("unexpected empty result".into()));
        };
        let raw_res = RawResultSet::try_from(proto_result_set)?;
        let result_set = ResultSet::try_from(raw_res)?;
        Ok(Some(result_set))
    }
}

impl From<RawQueryStats> for QueryStats{

    fn from(value: RawQueryStats) -> QueryStats {

        Self {
            process_cpu_time: value.process_cpu_time,
            total_duration: value.total_duration,
            total_cpu_time: value.total_cpu_time,
            query_ast: value.query_ast,
            query_plan: value.query_plan,

            query_phases: value.query_phases.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug)]
pub struct QueryPhaseStats {
    pub duration: std::time::Duration,
    pub table_access: Vec<TableAccessStats>,
    pub cpu_time: std::time::Duration,
    pub affected_shards: u64,
    pub literal_phase: bool,
}

impl From<RawQueryPhaseStats> for QueryPhaseStats {
    fn from(value: RawQueryPhaseStats) -> Self {
        Self {

            duration: value.duration,
            table_access: value.table_access.into_iter().map(Into::into).collect(),
            cpu_time: value.cpu_time,
            affected_shards: value.affected_shards,
            literal_phase: value.literal_phase,
        }
    }
}

#[derive(Debug)]
pub struct TableAccessStats {
    pub name: String,
    pub reads: Option<OperationStats>,
    pub updates: Option<OperationStats>,
    pub deletes: Option<OperationStats>,
    pub partitions_count: u64,
    pub affected_rows: u64
}




impl From<RawTableAccessStats> for TableAccessStats {
    fn from(value: RawTableAccessStats) -> Self {
        fn affected_rows(stats: &Option<OperationStats>) -> u64 {
            stats.as_ref().map(|x|x.rows).unwrap_or(0)
        }

        let reads= value.reads.map(Into::into);
        let updates= value.updates.map(Into::into);
        let deletes= value.deletes.map(Into::into);

        let affected_rows = 
            affected_rows(&reads) + affected_rows(&updates) + affected_rows(&deletes);

        Self {
            name: value.name,
            reads,
            updates,
            deletes,
            partitions_count: value.partitions_count,
            affected_rows
        }
    }
}



impl From<RawOperationStats> for OperationStats {
    fn from(value: RawOperationStats) -> Self {
        Self {
            rows: value.rows,
            bytes: value.bytes,
        }
    }
}

#[derive(Debug)]
pub struct OperationStats {
    pub rows: u64,
    pub bytes: u64,
}