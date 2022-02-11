use crate::errors;
use crate::errors::{YdbError, YdbResult, YdbStatusError};
use crate::internal::grpc::proto_issues_to_ydb_issues;
use crate::types::Value;
use std::collections::HashMap;
use std::rc::Rc;
use std::vec::IntoIter;
use tracing::trace;
use ydb_protobuf::generated::ydb::status_ids::StatusCode;
use ydb_protobuf::generated::ydb::table::{ExecuteQueryResult, ExecuteScanQueryPartialResponse};

#[derive(Debug)]
pub struct QueryResult {
    pub(crate) session_id: Option<String>,
    pub(crate) results: Vec<ResultSet>,
}

impl QueryResult {
    pub(crate) fn from_proto(
        proto_res: ExecuteQueryResult,
        error_on_truncate: bool,
    ) -> errors::YdbResult<Self> {
        trace!("proto_res: {:?}", proto_res);
        let mut results = Vec::with_capacity(proto_res.result_sets.len());
        for current_set in proto_res.result_sets.into_iter() {
            if error_on_truncate && current_set.truncated {
                return Err(
                    format!("got truncated result. result set index: {}", results.len())
                        .as_str()
                        .into(),
                );
            }
            let result_set = ResultSet::from_proto(current_set)?;

            results.push(result_set);
        }

        let session_id = if let Some(meta) = proto_res.tx_meta {
            Some(meta.id)
        } else {
            None
        };

        return Ok(QueryResult {
            session_id,
            results,
        });
    }

    pub fn first(self) -> Option<ResultSet> {
        self.results.into_iter().next()
    }
}

#[derive(Debug)]
pub struct ResultSet {
    columns: Vec<crate::types::Column>,
    columns_by_name: HashMap<String, usize>,
    pb: ydb_protobuf::generated::ydb::ResultSet,
}

impl ResultSet {
    #[allow(dead_code)]
    pub(crate) fn columns(&self) -> &Vec<crate::types::Column> {
        return &self.columns;
    }

    pub fn rows(self) -> ResultSetRowsIter {
        return ResultSetRowsIter {
            columns: Rc::new(self.columns),
            columns_by_name: Rc::new(self.columns_by_name),
            row_iter: self.pb.rows.into_iter(),
        };
    }

    #[allow(dead_code)]
    pub(crate) fn truncated(&self) -> bool {
        self.pb.truncated
    }

    pub(crate) fn from_proto(
        pb: ydb_protobuf::generated::ydb::ResultSet,
    ) -> errors::YdbResult<Self> {
        let mut columns = Vec::with_capacity(pb.columns.len());
        for pb_col in pb.columns.iter() {
            columns.push(crate::types::Column {
                name: pb_col.name.clone(),
                v_type: Value::from_proto_type(&pb_col.r#type)?,
            })
        }
        let columns_by_name = columns
            .iter()
            .enumerate()
            .map(|(k, v)| (v.name.clone(), k))
            .collect();
        Ok(Self {
            columns,
            columns_by_name,
            pb,
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
    columns: Rc<Vec<crate::types::Column>>,
    columns_by_name: Rc<HashMap<String, usize>>,
    pb: HashMap<usize, ydb_protobuf::generated::ydb::Value>,
}

impl Row {
    pub fn remove_field_by_name(&mut self, name: &str) -> errors::YdbResult<Value> {
        if let Some(&index) = self.columns_by_name.get(name) {
            return self.remove_field(index);
        }
        return Err(YdbError::Custom("field not found".into()));
    }

    pub(crate) fn remove_field(&mut self, index: usize) -> errors::YdbResult<Value> {
        match self.pb.remove(&index) {
            Some(val) => Value::from_proto(&self.columns[index].v_type, val),
            None => Err(YdbError::Custom("it has no the field".into())),
        }
    }
}

pub struct ResultSetRowsIter {
    columns: Rc<Vec<crate::types::Column>>,
    columns_by_name: Rc<HashMap<String, usize>>,
    row_iter: IntoIter<ydb_protobuf::generated::ydb::Value>,
}

impl Iterator for ResultSetRowsIter {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        match self.row_iter.next() {
            None => None,
            Some(row) => {
                return Some(Row {
                    columns: self.columns.clone(),
                    columns_by_name: self.columns_by_name.clone(),
                    pb: row.items.into_iter().enumerate().collect(),
                })
            }
        }
    }
}

pub struct StreamResult {
    pub(crate) results: tonic::codec::Streaming<ExecuteScanQueryPartialResponse>,
}

impl StreamResult {
    pub(crate) async fn next(&mut self) -> YdbResult<Option<ResultSet>> {
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
        let result_set = ResultSet::from_proto(proto_result_set)?;
        return Ok(Some(result_set));
    }
}
