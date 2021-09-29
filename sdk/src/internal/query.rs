use crate::errors::{Error, Result};
use crate::types::YdbValue;
use std::collections::HashMap;
use std::slice::Iter;
use std::sync::Arc;
use ydb_protobuf::generated::ydb::table::{ExecuteDataQueryRequest, ExecuteQueryResult};

pub struct Query {
    text: String,
    parameters: HashMap<String, YdbValue>,
}

impl Query {
    pub fn new() -> Self {
        Query {
            text: "".into(),
            parameters: HashMap::new(),
        }
    }

    pub fn with_query(mut self: Self, query: String) -> Self {
        self.text = query;
        return self;
    }

    pub fn with_params(mut self, params: HashMap<String, YdbValue>) -> Self {
        self.parameters = params;
        return self;
    }

    pub(crate) fn to_proto(self) -> ExecuteDataQueryRequest {
        // query
        let query = ydb_protobuf::generated::ydb::table::Query {
            query: Some(ydb_protobuf::generated::ydb::table::query::Query::YqlText(
                self.text,
            )),
            ..ydb_protobuf::generated::ydb::table::Query::default()
        };

        let mut params = HashMap::with_capacity(self.parameters.len());

        for (name, val) in self.parameters.into_iter() {
            params.insert(name, val.to_typed_value());
        }

        return ExecuteDataQueryRequest {
            query: Some(query),
            parameters: params,
            ..ExecuteDataQueryRequest::default()
        };
    }
}

impl Default for Query {
    fn default() -> Self {
        Query::new()
    }
}

impl From<&str> for Query {
    fn from(s: &str) -> Self {
        Query::new().with_query(s.to_string())
    }
}

impl From<String> for Query {
    fn from(s: String) -> Self {
        Query::new().with_query(s)
    }
}

#[derive(Debug)]
pub struct QueryResult {
    pub(crate) results: Vec<ResultSet>,
}

impl QueryResult {
    pub(crate) fn from_proto(
        proto_res: ExecuteQueryResult,
        error_on_truncate: bool,
    ) -> Result<Self> {
        println!("proto_res: {:?}", proto_res);
        let mut res = QueryResult {
            results: Vec::new(),
        };
        res.results.reserve_exact(proto_res.result_sets.len());
        for current_set in proto_res.result_sets.into_iter() {
            if error_on_truncate && current_set.truncated {
                return Err(format!(
                    "got truncated result. result set index: {}",
                    res.results.len()
                )
                .as_str()
                .into());
            }
            let mut result_set = ResultSet::from_proto(current_set)?;

            res.results.push(result_set);
        }
        return Ok(res);
    }

    pub fn first(&self) -> Option<&ResultSet> {
        self.results.first()
    }

    #[allow(dead_code)]
    pub fn result_sets(&self) -> Iter<'_, ResultSet> {
        self.results.iter()
    }
}

#[derive(Debug)]
pub struct ResultSet {
    columns: Vec<crate::types::Column>,
    pb: ydb_protobuf::generated::ydb::ResultSet,
}

impl ResultSet {
    #[allow(dead_code)]
    pub fn columns(&self) -> &Vec<crate::types::Column> {
        return &self.columns;
    }

    pub fn rows(&self) -> ResultSetRowsIter {
        return ResultSetRowsIter {
            columns: &self.columns,
            row_iter: self.pb.rows.iter(),
        };
    }

    pub fn truncated(&self) -> bool {
        self.pb.truncated
    }

    pub(crate) fn from_proto(pb: ydb_protobuf::generated::ydb::ResultSet) -> Result<Self> {
        let mut columns = Vec::with_capacity(pb.columns.len());
        for pb_col in pb.columns.iter() {
            columns.push(crate::types::Column {
                name: pb_col.name.clone(),
                v_type: YdbValue::from_proto_type(&pb_col.r#type)?,
            })
        }
        Ok(Self { columns, pb })
    }
}

#[derive(Debug)]
pub struct Row<'a> {
    columns: &'a Vec<crate::types::Column>,
    pb: &'a Vec<ydb_protobuf::generated::ydb::Value>,
}

impl<'a> Row<'a> {
    pub fn get_field(&self, name: &str) -> Result<YdbValue> {
        for (index, column) in self.columns.iter().enumerate() {
            if column.name == name {
                return self.get_field_index(index);
            }
        }

        return Err(Error::Custom("field not found".into()));
    }

    pub fn get_field_index(&self, index: usize) -> Result<YdbValue> {
        YdbValue::from_proto(&self.columns[index].v_type, self.pb[index].clone())
    }
}

pub struct ResultSetRowsIter<'a> {
    columns: &'a Vec<crate::types::Column>,
    row_iter: Iter<'a, ydb_protobuf::generated::ydb::Value>,
}

impl<'a> Iterator for ResultSetRowsIter<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.row_iter.next() {
            None => None,
            Some(row) => {
                return Some(Row {
                    columns: self.columns,
                    pb: &row.items,
                })
            }
        }
    }
}
