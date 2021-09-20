use crate::errors::{Error, Result};
use crate::types::YdbValue;
use std::slice::Iter;
use ydb_protobuf::generated::ydb::table::ExecuteQueryResult;

pub struct Query {
    text: String,
}

impl Query {
    fn new() -> Self {
        Query { text: "".into() }
    }

    pub fn with_query(mut self: Self, query: String) -> Self {
        self.text = query;
        return self;
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

impl Into<ydb_protobuf::generated::ydb::table::Query> for Query {
    fn into(self) -> ydb_protobuf::generated::ydb::table::Query {
        ydb_protobuf::generated::ydb::table::Query {
            query: Some(ydb_protobuf::generated::ydb::table::query::Query::YqlText(
                self.text,
            )),
            ..ydb_protobuf::generated::ydb::table::Query::default()
        }
    }
}

#[derive(Debug)]
pub struct QueryResult {
    pub(crate) error: Option<Error>,
    pub(crate) results: Vec<ResultSet>,
}

impl QueryResult {
    pub(crate) fn from_proto(
        proto_res: ExecuteQueryResult,
        error_on_truncate: bool,
    ) -> Result<Self> {
        println!("proto_res: {:?}", proto_res);
        let mut res = QueryResult::default();
        res.results.reserve_exact(proto_res.result_sets.len());
        for proto_result_set in proto_res.result_sets {
            let mut result_set = ResultSet::default();
            result_set.truncated = proto_result_set.truncated;
            if error_on_truncate && result_set.truncated {
                return Err(format!(
                    "got truncated result. result set index: {}",
                    res.results.len()
                )
                .as_str()
                .into());
            }

            result_set
                .columns
                .reserve_exact(proto_result_set.columns.len());

            for proto_column in proto_result_set.columns {
                result_set.columns.push(crate::types::Column {
                    name: proto_column.name,
                })
            }

            result_set.rows.reserve_exact(proto_result_set.rows.len());
            for mut proto_row in proto_result_set.rows {
                // for pop and consume items in column order
                proto_row.items.reverse();

                let mut row = Vec::with_capacity(result_set.columns.len());
                for _ in 0..result_set.columns.len() {
                    if let Some(proto_val) = proto_row.items.pop() {
                        let val = YdbValue::from_proto(proto_val)?;
                        println!("ydb val: {:?}", val);
                        row.push(val);
                    } else {
                        return Err(format!(
                            "mismatch items in for with columns count. result set index: {}, row number: {}, need items: {}, has items: {}",
                            res.results.len(),
                            result_set.rows.len(),
                            result_set.columns.len(),
                            row.len(),
                        ).as_str()
                        .into());
                    };
                }
                result_set.rows.push(row);
            }
            res.results.push(result_set);
        }
        return Ok(res);
    }

    pub fn first(&self) -> Option<&ResultSet> {
        self.results.first()
    }

    pub fn result_sets(&self) -> Iter<'_, ResultSet> {
        self.results.iter()
    }
}

impl Default for QueryResult {
    fn default() -> Self {
        return QueryResult {
            error: None,
            results: Vec::new(),
        };
    }
}

#[derive(Debug)]
pub struct ResultSet {
    pub truncated: bool,
    pub(crate) columns: Vec<crate::types::Column>,
    pub(crate) rows: Vec<Vec<crate::types::YdbValue>>,
}

impl ResultSet {
    pub fn columns(&self) -> &Vec<crate::types::Column> {
        return &self.columns;
    }

    pub fn rows(&self) -> ResultSetRowsIter {
        return ResultSetRowsIter {
            columns: &self.columns,
            row_iter: self.rows.iter(),
        };
    }
}

impl Default for ResultSet {
    fn default() -> Self {
        return ResultSet {
            truncated: false,
            columns: Vec::new(),
            rows: Vec::new(),
        };
    }
}

#[derive(Debug)]
pub struct Row<'a> {
    columns: &'a Vec<crate::types::Column>,
    fields: &'a Vec<crate::types::YdbValue>,
}

impl<'a> Row<'a> {
    pub fn get_field(&self, name: &str) -> Option<&YdbValue> {
        for (index, column) in self.columns.iter().enumerate() {
            if column.name == name {
                return self.fields.get(index);
            }
        }

        return None;
    }

    pub fn get_field_index(&self, index: usize) -> Option<&YdbValue> {
        return self.fields.get(index);
    }
}

pub struct ResultSetRowsIter<'a> {
    columns: &'a Vec<crate::types::Column>,
    row_iter: Iter<'a, Vec<YdbValue>>,
}

impl<'a> Iterator for ResultSetRowsIter<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.row_iter.next() {
            None => None,
            Some(row) => {
                return Some(Row {
                    columns: self.columns,
                    fields: row,
                })
            }
        }
    }
}
