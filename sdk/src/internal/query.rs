use crate::errors::YdbResult;
use crate::types::Value;
use std::collections::HashMap;

use ydb_protobuf::ydb_proto::TypedValue;

pub struct Query {
    text: String,
    parameters: HashMap<String, Value>,
}

impl Query {
    pub fn new<T: Into<String>>(query: T) -> Self {
        Query {
            text: query.into(),
            parameters: HashMap::new(),
        }
    }

    pub fn with_query(mut self: Self, query: String) -> Self {
        self.text = query;
        return self;
    }

    pub(crate) fn with_params(mut self, params: HashMap<String, Value>) -> Self {
        self.parameters = params;
        return self;
    }

    pub(crate) fn query_to_proto(&self) -> ydb_protobuf::ydb_proto::table::Query {
        return ydb_protobuf::ydb_proto::table::Query {
            query: Some(ydb_protobuf::ydb_proto::table::query::Query::YqlText(
                self.text.clone(),
            )),
            ..ydb_protobuf::ydb_proto::table::Query::default()
        };
    }

    pub(crate) fn params_to_proto(self) -> YdbResult<HashMap<String, TypedValue>> {
        let mut params = HashMap::with_capacity(self.parameters.len());

        for (name, val) in self.parameters.into_iter() {
            params.insert(name, val.to_typed_value()?);
        }
        return Ok(params);
    }
}

impl Default for Query {
    fn default() -> Self {
        Query::new("")
    }
}

impl From<&str> for Query {
    fn from(s: &str) -> Self {
        Query::new(s)
    }
}

impl From<String> for Query {
    fn from(s: String) -> Self {
        Query::new(s)
    }
}
