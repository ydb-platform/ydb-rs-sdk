use crate::types::Value;
use std::collections::HashMap;
use std::str::FromStr;

use crate::YdbError;

/// Query object
#[derive(Clone)]
pub struct Query {
    pub(crate) text: String,
    pub(crate) parameters: HashMap<String, Value>,
    pub(crate) keep_in_cache: bool,
    force_keep_in_cache: bool,
}

impl Query {
    /// Create query with query text
    pub fn new<T: Into<String>>(query: T) -> Self {
        Query {
            text: query.into(),
            parameters: HashMap::new(),
            keep_in_cache: false,
            force_keep_in_cache: false,
        }
    }

    /// Set query parameters
    ///
    /// parameters is data, sent to YDB in binary form
    ///
    /// Example with macros:
    /// ```
    /// # use ydb::{ydb_params, Query};
    /// let query = Query::new("
    ///
    /// SELECT $val AS res
    /// ").with_params(ydb_params!("$val" => 123 as i64));
    /// ```
    ///
    /// Example full:
    /// ```
    /// # use std::collections::HashMap;
    /// # use ydb::{Query, Value};
    /// let mut params: HashMap::<String,Value> = HashMap::new();
    /// params.insert("$val".to_string(), Value::from(123 as i64));
    /// let query = Query::new("
    ///
    /// SELECT $val AS res
    /// ").with_params(params);
    /// ```
    pub fn with_params(mut self, params: HashMap<String, Value>) -> Self {
        self.parameters = params;
        if !self.force_keep_in_cache {
            self.keep_in_cache = !self.parameters.is_empty();
        }
        self
    }

    ///  Set force keep in cache flag for query.
    ///  By default flag is true for query with non empty params and
    ///  false for query without params.
    ///
    ///  Example:
    /// ```
    /// # use ydb::{ydb_params, Query};
    ///
    /// // force use server cache for the query
    /// let q = Query::new("SELECT 1").with_keep_in_cache(true);
    ///
    /// // force disable server cache for the query
    ///  let q = Query::new("SELECT $res AS res")
    /// .with_params(ydb_params!("$val" => 123 as i64))
    /// .with_keep_in_cache(false);
    /// ```
    pub fn with_keep_in_cache(mut self, val: bool) -> Self {
        self.force_keep_in_cache = true;
        self.keep_in_cache = val;
        self
    }

    /// YQL text of the query.
    pub fn yql_text(&self) -> &str {
        &self.text
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

impl FromStr for Query {
    type Err = YdbError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Query::new(s))
    }
}
