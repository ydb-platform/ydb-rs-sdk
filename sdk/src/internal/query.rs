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

pub struct QueryResult {}
