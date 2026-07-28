use std::string::ToString;

use metrics::{Counter, Histogram, counter, histogram};

const DEFAULT_DRIVER_NAME: &str = "main";

#[derive(Clone, Debug)]
pub(crate) struct MetricsNames {
    pub client_new_counter: Counter,
    pub client_new_table_client_counter: Counter,
    pub client_new_query_client_counter: Counter,
    pub client_new_scheme_client_counter: Counter,
    pub client_new_topic_client_counter: Counter,
    pub client_query_row_counter: Counter,
    pub client_transaction_query_row_counter: Counter,
    pub client_transaction_exec_counter: Counter,
    pub client_transaction_commit_counter: Counter,
    pub client_transaction_rollback_counter: Counter,
    pub client_row_query_time_histogram: Histogram,
    pub client_transaction_row_query_time_histogram: Histogram,
}

impl Default for MetricsNames {
    fn default() -> Self {
        MetricsNames::new(None)
    }
}

impl MetricsNames {
    pub fn new(driver_name: Option<String>) -> Self {
        let labels = [(
            "driver_name",
            driver_name.unwrap_or(DEFAULT_DRIVER_NAME.to_string()),
        )];
        Self {
            client_new_counter: counter!(description: "ydb new client counter", "ydb_new_client_counter", &labels),
            client_new_table_client_counter: counter!(description: "ydb new table client counter", "ydb_new_table_client_counter", &labels),
            client_new_query_client_counter: counter!(description: "ydb new query client counter", "ydb_new_query_client_counter", &labels),
            client_new_scheme_client_counter: counter!(description: "ydb new scheme client counter", "ydb_new_scheme_client_counter", &labels),
            client_new_topic_client_counter: counter!(description: "ydb new topic client counter", "ydb_new_topic_client_counter", &labels),
            client_query_row_counter: counter!(description: "ydb client query row counter", "ydb_client_query_row_counter", &labels),
            client_transaction_query_row_counter: counter!(description: "ydb client transaction query row counter", "ydb_client_transaction_query_row_counter", &labels),
            client_transaction_exec_counter: counter!(description: "ydb client transaction exec counter", "ydb_client_transaction_exec_counter", &labels),
            client_transaction_commit_counter: counter!(description: "ydb client transaction commit counter", "ydb_client_transaction_commit_counter", &labels),
            client_transaction_rollback_counter: counter!(description: "ydb client transaction rollback counter", "ydb_client_transaction_rollback_counter", &labels),
            client_row_query_time_histogram: histogram!(description: "ydb row query time histogram", "ydb_row_query_time_histogram", &labels),
            client_transaction_row_query_time_histogram: histogram!(description: "ydb transaction row query time histogram", "ydb_transaction_row_query_time_histogram", &labels),
        }
    }
}
