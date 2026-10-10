use std::string::ToString;

use metrics::{Counter, Gauge, Histogram, counter, gauge, histogram};

use crate::client_metrics::dynamic::DynamicCaches;
use crate::client_metrics::{
    QueryOperation, QueryTransactionResult, SessionPoolAcquireResult, SessionPoolCloseReason,
};

const DEFAULT_DRIVER_NAME: &str = "main";

/// Number of `result` label values for `ydb_query_operations_total`; sizes its
/// per-operation handle arrays. Indexed by `usize::from(!ok)`.
pub(crate) const QUERY_RESULT_COUNT: usize = 2;

/// `result` label values for `ydb_query_operations_total`.
pub(crate) const QUERY_RESULT_LABELS: [&str; QUERY_RESULT_COUNT] = ["ok", "error"];

/// Number of keepalive result label values; sizes the pre-registered
/// `ydb_session_pool_keepalive_total` handle array.
pub(crate) const KEEPALIVE_RESULT_COUNT: usize = 2;

/// Result label values for `ydb_session_pool_keepalive_total` (no enum: the
/// pool reports a plain `bool`). Indexed by `usize::from(!ok)`, matching
/// [`crate::client_metrics::MetricsRecorder::session_pool_keepalive`].
pub(crate) const KEEPALIVE_RESULT_LABELS: [&str; KEEPALIVE_RESULT_COUNT] = ["ok", "error"];

/// Static label prefix shared by every series of one recorder: the driver
/// name label first, extra static labels after (part1 label order).
pub(crate) fn base_labels(
    driver_name: Option<String>,
    extra_labels: Vec<(String, String)>,
) -> Vec<metrics::Label> {
    let mut labels = vec![metrics::Label::new(
        "driver_name",
        driver_name.unwrap_or_else(|| DEFAULT_DRIVER_NAME.to_string()),
    )];
    labels.extend(
        extra_labels
            .into_iter()
            .map(|(key, value)| metrics::Label::new(key, value)),
    );
    labels
}

/// The full metric surface of one recorder: pre-registered handles for the
/// part1 and session pool series, plus the handle caches for the dynamic gRPC
/// series.
#[derive(Debug)]
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
    // Session pool series (closed enum label sets): handles are pre-registered
    // per enum value and indexed by the enum on the hot path, without macros
    // or label construction.
    pub session_pool_sessions_idle: Gauge,
    pub session_pool_sessions_active: Gauge,
    pub session_pool_sessions_creating: Gauge,
    pub session_pool_size_limit: Gauge,
    pub session_pool_pending_requests: Gauge,
    pub session_pool_acquire_total: [Counter; SessionPoolAcquireResult::LABEL_COUNT],
    pub session_pool_acquire_milliseconds: [Histogram; SessionPoolAcquireResult::LABEL_COUNT],
    pub session_pool_session_create_milliseconds: Histogram,
    pub session_pool_sessions_created_total: Counter,
    pub session_pool_sessions_closed_total: [Counter; SessionPoolCloseReason::LABEL_COUNT],
    pub session_pool_session_use_milliseconds: Histogram,
    pub session_pool_keepalive_total: [Counter; KEEPALIVE_RESULT_COUNT],
    // Query service series (closed enum label sets): handles are pre-registered
    // per operation and indexed by the enum on the hot path, without macros or
    // label construction.
    pub query_operations_total: [[Counter; QUERY_RESULT_COUNT]; QueryOperation::LABEL_COUNT],
    pub query_operation_duration_milliseconds: [Histogram; QueryOperation::LABEL_COUNT],
    pub query_result_rows: [Histogram; QueryOperation::LABEL_COUNT],
    pub query_result_bytes: [Histogram; QueryOperation::LABEL_COUNT],
    pub query_transactions_total: [Counter; QueryTransactionResult::LABEL_COUNT],
    pub query_transaction_duration_milliseconds: Histogram,
    /// Handle caches for the dynamic series (runtime string labels).
    pub dynamic: DynamicCaches,
    /// Static label prefix (driver name + extra labels), prepended to dynamic
    /// series labels when their handles are first registered.
    base_labels: Vec<metrics::Label>,
}

impl Default for MetricsNames {
    fn default() -> Self {
        MetricsNames::new(base_labels(None, Vec::new()), None)
    }
}

impl MetricsNames {
    pub fn new(
        base: Vec<metrics::Label>,
        recorder: Option<&(dyn metrics::Recorder + Send + Sync)>,
    ) -> Self {
        let build = || Self::from_base(&base);
        match recorder {
            Some(recorder) => metrics::with_local_recorder(recorder, build),
            None => build(),
        }
    }

    fn from_base(base: &[metrics::Label]) -> Self {
        let labeled = |name: &'static str, value: &'static str| {
            let mut labels = base.to_vec();
            labels.push(metrics::Label::new(name, value));
            labels
        };
        let labeled2 = |name1: &'static str,
                        value1: &'static str,
                        name2: &'static str,
                        value2: &'static str| {
            let mut labels = base.to_vec();
            labels.push(metrics::Label::new(name1, value1));
            labels.push(metrics::Label::new(name2, value2));
            labels
        };
        Self {
            client_new_counter: counter!(description: "ydb new client counter", "ydb_new_client_counter", base.iter()),
            client_new_table_client_counter: counter!(description: "ydb new table client counter", "ydb_new_table_client_counter", base.iter()),
            client_new_query_client_counter: counter!(description: "ydb new query client counter", "ydb_new_query_client_counter", base.iter()),
            client_new_scheme_client_counter: counter!(description: "ydb new scheme client counter", "ydb_new_scheme_client_counter", base.iter()),
            client_new_topic_client_counter: counter!(description: "ydb new topic client counter", "ydb_new_topic_client_counter", base.iter()),
            client_query_row_counter: counter!(description: "ydb client query row counter", "ydb_client_query_row_counter", base.iter()),
            client_transaction_query_row_counter: counter!(description: "ydb client transaction query row counter", "ydb_client_transaction_query_row_counter", base.iter()),
            client_transaction_exec_counter: counter!(description: "ydb client transaction exec counter", "ydb_client_transaction_exec_counter", base.iter()),
            client_transaction_commit_counter: counter!(description: "ydb client transaction commit counter", "ydb_client_transaction_commit_counter", base.iter()),
            client_transaction_rollback_counter: counter!(description: "ydb client transaction rollback counter", "ydb_client_transaction_rollback_counter", base.iter()),
            client_row_query_time_histogram: histogram!(description: "ydb row query time histogram", "ydb_row_query_time_histogram", base.iter()),
            client_transaction_row_query_time_histogram: histogram!(description: "ydb transaction row query time histogram", "ydb_transaction_row_query_time_histogram", base.iter()),
            session_pool_sessions_idle: gauge!(description: "ydb session pool sessions in idle state", "ydb_session_pool_sessions", labeled("state", "idle")),
            session_pool_sessions_active: gauge!(description: "ydb session pool sessions leased to callers", "ydb_session_pool_sessions", labeled("state", "active")),
            session_pool_sessions_creating: gauge!(description: "ydb session pool CreateSession RPCs in flight", "ydb_session_pool_sessions", labeled("state", "creating")),
            session_pool_size_limit: gauge!(description: "ydb session pool size limit", "ydb_session_pool_size_limit", base.iter()),
            session_pool_pending_requests: gauge!(description: "ydb session pool callers waiting for a session", "ydb_session_pool_pending_requests", base.iter()),
            session_pool_acquire_total: std::array::from_fn(
                |i| counter!(description: "ydb session pool acquire attempts by result", "ydb_session_pool_acquire_total", labeled("result", SessionPoolAcquireResult::LABELS[i])),
            ),
            session_pool_acquire_milliseconds: std::array::from_fn(
                |i| histogram!(description: "ydb session pool acquire duration by result", "ydb_session_pool_acquire_milliseconds", labeled("result", SessionPoolAcquireResult::LABELS[i])),
            ),
            session_pool_session_create_milliseconds: histogram!(description: "ydb session pool session creation duration", "ydb_session_pool_session_create_milliseconds", base.iter()),
            session_pool_sessions_created_total: counter!(description: "ydb session pool sessions created", "ydb_session_pool_sessions_created_total", base.iter()),
            session_pool_sessions_closed_total: std::array::from_fn(
                |i| counter!(description: "ydb session pool sessions closed by reason", "ydb_session_pool_sessions_closed_total", labeled("reason", SessionPoolCloseReason::LABELS[i])),
            ),
            session_pool_session_use_milliseconds: histogram!(description: "ydb session pool session lease duration", "ydb_session_pool_session_use_milliseconds", base.iter()),
            session_pool_keepalive_total: std::array::from_fn(
                |i| counter!(description: "ydb session pool keepalive checks by result", "ydb_session_pool_keepalive_total", labeled("result", KEEPALIVE_RESULT_LABELS[i])),
            ),
            query_operations_total: std::array::from_fn(|op| {
                std::array::from_fn(
                    |result| counter!(description: "ydb query operations by operation and result", "ydb_query_operations_total", labeled2("operation", QueryOperation::LABELS[op], "result", QUERY_RESULT_LABELS[result])),
                )
            }),
            query_operation_duration_milliseconds: std::array::from_fn(
                |op| histogram!(description: "ydb query operation duration", "ydb_query_operation_duration_milliseconds", labeled("operation", QueryOperation::LABELS[op])),
            ),
            query_result_rows: std::array::from_fn(
                |op| histogram!(description: "ydb query result rows", "ydb_query_result_rows", labeled("operation", QueryOperation::LABELS[op])),
            ),
            query_result_bytes: std::array::from_fn(
                |op| histogram!(description: "ydb query result bytes", "ydb_query_result_bytes", labeled("operation", QueryOperation::LABELS[op])),
            ),
            query_transactions_total: std::array::from_fn(
                |result| counter!(description: "ydb retried transactions by result", "ydb_query_transactions_total", labeled("result", QueryTransactionResult::LABELS[result])),
            ),
            query_transaction_duration_milliseconds: histogram!(description: "ydb retried transaction duration including all attempts", "ydb_query_transaction_duration_milliseconds", base.iter()),
            dynamic: DynamicCaches::default(),
            base_labels: base.to_vec(),
        }
    }

    /// Full label set for a first dynamic-series registration: the recorder's
    /// static labels first, then the series' dynamic labels.
    pub(crate) fn dynamic_labels(&self, dynamic: Vec<metrics::Label>) -> Vec<metrics::Label> {
        let mut labels = self.base_labels.clone();
        labels.extend(dynamic);
        labels
    }
}
