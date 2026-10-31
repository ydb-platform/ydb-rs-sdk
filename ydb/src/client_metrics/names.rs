use std::borrow::Cow;
use std::string::ToString;
use std::sync::Arc;

use crate::client_metrics::backend::{
    CounterHandle, GaugeHandle, HistogramHandle, MetricDescription, MetricLabel, MetricUnit,
    MetricsBackend,
};
use crate::client_metrics::dynamic::DynamicCaches;
use crate::client_metrics::endpoints::EndpointRegistry;
use crate::client_metrics::{
    QueryOperation, QueryTransactionResult, SessionPoolAcquireResult,
    SessionPoolAttachStreamCloseReason, SessionPoolCloseReason,
};

const DEFAULT_DRIVER_NAME: &str = "main";

/// Default cap of unique `query_name` values admitted by one recorder; valid
/// names beyond it collapse into the `__other__` bucket. Configurable per
/// client through [`crate::ClientBuilder::with_metrics_max_query_names`].
pub(crate) const DEFAULT_MAX_QUERY_NAMES: usize = 512;

/// Number of `result` label values for `ydb_query_operations_total`; sizes its
/// per-operation handle arrays. Indexed by `usize::from(!ok)`.
pub(crate) const QUERY_RESULT_COUNT: usize = 2;

/// `result` label values for `ydb_query_operations_total`.
pub(crate) const QUERY_RESULT_LABELS: [&str; QUERY_RESULT_COUNT] = ["ok", "error"];

/// Static label prefix shared by every series of one recorder: the driver
/// name label first, extra static labels after (existing label order).
pub(crate) fn base_labels(
    driver_name: Option<String>,
    extra_labels: Vec<(String, String)>,
) -> Vec<MetricLabel> {
    let mut labels = vec![MetricLabel::new(
        "driver_name",
        driver_name.unwrap_or_else(|| DEFAULT_DRIVER_NAME.to_string()),
    )];
    labels.extend(
        extra_labels
            .into_iter()
            .map(|(key, value)| MetricLabel::new(key, value)),
    );
    labels
}

fn counter(
    backend: &dyn MetricsBackend,
    name: impl Into<Cow<'static, str>>,
    unit: MetricUnit,
    description: impl Into<Cow<'static, str>>,
    labels: &[MetricLabel],
) -> CounterHandle {
    backend.register_counter(&MetricDescription::new(name, unit, description), labels)
}

fn gauge(
    backend: &dyn MetricsBackend,
    name: impl Into<Cow<'static, str>>,
    unit: MetricUnit,
    description: impl Into<Cow<'static, str>>,
    labels: &[MetricLabel],
) -> GaugeHandle {
    backend.register_gauge(&MetricDescription::new(name, unit, description), labels)
}

fn histogram(
    backend: &dyn MetricsBackend,
    name: impl Into<Cow<'static, str>>,
    unit: MetricUnit,
    description: impl Into<Cow<'static, str>>,
    labels: &[MetricLabel],
) -> HistogramHandle {
    backend.register_histogram(&MetricDescription::new(name, unit, description), labels)
}

/// The full metric surface of one recorder: pre-registered handles for the
/// existing and session pool series, plus the handle caches for the dynamic gRPC
/// series.
pub(crate) struct MetricsNames {
    pub client_new_counter: CounterHandle,
    pub client_new_table_client_counter: CounterHandle,
    pub client_new_query_client_counter: CounterHandle,
    pub client_new_scheme_client_counter: CounterHandle,
    pub client_new_topic_client_counter: CounterHandle,
    pub client_query_row_counter: CounterHandle,
    pub client_transaction_query_row_counter: CounterHandle,
    pub client_transaction_exec_counter: CounterHandle,
    pub client_transaction_commit_counter: CounterHandle,
    pub client_transaction_rollback_counter: CounterHandle,
    pub client_row_query_time_histogram: HistogramHandle,
    pub client_transaction_row_query_time_histogram: HistogramHandle,
    // Session pool series (closed enum label sets): handles are pre-registered
    // per enum value and indexed by the enum on the hot path, without macros
    // or label construction.
    pub session_pool_sessions_idle: GaugeHandle,
    pub session_pool_sessions_active: GaugeHandle,
    pub session_pool_sessions_creating: GaugeHandle,
    pub session_pool_size_limit: GaugeHandle,
    pub session_pool_pending_requests: GaugeHandle,
    pub session_pool_acquire_total: [CounterHandle; SessionPoolAcquireResult::LABEL_COUNT],
    pub session_pool_acquire_seconds: [HistogramHandle; SessionPoolAcquireResult::LABEL_COUNT],
    pub session_pool_session_create_seconds: HistogramHandle,
    pub session_pool_sessions_created_total: CounterHandle,
    pub session_pool_sessions_closed_total: [CounterHandle; SessionPoolCloseReason::LABEL_COUNT],
    pub session_pool_session_use_seconds: HistogramHandle,
    pub session_pool_attach_stream_closed_total:
        [CounterHandle; SessionPoolAttachStreamCloseReason::LABEL_COUNT],
    // Query service series (closed enum label sets): handles are pre-registered
    // per operation and indexed by the enum on the hot path, without macros or
    // label construction.
    pub query_operations_total: [[CounterHandle; QUERY_RESULT_COUNT]; QueryOperation::LABEL_COUNT],
    pub query_operation_duration_seconds: [HistogramHandle; QueryOperation::LABEL_COUNT],
    pub query_result_rows: [HistogramHandle; QueryOperation::LABEL_COUNT],
    pub query_result_bytes: [HistogramHandle; QueryOperation::LABEL_COUNT],
    pub query_transactions_total: [CounterHandle; QueryTransactionResult::LABEL_COUNT],
    pub query_transaction_duration_seconds: HistogramHandle,
    /// Registry of admitted query names for the named query series
    /// (`ydb_query_named_*`), with the unique-name cap applied.
    pub query_names: super::query_names::QueryNameRegistry,
    /// Endpoint label policy and bounded set of admitted endpoint values.
    pub endpoints: EndpointRegistry,
    /// Handle caches for the dynamic series (runtime string labels).
    pub dynamic: DynamicCaches,
    /// Static label prefix (driver name + extra labels), prepended to dynamic
    /// series labels when their handles are first registered.
    base_labels: Vec<MetricLabel>,
    backend: Arc<dyn MetricsBackend>,
}

impl MetricsNames {
    pub fn new(base: Vec<MetricLabel>, backend: Arc<dyn MetricsBackend>) -> Self {
        Self::from_base(&base, backend)
    }

    fn from_base(base: &[MetricLabel], backend: Arc<dyn MetricsBackend>) -> Self {
        let labeled = |name: &'static str, value: &'static str| {
            let mut labels = base.to_vec();
            labels.push(MetricLabel::new(name, value));
            labels
        };
        let labeled2 = |name1: &'static str,
                        value1: &'static str,
                        name2: &'static str,
                        value2: &'static str| {
            let mut labels = base.to_vec();
            labels.push(MetricLabel::new(name1, value1));
            labels.push(MetricLabel::new(name2, value2));
            labels
        };
        let backend_ref = backend.as_ref();
        Self {
            client_new_counter: counter(
                backend_ref,
                "ydb_new_client_counter",
                MetricUnit::Count,
                "ydb new client counter",
                base,
            ),
            client_new_table_client_counter: counter(
                backend_ref,
                "ydb_new_table_client_counter",
                MetricUnit::Count,
                "ydb new table client counter",
                base,
            ),
            client_new_query_client_counter: counter(
                backend_ref,
                "ydb_new_query_client_counter",
                MetricUnit::Count,
                "ydb new query client counter",
                base,
            ),
            client_new_scheme_client_counter: counter(
                backend_ref,
                "ydb_new_scheme_client_counter",
                MetricUnit::Count,
                "ydb new scheme client counter",
                base,
            ),
            client_new_topic_client_counter: counter(
                backend_ref,
                "ydb_new_topic_client_counter",
                MetricUnit::Count,
                "ydb new topic client counter",
                base,
            ),
            client_query_row_counter: counter(
                backend_ref,
                "ydb_client_query_row_counter",
                MetricUnit::Count,
                "ydb client query row counter",
                base,
            ),
            client_transaction_query_row_counter: counter(
                backend_ref,
                "ydb_client_transaction_query_row_counter",
                MetricUnit::Count,
                "ydb client transaction query row counter",
                base,
            ),
            client_transaction_exec_counter: counter(
                backend_ref,
                "ydb_client_transaction_exec_counter",
                MetricUnit::Count,
                "ydb client transaction exec counter",
                base,
            ),
            client_transaction_commit_counter: counter(
                backend_ref,
                "ydb_client_transaction_commit_counter",
                MetricUnit::Count,
                "ydb client transaction commit counter",
                base,
            ),
            client_transaction_rollback_counter: counter(
                backend_ref,
                "ydb_client_transaction_rollback_counter",
                MetricUnit::Count,
                "ydb client transaction rollback counter",
                base,
            ),
            client_row_query_time_histogram: histogram(
                backend_ref,
                "ydb_row_query_time_histogram",
                MetricUnit::Seconds,
                "ydb row query time histogram",
                base,
            ),
            client_transaction_row_query_time_histogram: histogram(
                backend_ref,
                "ydb_transaction_row_query_time_histogram",
                MetricUnit::Seconds,
                "ydb transaction row query time histogram",
                base,
            ),
            session_pool_sessions_idle: gauge(
                backend_ref,
                "ydb_session_pool_sessions",
                MetricUnit::Count,
                "ydb session pool sessions in idle state",
                &labeled("state", "idle"),
            ),
            session_pool_sessions_active: gauge(
                backend_ref,
                "ydb_session_pool_sessions",
                MetricUnit::Count,
                "ydb session pool sessions leased to callers",
                &labeled("state", "active"),
            ),
            session_pool_sessions_creating: gauge(
                backend_ref,
                "ydb_session_pool_sessions",
                MetricUnit::Count,
                "ydb session pool CreateSession RPCs in flight",
                &labeled("state", "creating"),
            ),
            session_pool_size_limit: gauge(
                backend_ref,
                "ydb_session_pool_size_limit",
                MetricUnit::Count,
                "ydb session pool size limit",
                base,
            ),
            session_pool_pending_requests: gauge(
                backend_ref,
                "ydb_session_pool_pending_requests",
                MetricUnit::Count,
                "ydb session pool callers waiting for a session",
                base,
            ),
            session_pool_acquire_total: std::array::from_fn(|i| {
                counter(
                    backend_ref,
                    "ydb_session_pool_acquire_total",
                    MetricUnit::Count,
                    "ydb session pool acquire attempts by result",
                    &labeled("result", SessionPoolAcquireResult::LABELS[i]),
                )
            }),
            session_pool_acquire_seconds: std::array::from_fn(|i| {
                histogram(
                    backend_ref,
                    "ydb_session_pool_acquire_seconds",
                    MetricUnit::Seconds,
                    "ydb session pool acquire duration by result",
                    &labeled("result", SessionPoolAcquireResult::LABELS[i]),
                )
            }),
            session_pool_session_create_seconds: histogram(
                backend_ref,
                "ydb_session_pool_session_create_seconds",
                MetricUnit::Seconds,
                "ydb session pool session creation duration",
                base,
            ),
            session_pool_sessions_created_total: counter(
                backend_ref,
                "ydb_session_pool_sessions_created_total",
                MetricUnit::Count,
                "ydb session pool sessions created",
                base,
            ),
            session_pool_sessions_closed_total: std::array::from_fn(|i| {
                counter(
                    backend_ref,
                    "ydb_session_pool_sessions_closed_total",
                    MetricUnit::Count,
                    "ydb session pool sessions closed by reason",
                    &labeled("reason", SessionPoolCloseReason::LABELS[i]),
                )
            }),
            session_pool_session_use_seconds: histogram(
                backend_ref,
                "ydb_session_pool_session_use_seconds",
                MetricUnit::Seconds,
                "ydb session pool session lease duration",
                base,
            ),
            session_pool_attach_stream_closed_total: std::array::from_fn(|i| {
                counter(
                    backend_ref,
                    "ydb_session_pool_attach_stream_closed_total",
                    MetricUnit::Count,
                    "ydb session pool AttachSession stream closures by reason",
                    &labeled("reason", SessionPoolAttachStreamCloseReason::LABELS[i]),
                )
            }),
            query_operations_total: std::array::from_fn(|op| {
                std::array::from_fn(|result| {
                    counter(
                        backend_ref,
                        "ydb_query_operations_total",
                        MetricUnit::Count,
                        "ydb query operations by operation and result",
                        &labeled2(
                            "operation",
                            QueryOperation::LABELS[op],
                            "result",
                            QUERY_RESULT_LABELS[result],
                        ),
                    )
                })
            }),
            query_operation_duration_seconds: std::array::from_fn(|op| {
                histogram(
                    backend_ref,
                    "ydb_query_operation_duration_seconds",
                    MetricUnit::Seconds,
                    "ydb query operation duration",
                    &labeled("operation", QueryOperation::LABELS[op]),
                )
            }),
            query_result_rows: std::array::from_fn(|op| {
                histogram(
                    backend_ref,
                    "ydb_query_result_rows",
                    MetricUnit::Count,
                    "ydb query result rows",
                    &labeled("operation", QueryOperation::LABELS[op]),
                )
            }),
            query_result_bytes: std::array::from_fn(|op| {
                histogram(
                    backend_ref,
                    "ydb_query_result_bytes",
                    MetricUnit::Bytes,
                    "ydb query approximate result bytes",
                    &labeled("operation", QueryOperation::LABELS[op]),
                )
            }),
            query_transactions_total: std::array::from_fn(|result| {
                counter(
                    backend_ref,
                    "ydb_query_transactions_total",
                    MetricUnit::Count,
                    "ydb retried transactions by result",
                    &labeled("result", QueryTransactionResult::LABELS[result]),
                )
            }),
            query_transaction_duration_seconds: histogram(
                backend_ref,
                "ydb_query_transaction_duration_seconds",
                MetricUnit::Seconds,
                "ydb retried transaction duration including all attempts",
                base,
            ),
            query_names: super::query_names::QueryNameRegistry::default(),
            endpoints: EndpointRegistry::default(),
            dynamic: DynamicCaches::default(),
            base_labels: base.to_vec(),
            backend,
        }
    }

    /// Full label set for a first dynamic-series registration: the recorder's
    /// static labels first, then the series' dynamic labels.
    pub(crate) fn dynamic_labels(&self, dynamic: Vec<MetricLabel>) -> Vec<MetricLabel> {
        let mut labels = self.base_labels.clone();
        labels.extend(dynamic);
        labels
    }

    pub(crate) fn counter(
        &self,
        name: impl Into<Cow<'static, str>>,
        unit: MetricUnit,
        description: impl Into<Cow<'static, str>>,
        labels: &[MetricLabel],
    ) -> CounterHandle {
        counter(self.backend.as_ref(), name, unit, description, labels)
    }

    pub(crate) fn gauge(
        &self,
        name: impl Into<Cow<'static, str>>,
        unit: MetricUnit,
        description: impl Into<Cow<'static, str>>,
        labels: &[MetricLabel],
    ) -> GaugeHandle {
        gauge(self.backend.as_ref(), name, unit, description, labels)
    }

    pub(crate) fn histogram(
        &self,
        name: impl Into<Cow<'static, str>>,
        unit: MetricUnit,
        description: impl Into<Cow<'static, str>>,
        labels: &[MetricLabel],
    ) -> HistogramHandle {
        histogram(self.backend.as_ref(), name, unit, description, labels)
    }
}
