use std::sync::Arc;
use std::time::Duration;

use ydb_grpc::ydb_proto::status_ids::StatusCode;

use crate::errors::{YdbError, YdbOrCustomerError};

pub(crate) mod backend;
pub(crate) mod dynamic;
pub(crate) mod endpoints;
pub(crate) mod interning;
#[cfg(feature = "metrics")]
pub(crate) mod metrics_facade;
pub(crate) mod names;
pub(crate) mod query_names;
#[cfg(test)]
pub(crate) mod test_recorder;

pub use backend::{
    Counter, Gauge, Histogram, MetricDescription, MetricLabel, MetricUnit, MetricsBackend,
};
pub use endpoints::MetricsDetail;

use backend::{CounterHandle, HistogramHandle};
use dynamic::GrpcCodeKey;
use endpoints::EndpointKey;
use interning::{intern, resolve};
use query_names::QueryNameRegistry;

/// Connection state label for the `ydb_grpc_connections` gauge.
///
/// The `idle` state from the metrics spec is not emitted: pool channels are created
/// with `connect_lazy`, so an idle live-socket state cannot be observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GrpcConnectionState {
    Connecting,
    Active,
    Failed,
}

impl GrpcConnectionState {
    /// Number of label values.
    pub(crate) const LABEL_COUNT: usize = 3;

    /// Label values in [`Self::index`] order.
    pub(crate) const LABELS: [&'static str; Self::LABEL_COUNT] = ["connecting", "active", "failed"];

    /// Index of the value in [`MetricsNames`](names::MetricsNames) handle
    /// arrays and in dynamic cache keys.
    pub(crate) fn index(self) -> usize {
        match self {
            Self::Connecting => 0,
            Self::Active => 1,
            Self::Failed => 2,
        }
    }

    pub(crate) fn as_label(self) -> &'static str {
        Self::LABELS[self.index()]
    }
}

/// Direction of a gRPC stream message (`ydb_grpc_stream_messages_total`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamDirection {
    Sent,
    Received,
}

impl StreamDirection {
    /// Number of label values.
    pub(crate) const LABEL_COUNT: usize = 2;

    /// Label values in [`Self::index`] order.
    pub(crate) const LABELS: [&'static str; Self::LABEL_COUNT] = ["sent", "received"];

    /// Index of the value in dynamic cache keys.
    pub(crate) fn index(self) -> usize {
        match self {
            Self::Sent => 0,
            Self::Received => 1,
        }
    }

    pub(crate) fn as_label(self) -> &'static str {
        Self::LABELS[self.index()]
    }
}

/// Acquire outcome for `ydb_session_pool_acquire_*` metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SessionPoolAcquireResult {
    Ok,
    Timeout,
    Error,
}

impl SessionPoolAcquireResult {
    /// Number of label values; sizes the pre-registered
    /// `ydb_session_pool_acquire_*` handle arrays in
    /// [`MetricsNames`](names::MetricsNames).
    pub(crate) const LABEL_COUNT: usize = 3;

    /// Label values in [`Self::index`] order.
    pub(crate) const LABELS: [&'static str; Self::LABEL_COUNT] = ["ok", "timeout", "error"];

    pub(crate) fn index(self) -> usize {
        match self {
            Self::Ok => 0,
            Self::Timeout => 1,
            Self::Error => 2,
        }
    }

    /// Label of the value; used by the test recorder (production code indexes
    /// the pre-registered handle arrays via [`Self::index`]).
    #[cfg(test)]
    pub(crate) fn as_label(self) -> &'static str {
        Self::LABELS[self.index()]
    }
}

/// Why a session was closed (`ydb_session_pool_sessions_closed_total`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SessionPoolCloseReason {
    IdleTtl,
    UsageLimit,
    BadSession,
    Shutdown,
    KeepaliveFailed,
}

impl SessionPoolCloseReason {
    /// Number of label values; sizes the pre-registered
    /// `ydb_session_pool_sessions_closed_total` handle array in
    /// [`MetricsNames`](names::MetricsNames).
    pub(crate) const LABEL_COUNT: usize = 5;

    /// Label values in [`Self::index`] order.
    pub(crate) const LABELS: [&'static str; Self::LABEL_COUNT] = [
        "idle_ttl",
        "usage_limit",
        "bad_session",
        "shutdown",
        "keepalive_failed",
    ];

    pub(crate) fn index(self) -> usize {
        match self {
            Self::IdleTtl => 0,
            Self::UsageLimit => 1,
            Self::BadSession => 2,
            Self::Shutdown => 3,
            Self::KeepaliveFailed => 4,
        }
    }

    /// Label of the value; used by the test recorder (production code indexes
    /// the pre-registered handle arrays via [`Self::index`]).
    #[cfg(test)]
    pub(crate) fn as_label(self) -> &'static str {
        Self::LABELS[self.index()]
    }
}

/// Operation label for the `ydb_query_*` metric series.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueryOperation {
    Exec,
    QueryRow,
    QueryResultSet,
    Query,
    ExecuteScript,
    FetchScriptResults,
}

impl QueryOperation {
    /// Number of label values; sizes the pre-registered `ydb_query_*` handle
    /// arrays in [`MetricsNames`](names::MetricsNames).
    pub(crate) const LABEL_COUNT: usize = 6;

    /// Label values in [`Self::index`] order.
    pub(crate) const LABELS: [&'static str; Self::LABEL_COUNT] = [
        "exec",
        "query_row",
        "query_result_set",
        "query",
        "execute_script",
        "fetch_script_results",
    ];

    pub(crate) fn index(self) -> usize {
        match self {
            Self::Exec => 0,
            Self::QueryRow => 1,
            Self::QueryResultSet => 2,
            Self::Query => 3,
            Self::ExecuteScript => 4,
            Self::FetchScriptResults => 5,
        }
    }
}

/// Outcome label for `ydb_query_transactions_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueryTransactionResult {
    Commit,
    Rollback,
    Error,
}

impl QueryTransactionResult {
    /// Number of label values; sizes the pre-registered
    /// `ydb_query_transactions_total` handle array in
    /// [`MetricsNames`](names::MetricsNames).
    pub(crate) const LABEL_COUNT: usize = 3;

    /// Label values in [`Self::index`] order.
    pub(crate) const LABELS: [&'static str; Self::LABEL_COUNT] = ["commit", "rollback", "error"];

    pub(crate) fn index(self) -> usize {
        match self {
            Self::Commit => 0,
            Self::Rollback => 1,
            Self::Error => 2,
        }
    }
}

/// `status_code` label value for the error and retry series: the native
/// `Ydb.StatusIds.StatusCode` name, with transport-level errors mapped to the
/// closest native status and unknown numeric codes to `STATUS_CODE_UNSPECIFIED`.
pub(crate) fn ydb_status_code_label(error: &YdbError) -> &'static str {
    match error {
        YdbError::YdbStatusError(status) => StatusCode::try_from(status.operation_status)
            .map(|code| code.as_str_name())
            .unwrap_or(StatusCode::Unspecified.as_str_name()),
        YdbError::TransportGRPCStatus(status) => match status.code() {
            tonic::Code::DeadlineExceeded => StatusCode::Timeout.as_str_name(),
            tonic::Code::Cancelled => StatusCode::Cancelled.as_str_name(),
            tonic::Code::ResourceExhausted => StatusCode::Overloaded.as_str_name(),
            _ => StatusCode::Unavailable.as_str_name(),
        },
        YdbError::TransportDial(_) | YdbError::Transport(_) => {
            StatusCode::Unavailable.as_str_name()
        }
        YdbError::DeadlineExceeded => StatusCode::Timeout.as_str_name(),
        YdbError::Custom(_)
        | YdbError::Convert(_)
        | YdbError::NoRows
        | YdbError::InternalError(_)
        | YdbError::EndpointHasNoHost(_) => StatusCode::GenericError.as_str_name(),
    }
}

/// [`ydb_status_code_label`] for retry classification errors: customer errors
/// never classify as retryable, so the fallback bucket is unreachable in
/// practice on retry paths.
pub(crate) fn ydb_or_customer_status_code_label(error: &YdbOrCustomerError) -> &'static str {
    match error {
        YdbOrCustomerError::YDB(error) => ydb_status_code_label(error),
        YdbOrCustomerError::Customer(_) => StatusCode::GenericError.as_str_name(),
    }
}

/// Synchronous session pool gauge values, re-emitted by the pool at every mutation
/// site (never sampled by a timer).
///
/// The values are derived from the pool's own counters, so they are always consistent
/// with `SessionPool::stats()`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct SessionPoolGaugeSnapshot {
    /// Sessions waiting in the idle stack (`state="idle"`).
    pub idle: f64,
    /// Sessions leased to callers (`state="active"`).
    pub active: f64,
    /// CreateSession RPCs in flight (`state="creating"`).
    pub creating: f64,
    /// Configured pool size limit (`ydb_session_pool_size_limit`).
    pub limit: f64,
    /// Callers waiting for a free session (`ydb_session_pool_pending_requests`).
    pub pending: f64,
}

impl SessionPoolGaugeSnapshot {
    /// Snapshot of a freshly created pool: nothing idle, active, or pending.
    pub(crate) fn initial(limit: usize) -> Self {
        Self {
            idle: 0.0,
            active: 0.0,
            creating: 0.0,
            limit: limit as f64,
            pending: 0.0,
        }
    }
}

/// Internal component-facing access to the SDK's individual metric handles.
///
/// External backends implement [`MetricsBackend`]; component metric names and
/// label enums remain crate-private.
pub(crate) trait MetricsRecorder: Send + Sync {
    fn client_new_counter(&self) -> &CounterHandle;
    fn client_new_table_client_counter(&self) -> &CounterHandle;
    fn client_new_query_client_counter(&self) -> &CounterHandle;
    fn client_new_scheme_client_counter(&self) -> &CounterHandle;
    fn client_new_topic_client_counter(&self) -> &CounterHandle;
    fn client_query_row_counter(&self) -> &CounterHandle;
    fn client_transaction_query_row_counter(&self) -> &CounterHandle;
    fn client_transaction_exec_counter(&self) -> &CounterHandle;
    fn client_transaction_commit_counter(&self) -> &CounterHandle;
    fn client_transaction_rollback_counter(&self) -> &CounterHandle;
    fn client_row_query_time_histogram(&self) -> &HistogramHandle;
    fn client_transaction_row_query_time_histogram(&self) -> &HistogramHandle;

    /// Change a connection pool entry count for `endpoint` (gRPC transport gauge).
    ///
    /// The gauge is emitted synchronously at pool entry creation; with lazy channels
    /// it reflects pool entries rather than live sockets.
    fn grpc_connections_add(&self, _endpoint: &str, _state: GrpcConnectionState, _delta: f64) {}

    /// Record one connection establishment attempt (dial + TLS handshake) duration.
    ///
    /// With lazy channels this measures endpoint/channel assembly in the pool rather
    /// than the real first handshake.
    fn grpc_connection_establish(&self, _endpoint: &str, _ok: bool, _duration: Duration) {}

    /// Record a completed RPC: request counter, duration, and transport error view.
    ///
    /// `grpc_code` is the gRPC status code name; a non-`ok` code additionally counts
    /// as a transport error.
    fn grpc_request(
        &self,
        _endpoint: &str,
        _service: &str,
        _method: &str,
        _grpc_code: &str,
        _duration: Duration,
    ) {
    }

    /// Count a gRPC stream message flowing in `direction`.
    fn grpc_stream_message(
        &self,
        _endpoint: &str,
        _service: &str,
        _method: &str,
        _direction: StreamDirection,
    ) {
    }

    /// Emit the session pool gauges synchronously after a pool state change.
    fn session_pool_gauges(&self, _snapshot: SessionPoolGaugeSnapshot) {}

    /// Record one acquire attempt outcome and its duration.
    fn session_pool_acquire(&self, _result: SessionPoolAcquireResult, _duration: Duration) {}

    /// Record a CreateSession(+Attach) duration (successful creations only).
    fn session_pool_session_create(&self, _duration: Duration) {}

    /// Count a successfully created session.
    fn session_pool_session_created(&self) {}

    /// Count a session close with its reason.
    fn session_pool_session_closed(&self, _reason: SessionPoolCloseReason) {}

    /// Record how long a session was leased to one caller.
    fn session_pool_session_use(&self, _duration: Duration) {}

    /// Record the outcome of the background session liveness (attach stream) watcher.
    fn session_pool_keepalive(&self, _ok: bool) {}

    /// Record one query service operation and its duration.
    fn query_operation(&self, _operation: QueryOperation, _ok: bool, _duration: Duration) {}

    /// Record the status of a failed query service operation.
    fn query_operation_error(&self, _operation: QueryOperation, _status_code: &str) {}

    /// Record the result size of a query service operation.
    fn query_operation_result(&self, _operation: QueryOperation, _rows: u64, _bytes: u64) {}

    /// Record the outcome and duration of a whole retried transaction.
    fn query_transaction(&self, _result: QueryTransactionResult, _duration: Duration) {}

    /// Record one retryable error inside a retried transaction.
    fn query_transaction_retry(&self, _status_code: &str) {}

    /// Record one named query service operation and its duration.
    fn query_named_operation(&self, _query_name: &'static str, _ok: bool, _duration: Duration) {}

    /// Record the status of a failed named query service operation.
    fn query_named_operation_error(&self, _query_name: &'static str, _status_code: &str) {}

    /// Record the result size of a named query service operation.
    fn query_named_operation_result(&self, _query_name: &'static str, _rows: u64, _bytes: u64) {}
}

/// Default internal [`MetricsRecorder`] implementation.
///
/// Handles are registered through the SDK [`MetricsBackend`]. With no explicit
/// backend, the `metrics` feature selects the ambient facade adapter; without
/// that feature the default backend is a no-op.
pub(crate) struct DefaultMetricsRecorder {
    names: names::MetricsNames,
}

impl DefaultMetricsRecorder {
    pub(crate) fn new() -> Self {
        Self::from_parts(None, Vec::new(), None)
    }

    #[cfg(test)]
    pub(crate) fn with_backend<B>(backend: B) -> Self
    where
        B: MetricsBackend + 'static,
    {
        Self::from_parts(None, Vec::new(), Some(Arc::new(backend)))
    }

    pub(crate) fn from_parts(
        driver_name: Option<String>,
        extra_labels: Vec<(String, String)>,
        backend: Option<Arc<dyn MetricsBackend>>,
    ) -> Self {
        let base = names::base_labels(driver_name, extra_labels);
        let backend = backend.unwrap_or_else(default_backend);
        Self {
            names: names::MetricsNames::new(base, backend),
        }
    }

    pub(crate) fn with_metrics_detail(mut self, detail: MetricsDetail) -> Self {
        self.names.endpoints.set_detail(detail);
        self
    }

    pub(crate) fn with_max_endpoints(mut self, max_endpoints: usize) -> Self {
        self.names.endpoints.set_max_endpoints(max_endpoints);
        self
    }

    /// Cap the number of unique `query_name` values admitted into the named
    /// query series (`ydb_query_named_*`); defaults to 512.
    pub(crate) fn with_max_query_names(mut self, max_query_names: usize) -> Self {
        self.names.query_names = QueryNameRegistry::new(max_query_names);
        self
    }

    fn grpc_labels(
        &self,
        endpoint: EndpointKey,
        mut dynamic: Vec<MetricLabel>,
    ) -> Vec<MetricLabel> {
        if let Some(label) = endpoint.label() {
            dynamic.insert(0, label);
        }
        self.names.dynamic_labels(dynamic)
    }
}

impl Default for DefaultMetricsRecorder {
    fn default() -> Self {
        Self::new()
    }
}

fn default_backend() -> Arc<dyn MetricsBackend> {
    #[cfg(feature = "metrics")]
    {
        Arc::new(metrics_facade::MetricsFacadeBackend::ambient())
    }
    #[cfg(not(feature = "metrics"))]
    {
        Arc::new(backend::NoopMetricsBackend)
    }
}

impl MetricsRecorder for DefaultMetricsRecorder {
    fn client_new_counter(&self) -> &CounterHandle {
        &self.names.client_new_counter
    }

    fn client_new_table_client_counter(&self) -> &CounterHandle {
        &self.names.client_new_table_client_counter
    }

    fn client_new_query_client_counter(&self) -> &CounterHandle {
        &self.names.client_new_query_client_counter
    }

    fn client_new_scheme_client_counter(&self) -> &CounterHandle {
        &self.names.client_new_scheme_client_counter
    }

    fn client_new_topic_client_counter(&self) -> &CounterHandle {
        &self.names.client_new_topic_client_counter
    }

    fn client_query_row_counter(&self) -> &CounterHandle {
        &self.names.client_query_row_counter
    }

    fn client_transaction_query_row_counter(&self) -> &CounterHandle {
        &self.names.client_transaction_query_row_counter
    }

    fn client_transaction_exec_counter(&self) -> &CounterHandle {
        &self.names.client_transaction_exec_counter
    }

    fn client_transaction_commit_counter(&self) -> &CounterHandle {
        &self.names.client_transaction_commit_counter
    }

    fn client_transaction_rollback_counter(&self) -> &CounterHandle {
        &self.names.client_transaction_rollback_counter
    }

    fn client_row_query_time_histogram(&self) -> &HistogramHandle {
        &self.names.client_row_query_time_histogram
    }

    fn client_transaction_row_query_time_histogram(&self) -> &HistogramHandle {
        &self.names.client_transaction_row_query_time_histogram
    }

    fn grpc_connections_add(&self, endpoint: &str, state: GrpcConnectionState, delta: f64) {
        let endpoint = self.names.endpoints.resolve(endpoint);
        let handle = dynamic::cached(
            &self.names.dynamic.grpc_connections,
            (endpoint, state.index()),
            || {
                let labels =
                    self.grpc_labels(endpoint, vec![MetricLabel::new("state", state.as_label())]);
                self.names.gauge(
                    "ydb_grpc_connections",
                    MetricUnit::Count,
                    "ydb gRPC connection pool entries by state",
                    &labels,
                )
            },
        );
        handle.increment(delta);
    }

    fn grpc_connection_establish(&self, endpoint: &str, ok: bool, duration: Duration) {
        let endpoint = self.names.endpoints.resolve(endpoint);
        let handle = dynamic::cached(
            &self.names.dynamic.grpc_connection_establish,
            (endpoint, ok),
            || {
                let labels = self.grpc_labels(
                    endpoint,
                    vec![MetricLabel::new("result", if ok { "ok" } else { "error" })],
                );
                self.names.histogram(
                    "ydb_grpc_connection_establish_seconds",
                    MetricUnit::Seconds,
                    "ydb gRPC connection establishment duration by result",
                    &labels,
                )
            },
        );
        handle.record(duration.as_secs_f64());
    }

    fn grpc_request(
        &self,
        endpoint: &str,
        service: &str,
        method: &str,
        grpc_code: &str,
        duration: Duration,
    ) {
        let endpoint = self.names.endpoints.resolve(endpoint);
        let service = intern(service);
        let method = intern(method);
        let code = GrpcCodeKey::from_label(grpc_code);

        dynamic::cached(
            &self.names.dynamic.grpc_requests,
            (endpoint, service, method, code),
            || {
                let labels = self.grpc_labels(
                    endpoint,
                    vec![
                        MetricLabel::new("service", resolve(service)),
                        MetricLabel::new("method", resolve(method)),
                        MetricLabel::new("grpc_code", code.label_value()),
                    ],
                );
                self.names.counter(
                    "ydb_grpc_requests_total",
                    MetricUnit::Count,
                    "ydb gRPC requests by status",
                    &labels,
                )
            },
        )
        .increment(1);

        // No `grpc_code` label in this series: it must not share a cache key
        // with `ydb_grpc_requests_total`, or each observation would be recorded
        // through both handles.
        dynamic::cached(
            &self.names.dynamic.grpc_request_duration,
            (endpoint, service, method),
            || {
                let labels = self.grpc_labels(
                    endpoint,
                    vec![
                        MetricLabel::new("service", resolve(service)),
                        MetricLabel::new("method", resolve(method)),
                    ],
                );
                self.names.histogram(
                    "ydb_grpc_request_duration_seconds",
                    MetricUnit::Seconds,
                    "ydb gRPC request duration",
                    &labels,
                )
            },
        )
        .record(duration.as_secs_f64());

        if !code.is_ok() {
            dynamic::cached(&self.names.dynamic.grpc_errors, (endpoint, code), || {
                let labels = self.grpc_labels(
                    endpoint,
                    vec![MetricLabel::new("grpc_code", code.label_value())],
                );
                self.names.counter(
                    "ydb_grpc_errors_total",
                    MetricUnit::Count,
                    "ydb gRPC transport errors by status",
                    &labels,
                )
            })
            .increment(1);
        }
    }

    fn grpc_stream_message(
        &self,
        endpoint: &str,
        service: &str,
        method: &str,
        direction: StreamDirection,
    ) {
        let endpoint = self.names.endpoints.resolve(endpoint);
        let service = intern(service);
        let method = intern(method);
        dynamic::cached(
            &self.names.dynamic.grpc_stream_messages,
            (endpoint, service, method, direction.index()),
            || {
                let labels = self.grpc_labels(
                    endpoint,
                    vec![
                        MetricLabel::new("service", resolve(service)),
                        MetricLabel::new("method", resolve(method)),
                        MetricLabel::new("direction", direction.as_label()),
                    ],
                );
                self.names.counter(
                    "ydb_grpc_stream_messages_total",
                    MetricUnit::Count,
                    "ydb gRPC stream messages by direction",
                    &labels,
                )
            },
        )
        .increment(1);
    }

    fn session_pool_gauges(&self, snapshot: SessionPoolGaugeSnapshot) {
        self.names.session_pool_sessions_idle.set(snapshot.idle);
        self.names.session_pool_sessions_active.set(snapshot.active);
        self.names
            .session_pool_sessions_creating
            .set(snapshot.creating);
        self.names.session_pool_size_limit.set(snapshot.limit);
        self.names
            .session_pool_pending_requests
            .set(snapshot.pending);
    }

    fn session_pool_acquire(&self, result: SessionPoolAcquireResult, duration: Duration) {
        self.names.session_pool_acquire_total[result.index()].increment(1);
        self.names.session_pool_acquire_seconds[result.index()].record(duration.as_secs_f64());
    }

    fn session_pool_session_create(&self, duration: Duration) {
        self.names
            .session_pool_session_create_seconds
            .record(duration.as_secs_f64());
    }

    fn session_pool_session_created(&self) {
        self.names.session_pool_sessions_created_total.increment(1);
    }

    fn session_pool_session_closed(&self, reason: SessionPoolCloseReason) {
        self.names.session_pool_sessions_closed_total[reason.index()].increment(1);
    }

    fn session_pool_session_use(&self, duration: Duration) {
        self.names
            .session_pool_session_use_seconds
            .record(duration.as_secs_f64());
    }

    fn session_pool_keepalive(&self, ok: bool) {
        self.names.session_pool_keepalive_total[usize::from(!ok)].increment(1);
    }

    fn query_operation(&self, operation: QueryOperation, ok: bool, duration: Duration) {
        self.names.query_operations_total[operation.index()][usize::from(!ok)].increment(1);
        self.names.query_operation_duration_seconds[operation.index()]
            .record(duration.as_secs_f64());
    }

    fn query_operation_error(&self, operation: QueryOperation, status_code: &str) {
        let status_code = intern(status_code);
        dynamic::cached(
            &self.names.dynamic.query_errors,
            (operation.index(), status_code),
            || {
                let labels = self.names.dynamic_labels(vec![
                    MetricLabel::new("operation", QueryOperation::LABELS[operation.index()]),
                    MetricLabel::new("status_code", resolve(status_code)),
                ]);
                self.names.counter(
                    "ydb_query_errors_total",
                    MetricUnit::Count,
                    "ydb query errors by operation and status",
                    &labels,
                )
            },
        )
        .increment(1);
    }

    fn query_operation_result(&self, operation: QueryOperation, rows: u64, bytes: u64) {
        self.names.query_result_rows[operation.index()].record(rows as f64);
        self.names.query_result_bytes[operation.index()].record(bytes as f64);
    }

    fn query_transaction(&self, result: QueryTransactionResult, duration: Duration) {
        self.names.query_transactions_total[result.index()].increment(1);
        self.names
            .query_transaction_duration_seconds
            .record(duration.as_secs_f64());
    }

    fn query_transaction_retry(&self, status_code: &str) {
        let status_code = intern(status_code);
        dynamic::cached(&self.names.dynamic.query_tx_retries, status_code, || {
            let labels = self
                .names
                .dynamic_labels(vec![MetricLabel::new("status_code", resolve(status_code))]);
            self.names.counter(
                "ydb_query_transaction_retries_total",
                MetricUnit::Count,
                "ydb query transaction retryable errors by status",
                &labels,
            )
        })
        .increment(1);
    }

    fn query_named_operation(&self, query_name: &'static str, ok: bool, duration: Duration) {
        let name = self.names.query_names.resolve(query_name);
        let result = usize::from(!ok);
        dynamic::cached(
            &self.names.dynamic.query_named_operations,
            (name, result),
            || {
                let labels = self.names.dynamic_labels(vec![
                    MetricLabel::new("query_name", QueryNameRegistry::label_value(name)),
                    MetricLabel::new("result", names::QUERY_RESULT_LABELS[result]),
                ]);
                self.names.counter(
                    "ydb_query_named_operations_total",
                    MetricUnit::Count,
                    "ydb named query operations by result",
                    &labels,
                )
            },
        )
        .increment(1);

        dynamic::cached(
            &self.names.dynamic.query_named_operation_duration,
            name,
            || {
                let labels = self.names.dynamic_labels(vec![MetricLabel::new(
                    "query_name",
                    QueryNameRegistry::label_value(name),
                )]);
                self.names.histogram(
                    "ydb_query_named_operation_duration_seconds",
                    MetricUnit::Seconds,
                    "ydb named query operation duration",
                    &labels,
                )
            },
        )
        .record(duration.as_secs_f64());
    }

    fn query_named_operation_error(&self, query_name: &'static str, status_code: &str) {
        let name = self.names.query_names.resolve(query_name);
        let status_code = intern(status_code);
        dynamic::cached(
            &self.names.dynamic.query_named_errors,
            (name, status_code),
            || {
                let labels = self.names.dynamic_labels(vec![
                    MetricLabel::new("query_name", QueryNameRegistry::label_value(name)),
                    MetricLabel::new("status_code", resolve(status_code)),
                ]);
                self.names.counter(
                    "ydb_query_named_errors_total",
                    MetricUnit::Count,
                    "ydb named query errors by status",
                    &labels,
                )
            },
        )
        .increment(1);
    }

    fn query_named_operation_result(&self, query_name: &'static str, rows: u64, bytes: u64) {
        let name = self.names.query_names.resolve(query_name);
        let label = || {
            self.names.dynamic_labels(vec![MetricLabel::new(
                "query_name",
                QueryNameRegistry::label_value(name),
            )])
        };
        dynamic::cached(&self.names.dynamic.query_named_result_rows, name, || {
            self.names.histogram(
                "ydb_query_named_result_rows",
                MetricUnit::Count,
                "ydb named query result rows",
                &label(),
            )
        })
        .record(rows as f64);
        dynamic::cached(&self.names.dynamic.query_named_result_bytes, name, || {
            self.names.histogram(
                "ydb_query_named_result_bytes",
                MetricUnit::Bytes,
                "ydb named query approximate result bytes",
                &label(),
            )
        })
        .record(bytes as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[cfg(not(feature = "metrics"))]
    #[test]
    fn default_backend_without_metrics_feature_is_noop() {
        let backend = default_backend();
        let description =
            MetricDescription::new("noop_test", MetricUnit::Count, "No-op backend test");

        backend.register_counter(&description, &[]).increment(1);
        let gauge = backend.register_gauge(&description, &[]);
        gauge.increment(1.0);
        gauge.decrement(0.5);
        gauge.set(2.0);
        backend.register_histogram(&description, &[]).record(1.0);
    }

    #[derive(Clone)]
    struct CapturedMetric {
        name: String,
        unit: MetricUnit,
        description: String,
        labels: Vec<(String, String)>,
    }

    /// Minimal SDK backend capturing counter/gauge/histogram registrations.
    #[derive(Clone, Default)]
    struct CaptureRecorder {
        registered: Arc<Mutex<Vec<CapturedMetric>>>,
    }

    impl CaptureRecorder {
        fn push(&self, description: &MetricDescription, labels: &[MetricLabel]) {
            let labels = labels
                .iter()
                .map(|label| (label.key().to_string(), label.value().to_string()))
                .collect();
            self.registered.lock().unwrap().push(CapturedMetric {
                name: description.name().to_string(),
                unit: description.unit(),
                description: description.description().to_string(),
                labels,
            });
        }

        fn registered(&self) -> Vec<CapturedMetric> {
            self.registered.lock().unwrap().clone()
        }

        fn count(&self, name: &str) -> usize {
            self.registered()
                .iter()
                .filter(|metric| metric.name == name)
                .count()
        }
    }

    impl MetricsBackend for CaptureRecorder {
        fn register_counter(
            &self,
            description: &MetricDescription,
            labels: &[MetricLabel],
        ) -> Arc<dyn Counter> {
            self.push(description, labels);
            backend::noop_counter()
        }

        fn register_gauge(
            &self,
            description: &MetricDescription,
            labels: &[MetricLabel],
        ) -> Arc<dyn Gauge> {
            self.push(description, labels);
            backend::noop_gauge()
        }

        fn register_histogram(
            &self,
            description: &MetricDescription,
            labels: &[MetricLabel],
        ) -> Arc<dyn Histogram> {
            self.push(description, labels);
            backend::noop_histogram()
        }
    }

    fn expected_metric_names() -> Vec<&'static str> {
        vec![
            "ydb_new_client_counter",
            "ydb_new_table_client_counter",
            "ydb_new_query_client_counter",
            "ydb_new_scheme_client_counter",
            "ydb_new_topic_client_counter",
            "ydb_client_query_row_counter",
            "ydb_client_transaction_query_row_counter",
            "ydb_client_transaction_exec_counter",
            "ydb_client_transaction_commit_counter",
            "ydb_client_transaction_rollback_counter",
            "ydb_row_query_time_histogram",
            "ydb_transaction_row_query_time_histogram",
        ]
    }

    #[test]
    fn default_recorder_registers_existing_series_with_labels() {
        let capture = Arc::new(CaptureRecorder::default());
        let recorder = DefaultMetricsRecorder::from_parts(
            Some("driver-a".to_string()),
            vec![("env".to_string(), "prod".to_string())],
            Some(Arc::clone(&capture) as Arc<dyn MetricsBackend>),
        );

        let _ = Arc::new(recorder);

        // The pool series are pre-registered too; this test pins existing only.
        let existing = expected_metric_names();
        let mut registered: Vec<CapturedMetric> = capture
            .registered()
            .into_iter()
            .filter(|metric| existing.contains(&metric.name.as_str()))
            .collect();
        registered.sort_by(|a, b| a.name.cmp(&b.name));
        let mut expected = expected_metric_names();
        expected.sort_unstable();
        assert_eq!(
            registered
                .iter()
                .map(|m| m.name.as_str())
                .collect::<Vec<_>>(),
            expected,
            "all existing series must register unchanged"
        );

        for metric in &registered {
            assert_eq!(
                metric.labels,
                vec![
                    ("driver_name".to_string(), "driver-a".to_string()),
                    ("env".to_string(), "prod".to_string()),
                ],
                "metric {} must keep the driver_name label first and static labels after",
                metric.name
            );
        }
    }

    #[test]
    fn default_recorder_without_driver_name_uses_main() {
        let capture = Arc::new(CaptureRecorder::default());
        let _recorder = DefaultMetricsRecorder::from_parts(
            None,
            Vec::new(),
            Some(Arc::clone(&capture) as Arc<dyn MetricsBackend>),
        );

        for metric in capture.registered() {
            assert_eq!(
                metric
                    .labels
                    .first()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
                Some(("driver_name", "main")),
                "metric {} must default to driver_name=main as the first label",
                metric.name
            );
        }
    }

    #[test]
    fn default_recorder_registers_pool_series_per_enum_label() {
        let capture = Arc::new(CaptureRecorder::default());
        let _recorder = DefaultMetricsRecorder::from_parts(
            Some("driver-pool".to_string()),
            Vec::new(),
            Some(Arc::clone(&capture) as Arc<dyn MetricsBackend>),
        );

        // One pre-registered series per closed enum label value, each carrying
        // the static labels plus exactly one series label.
        for (name, label, values) in [
            (
                "ydb_session_pool_acquire_total",
                "result",
                SessionPoolAcquireResult::LABELS.to_vec(),
            ),
            (
                "ydb_session_pool_acquire_seconds",
                "result",
                SessionPoolAcquireResult::LABELS.to_vec(),
            ),
            (
                "ydb_session_pool_sessions_closed_total",
                "reason",
                SessionPoolCloseReason::LABELS.to_vec(),
            ),
            (
                "ydb_session_pool_keepalive_total",
                "result",
                names::KEEPALIVE_RESULT_LABELS.to_vec(),
            ),
            (
                "ydb_session_pool_sessions",
                "state",
                vec!["idle", "active", "creating"],
            ),
        ] {
            let registered: Vec<CapturedMetric> = capture
                .registered()
                .into_iter()
                .filter(|metric| metric.name == name)
                .collect();
            assert_eq!(
                registered.len(),
                values.len(),
                "{name} must register one series per label value"
            );
            let mut seen: Vec<String> = registered
                .iter()
                .map(|metric| {
                    assert_eq!(
                        metric.labels.first().map(|(key, _)| key.as_str()),
                        Some("driver_name"),
                        "{name} must keep driver_name first"
                    );
                    metric
                        .labels
                        .iter()
                        .find(|(key, _)| key == label)
                        .map(|(_, value)| value.clone())
                        .unwrap_or_default()
                })
                .collect();
            seen.sort_unstable();
            let mut expected = values;
            expected.sort_unstable();
            assert_eq!(seen, expected, "{name} label values mismatch");
        }

        for name in [
            "ydb_session_pool_size_limit",
            "ydb_session_pool_pending_requests",
        ] {
            assert_eq!(capture.count(name), 1, "{name} must register once");
        }
    }

    #[test]
    fn duration_units_and_approximate_size_descriptions_are_registered() {
        let capture = Arc::new(CaptureRecorder::default());
        let recorder =
            DefaultMetricsRecorder::with_backend(Arc::clone(&capture) as Arc<dyn MetricsBackend>);
        recorder.grpc_request(
            "ep:2135",
            "service",
            "method",
            "ok",
            Duration::from_millis(1),
        );
        recorder.grpc_connection_establish("ep:2135", true, Duration::from_millis(1));
        recorder.query_named_operation("named", true, Duration::from_millis(1));
        recorder.query_named_operation_result("named", 1, 1);

        let registered = capture.registered();
        for metric in registered.iter().filter(|metric| {
            metric.name.ends_with("_seconds")
                || matches!(
                    metric.name.as_str(),
                    "ydb_row_query_time_histogram" | "ydb_transaction_row_query_time_histogram"
                )
        }) {
            assert_eq!(metric.unit, MetricUnit::Seconds, "metric {}", metric.name);
        }
        assert!(
            registered
                .iter()
                .all(|metric| !metric.name.ends_with("_milliseconds"))
        );

        for name in ["ydb_query_result_bytes", "ydb_query_named_result_bytes"] {
            let metrics: Vec<_> = registered
                .iter()
                .filter(|metric| metric.name == name)
                .collect();
            assert!(!metrics.is_empty(), "{name} must be registered");
            assert!(
                metrics
                    .iter()
                    .all(|metric| metric.description.contains("approximate")),
                "{name} must describe its byte count as approximate"
            );
        }
    }

    #[test]
    fn basic_detail_aggregates_grpc_series_without_endpoint_labels() {
        let capture = Arc::new(CaptureRecorder::default());
        let recorder =
            DefaultMetricsRecorder::with_backend(Arc::clone(&capture) as Arc<dyn MetricsBackend>);

        for endpoint in ["r3-basic-a:2135", "r3-basic-b:2135"] {
            recorder.grpc_request(
                endpoint,
                "ydb.api.v1.QueryService",
                "ExecuteQuery",
                "unavailable",
                Duration::from_millis(2),
            );
            recorder.grpc_stream_message(
                endpoint,
                "ydb.api.v1.QueryService",
                "ExecuteQuery",
                StreamDirection::Received,
            );
            recorder.grpc_connections_add(endpoint, GrpcConnectionState::Active, 1.0);
            recorder.grpc_connection_establish(endpoint, true, Duration::from_millis(2));
            assert_eq!(interning::lookup(endpoint), None);
        }

        for name in [
            "ydb_grpc_requests_total",
            "ydb_grpc_request_duration_seconds",
            "ydb_grpc_errors_total",
            "ydb_grpc_stream_messages_total",
            "ydb_grpc_connections",
            "ydb_grpc_connection_establish_seconds",
        ] {
            assert_eq!(capture.count(name), 1, "{name} must be aggregated");
        }
        for metric in capture
            .registered()
            .into_iter()
            .filter(|metric| metric.name.starts_with("ydb_grpc_"))
        {
            assert!(
                metric.labels.iter().all(|(key, _)| key != "endpoint"),
                "{} must not carry an endpoint label",
                metric.name
            );
            assert_eq!(
                metric.labels.first().map(|(key, _)| key.as_str()),
                Some("driver_name")
            );
        }
    }

    #[test]
    fn endpoint_overflow_bounds_series_without_retaining_addresses() {
        let capture = Arc::new(CaptureRecorder::default());
        let recorder =
            DefaultMetricsRecorder::with_backend(Arc::clone(&capture) as Arc<dyn MetricsBackend>)
                .with_metrics_detail(MetricsDetail::Endpoints)
                .with_max_endpoints(2);

        for index in 0..1_000 {
            let endpoint = format!("r3-series-overflow-{index}:2135");
            recorder.grpc_request(
                &endpoint,
                "ydb.api.v1.QueryService",
                "ExecuteQuery",
                "ok",
                Duration::from_millis(1),
            );
            if index >= 2 {
                assert_eq!(interning::lookup(&endpoint), None);
            }
        }
        assert_eq!(capture.count("ydb_grpc_requests_total"), 3);
        assert_eq!(capture.count("ydb_grpc_request_duration_seconds"), 3);

        let mut endpoints: Vec<String> = capture
            .registered()
            .into_iter()
            .filter(|metric| metric.name == "ydb_grpc_requests_total")
            .map(|metric| {
                assert_eq!(
                    metric.labels.first().map(|(key, _)| key.as_str()),
                    Some("driver_name")
                );
                metric
                    .labels
                    .into_iter()
                    .find(|(key, _)| key == "endpoint")
                    .map(|(_, value)| value)
                    .expect("endpoint detail must add the endpoint label")
            })
            .collect();
        endpoints.sort_unstable();
        assert_eq!(
            endpoints,
            vec![
                "__other__".to_string(),
                "r3-series-overflow-0:2135".to_string(),
                "r3-series-overflow-1:2135".to_string(),
            ]
        );
    }

    #[test]
    fn dynamic_series_register_once_per_label_combination() {
        let capture = Arc::new(CaptureRecorder::default());
        let recorder =
            DefaultMetricsRecorder::with_backend(Arc::clone(&capture) as Arc<dyn MetricsBackend>)
                .with_metrics_detail(MetricsDetail::Endpoints);

        for _ in 0..5 {
            recorder.grpc_request(
                "ep-1:2135",
                "ydb.api.v1.TableService",
                "ExecuteDataQuery",
                "ok",
                Duration::from_millis(2),
            );
        }

        assert_eq!(
            capture.count("ydb_grpc_requests_total"),
            1,
            "repeated emissions of one series must register exactly once"
        );
        assert_eq!(capture.count("ydb_grpc_request_duration_seconds"), 1);
        assert_eq!(capture.count("ydb_grpc_errors_total"), 0);

        // A different endpoint is a new series: one more registration each.
        recorder.grpc_request(
            "ep-2:2135",
            "ydb.api.v1.TableService",
            "ExecuteDataQuery",
            "ok",
            Duration::from_millis(2),
        );
        assert_eq!(capture.count("ydb_grpc_requests_total"), 2);
        assert_eq!(capture.count("ydb_grpc_request_duration_seconds"), 2);

        // Known combinations stay cached; emissions keep flowing.
        for _ in 0..5 {
            recorder.grpc_request(
                "ep-1:2135",
                "ydb.api.v1.TableService",
                "ExecuteDataQuery",
                "ok",
                Duration::from_millis(2),
            );
        }
        assert_eq!(capture.count("ydb_grpc_requests_total"), 2);
    }

    #[test]
    fn concurrent_first_emissions_register_once() {
        let capture = Arc::new(CaptureRecorder::default());
        let recorder = Arc::new(DefaultMetricsRecorder::with_backend(
            Arc::clone(&capture) as Arc<dyn MetricsBackend>
        ));

        std::thread::scope(|scope| {
            for _ in 0..8 {
                let recorder = Arc::clone(&recorder);
                scope.spawn(move || {
                    for _ in 0..10 {
                        recorder.grpc_request(
                            "ep-race:2135",
                            "ydb.api.v1.TableService",
                            "ExecuteDataQuery",
                            "ok",
                            Duration::from_millis(1),
                        );
                    }
                });
            }
        });

        assert_eq!(
            capture.count("ydb_grpc_requests_total"),
            1,
            "80 racing first emissions of one series must register exactly once"
        );
        assert_eq!(capture.count("ydb_grpc_request_duration_seconds"), 1);
    }

    #[test]
    fn default_recorder_emits_dynamic_series_values_into_backend() {
        let test = Arc::new(crate::client_metrics::test_recorder::TestRecorder::new());
        let recorder: Arc<dyn MetricsRecorder> = Arc::new(
            DefaultMetricsRecorder::with_backend(Arc::clone(&test) as Arc<dyn MetricsBackend>)
                .with_metrics_detail(MetricsDetail::Endpoints),
        );

        recorder.grpc_request(
            "ep:2135",
            "ydb.api.v1.QueryService",
            "ExecuteQuery",
            "ok",
            Duration::from_millis(5),
        );
        recorder.grpc_request(
            "ep:2135",
            "ydb.api.v1.QueryService",
            "ExecuteQuery",
            "unavailable",
            Duration::from_millis(7),
        );

        assert_eq!(test.counter_value("ydb_grpc_requests_total"), 2);
        assert_eq!(test.histogram_count("ydb_grpc_request_duration_seconds"), 2);
        assert_eq!(
            test.counter_value_with_label("ydb_grpc_errors_total", "grpc_code", "unavailable"),
            1
        );
        // Dynamic series carry the static labels first.
        assert_eq!(
            test.label_of_last("ydb_grpc_requests_total", "driver_name")
                .as_deref(),
            Some("main")
        );
        assert_eq!(
            test.label_of_last("ydb_grpc_requests_total", "endpoint")
                .as_deref(),
            Some("ep:2135")
        );

        recorder.grpc_stream_message(
            "ep:2135",
            "ydb.api.v1.QueryService",
            "ExecuteQuery",
            StreamDirection::Sent,
        );
        assert_eq!(test.counter_value("ydb_grpc_stream_messages_total"), 1);

        recorder.grpc_connections_add("ep:2135", GrpcConnectionState::Active, 1.0);
        assert_eq!(
            test.gauge_value_with_label("ydb_grpc_connections", "state", "active"),
            1.0
        );

        recorder.grpc_connection_establish("ep:2135", true, Duration::from_millis(10));
        assert_eq!(
            test.histogram_count("ydb_grpc_connection_establish_seconds"),
            1
        );
    }

    #[test]
    fn default_recorder_emits_pool_series_values_into_backend() {
        let test = Arc::new(crate::client_metrics::test_recorder::TestRecorder::new());
        let recorder: Arc<dyn MetricsRecorder> = Arc::new(DefaultMetricsRecorder::with_backend(
            Arc::clone(&test) as Arc<dyn MetricsBackend>,
        ));

        recorder.session_pool_acquire(SessionPoolAcquireResult::Ok, Duration::from_millis(3));
        assert_eq!(
            test.counter_value_with_label("ydb_session_pool_acquire_total", "result", "ok"),
            1
        );
        assert_eq!(test.histogram_count("ydb_session_pool_acquire_seconds"), 1);

        recorder.session_pool_session_created();
        recorder.session_pool_session_create(Duration::from_millis(30));
        assert_eq!(
            test.counter_value("ydb_session_pool_sessions_created_total"),
            1
        );
        assert_eq!(
            test.histogram_count("ydb_session_pool_session_create_seconds"),
            1
        );

        recorder.session_pool_session_closed(SessionPoolCloseReason::BadSession);
        assert_eq!(
            test.counter_value_with_label(
                "ydb_session_pool_sessions_closed_total",
                "reason",
                "bad_session"
            ),
            1
        );

        recorder.session_pool_session_use(Duration::from_millis(100));
        assert_eq!(
            test.histogram_count("ydb_session_pool_session_use_seconds"),
            1
        );

        recorder.session_pool_keepalive(false);
        recorder.session_pool_keepalive(true);
        assert_eq!(
            test.counter_value_with_label("ydb_session_pool_keepalive_total", "result", "error"),
            1
        );
        assert_eq!(
            test.counter_value_with_label("ydb_session_pool_keepalive_total", "result", "ok"),
            1
        );

        recorder.session_pool_gauges(SessionPoolGaugeSnapshot {
            idle: 1.0,
            active: 2.0,
            creating: 3.0,
            limit: 10.0,
            pending: 4.0,
        });
        assert_eq!(
            test.gauge_value_with_label("ydb_session_pool_sessions", "state", "idle"),
            1.0
        );
        assert_eq!(
            test.gauge_value_with_label("ydb_session_pool_sessions", "state", "active"),
            2.0
        );
        assert_eq!(
            test.gauge_value_with_label("ydb_session_pool_sessions", "state", "creating"),
            3.0
        );
        assert_eq!(test.gauge_value("ydb_session_pool_size_limit"), 10.0);
        assert_eq!(test.gauge_value("ydb_session_pool_pending_requests"), 4.0);

        // Pre-registered pool series carry the static labels too.
        assert_eq!(
            test.label_of_last("ydb_session_pool_acquire_total", "driver_name")
                .as_deref(),
            Some("main")
        );
    }

    #[test]
    fn duration_metrics_record_seconds_without_rescaling() {
        let test = Arc::new(crate::client_metrics::test_recorder::TestRecorder::new());
        let recorder: Arc<dyn MetricsRecorder> = Arc::new(DefaultMetricsRecorder::with_backend(
            Arc::clone(&test) as Arc<dyn MetricsBackend>,
        ));

        recorder.grpc_request("ep", "service", "method", "ok", Duration::from_millis(5));
        recorder.grpc_connection_establish("ep", true, Duration::from_millis(10));
        recorder.session_pool_acquire(SessionPoolAcquireResult::Ok, Duration::from_millis(3));
        recorder.session_pool_session_create(Duration::from_millis(30));
        recorder.session_pool_session_use(Duration::from_millis(100));
        recorder.query_operation(QueryOperation::Exec, true, Duration::from_millis(10));
        recorder.query_transaction(QueryTransactionResult::Commit, Duration::from_millis(100));
        recorder.query_named_operation("named", true, Duration::from_millis(12));

        for (name, expected) in [
            ("ydb_grpc_request_duration_seconds", 0.005),
            ("ydb_grpc_connection_establish_seconds", 0.010),
            ("ydb_session_pool_acquire_seconds", 0.003),
            ("ydb_session_pool_session_create_seconds", 0.030),
            ("ydb_session_pool_session_use_seconds", 0.100),
            ("ydb_query_operation_duration_seconds", 0.010),
            ("ydb_query_transaction_duration_seconds", 0.100),
            ("ydb_query_named_operation_duration_seconds", 0.012),
        ] {
            assert_eq!(test.histogram_observations(name), vec![expected], "{name}");
        }
    }

    #[test]
    fn default_recorder_with_backend_registers_into_backend() {
        let capture = Arc::new(CaptureRecorder::default());
        let recorder: Arc<dyn MetricsRecorder> = Arc::new(DefaultMetricsRecorder::with_backend(
            Arc::clone(&capture) as Arc<dyn MetricsBackend>,
        ));

        recorder.client_new_counter().increment(1);

        assert_eq!(
            capture
                .registered()
                .iter()
                .filter(|m| m.name == "ydb_new_client_counter")
                .count(),
            1,
            "with_backend must register handles into the provided backend"
        );
    }

    #[test]
    fn default_recorder_registers_query_series_per_enum_label() {
        let capture = Arc::new(CaptureRecorder::default());
        let _recorder = DefaultMetricsRecorder::from_parts(
            Some("driver-q".to_string()),
            Vec::new(),
            Some(Arc::clone(&capture) as Arc<dyn MetricsBackend>),
        );

        // operations_total: one series per (operation, result) combination.
        let operations: Vec<CapturedMetric> = capture
            .registered()
            .into_iter()
            .filter(|metric| metric.name == "ydb_query_operations_total")
            .collect();
        assert_eq!(
            operations.len(),
            QueryOperation::LABEL_COUNT * names::QUERY_RESULT_COUNT,
            "one ydb_query_operations_total series per operation/result pair"
        );
        let mut pairs: Vec<(String, String)> = operations
            .iter()
            .map(|metric| {
                assert_eq!(
                    metric.labels.first().map(|(key, _)| key.as_str()),
                    Some("driver_name"),
                    "driver_name must stay first"
                );
                let value = |name: &str| {
                    metric
                        .labels
                        .iter()
                        .find(|(key, _)| key == name)
                        .map(|(_, value)| value.clone())
                        .unwrap_or_default()
                };
                (value("operation"), value("result"))
            })
            .collect();
        pairs.sort();
        let mut expected: Vec<(String, String)> = QueryOperation::LABELS
            .iter()
            .flat_map(|operation| {
                names::QUERY_RESULT_LABELS
                    .iter()
                    .map(move |result| (operation.to_string(), result.to_string()))
            })
            .collect();
        expected.sort();
        assert_eq!(pairs, expected);

        assert_eq!(
            capture.count("ydb_query_operation_duration_seconds"),
            QueryOperation::LABEL_COUNT
        );
        assert_eq!(
            capture.count("ydb_query_result_rows"),
            QueryOperation::LABEL_COUNT
        );
        assert_eq!(
            capture.count("ydb_query_result_bytes"),
            QueryOperation::LABEL_COUNT
        );
        assert_eq!(
            capture.count("ydb_query_transactions_total"),
            QueryTransactionResult::LABEL_COUNT
        );
        assert_eq!(capture.count("ydb_query_transaction_duration_seconds"), 1);
    }

    #[test]
    fn default_recorder_emits_query_series_values_into_backend() {
        let test = Arc::new(crate::client_metrics::test_recorder::TestRecorder::new());
        let recorder: Arc<dyn MetricsRecorder> = Arc::new(DefaultMetricsRecorder::with_backend(
            Arc::clone(&test) as Arc<dyn MetricsBackend>,
        ));

        recorder.query_operation(QueryOperation::Exec, true, Duration::from_millis(10));
        recorder.query_operation(QueryOperation::QueryRow, false, Duration::from_millis(20));
        recorder.query_operation_error(QueryOperation::Exec, "UNAVAILABLE");
        recorder.query_operation_error(QueryOperation::QueryRow, "ABORTED");
        recorder.query_operation_result(QueryOperation::Exec, 5, 128);
        recorder.query_operation_result(QueryOperation::Query, 3, 24);
        recorder.query_transaction(QueryTransactionResult::Commit, Duration::from_millis(100));
        recorder.query_transaction(QueryTransactionResult::Error, Duration::from_millis(50));
        recorder.query_transaction_retry("ABORTED");
        recorder.query_transaction_retry("ABORTED");

        assert_eq!(
            test.counter_value_with_label("ydb_query_operations_total", "operation", "exec"),
            1
        );
        assert_eq!(
            test.counter_value_with_label("ydb_query_operations_total", "result", "error"),
            1
        );
        assert_eq!(
            test.histogram_count("ydb_query_operation_duration_seconds"),
            2
        );
        assert_eq!(
            test.counter_value_with_label("ydb_query_errors_total", "status_code", "UNAVAILABLE"),
            1
        );
        assert_eq!(
            test.counter_value_with_label("ydb_query_errors_total", "status_code", "ABORTED"),
            1
        );
        let mut rows = test.histogram_observations("ydb_query_result_rows");
        rows.sort_by(|a, b| a.total_cmp(b));
        assert_eq!(rows, vec![3.0, 5.0]);
        let mut bytes = test.histogram_observations("ydb_query_result_bytes");
        bytes.sort_by(|a, b| a.total_cmp(b));
        assert_eq!(bytes, vec![24.0, 128.0]);
        assert_eq!(
            test.counter_value_with_label("ydb_query_transactions_total", "result", "commit"),
            1
        );
        assert_eq!(
            test.counter_value_with_label("ydb_query_transactions_total", "result", "error"),
            1
        );
        assert_eq!(
            test.histogram_count("ydb_query_transaction_duration_seconds"),
            2
        );
        assert_eq!(
            test.counter_value_with_label(
                "ydb_query_transaction_retries_total",
                "status_code",
                "ABORTED"
            ),
            2
        );
        // Dynamic series carry the static labels too.
        assert_eq!(
            test.label_of_last("ydb_query_errors_total", "driver_name")
                .as_deref(),
            Some("main")
        );
        assert_eq!(
            test.label_of_last("ydb_query_transaction_retries_total", "driver_name")
                .as_deref(),
            Some("main")
        );
    }

    #[test]
    fn default_recorder_emits_named_query_series_into_backend() {
        let test = Arc::new(crate::client_metrics::test_recorder::TestRecorder::new());
        let recorder: Arc<dyn MetricsRecorder> = Arc::new(DefaultMetricsRecorder::with_backend(
            Arc::clone(&test) as Arc<dyn MetricsBackend>,
        ));

        recorder.query_named_operation("get_account_balance", true, Duration::from_millis(12));
        recorder.query_named_operation("get_account_balance", false, Duration::from_millis(34));
        recorder.query_named_operation_error("get_account_balance", "ABORTED");
        recorder.query_named_operation_result("get_account_balance", 7, 256);

        assert_eq!(
            test.counter_value_with_label("ydb_query_named_operations_total", "result", "ok"),
            1
        );
        assert_eq!(
            test.counter_value_with_label("ydb_query_named_operations_total", "result", "error"),
            1
        );
        assert_eq!(
            test.label_of_last("ydb_query_named_operations_total", "query_name")
                .as_deref(),
            Some("get_account_balance")
        );
        assert_eq!(
            test.histogram_count("ydb_query_named_operation_duration_seconds"),
            2
        );
        assert_eq!(
            test.counter_value_with_label("ydb_query_named_errors_total", "status_code", "ABORTED"),
            1
        );
        assert_eq!(
            test.histogram_observations("ydb_query_named_result_rows"),
            vec![7.0]
        );
        assert_eq!(
            test.histogram_observations("ydb_query_named_result_bytes"),
            vec![256.0]
        );
        // Named dynamic series carry the static labels first.
        assert_eq!(
            test.label_of_last("ydb_query_named_operations_total", "driver_name")
                .as_deref(),
            Some("main")
        );
    }

    #[test]
    fn named_series_register_once_per_query_name() {
        let capture = Arc::new(CaptureRecorder::default());
        let recorder =
            DefaultMetricsRecorder::with_backend(Arc::clone(&capture) as Arc<dyn MetricsBackend>);

        for _ in 0..5 {
            recorder.query_named_operation("named_q", true, Duration::from_millis(1));
        }
        assert_eq!(capture.count("ydb_query_named_operations_total"), 1);
        assert_eq!(
            capture.count("ydb_query_named_operation_duration_seconds"),
            1
        );

        recorder.query_named_operation("other_q", true, Duration::from_millis(1));
        assert_eq!(capture.count("ydb_query_named_operations_total"), 2);
    }

    #[test]
    fn query_name_limit_collapses_new_names_to_other() {
        let test = Arc::new(crate::client_metrics::test_recorder::TestRecorder::new());
        let recorder: Arc<dyn MetricsRecorder> = Arc::new(
            DefaultMetricsRecorder::with_backend(Arc::clone(&test) as Arc<dyn MetricsBackend>)
                .with_max_query_names(1),
        );

        recorder.query_named_operation("first_query", true, Duration::from_millis(1));
        recorder.query_named_operation("second_query", true, Duration::from_millis(1));

        assert_eq!(
            test.counter_value_with_label(
                "ydb_query_named_operations_total",
                "query_name",
                "first_query"
            ),
            1
        );
        assert_eq!(
            test.counter_value_with_label(
                "ydb_query_named_operations_total",
                "query_name",
                "second_query"
            ),
            0,
            "a name beyond the limit must not appear as its own series"
        );
        assert_eq!(
            test.counter_value_with_label(
                "ydb_query_named_operations_total",
                "query_name",
                "__other__"
            ),
            1
        );
    }

    #[test]
    fn invalid_query_name_maps_to_invalid_bucket() {
        // Bypasses the `with_name` debug gate to pin the release-build mapping.
        let test = Arc::new(crate::client_metrics::test_recorder::TestRecorder::new());
        let recorder: Arc<dyn MetricsRecorder> = Arc::new(DefaultMetricsRecorder::with_backend(
            Arc::clone(&test) as Arc<dyn MetricsBackend>,
        ));

        recorder.query_named_operation("Bad-Name", true, Duration::from_millis(1));

        assert_eq!(
            test.counter_value_with_label(
                "ydb_query_named_operations_total",
                "query_name",
                "Bad-Name"
            ),
            0,
            "a malformed name must never appear as its own series"
        );
        assert_eq!(
            test.counter_value_with_label(
                "ydb_query_named_operations_total",
                "query_name",
                "__invalid__"
            ),
            1
        );
    }

    #[test]
    fn ydb_status_code_label_maps_errors_to_native_statuses() {
        use crate::errors::YdbStatusError;
        use ydb_grpc::ydb_proto::status_ids::StatusCode;

        let status = |code: StatusCode| {
            YdbError::YdbStatusError(YdbStatusError::new("test", code as i32, Vec::new()))
        };
        assert_eq!(
            ydb_status_code_label(&status(StatusCode::Aborted)),
            "ABORTED"
        );
        assert_eq!(
            ydb_status_code_label(&status(StatusCode::Overloaded)),
            "OVERLOADED"
        );
        assert_eq!(
            ydb_status_code_label(&YdbError::Transport("dial failed".to_string())),
            "UNAVAILABLE"
        );
        assert_eq!(
            ydb_status_code_label(&YdbError::TransportGRPCStatus(Arc::new(
                tonic::Status::unavailable("off")
            ))),
            "UNAVAILABLE"
        );
        assert_eq!(
            ydb_status_code_label(&YdbError::TransportGRPCStatus(Arc::new(
                tonic::Status::deadline_exceeded("late")
            ))),
            "TIMEOUT"
        );
        assert_eq!(
            ydb_status_code_label(&YdbError::TransportGRPCStatus(Arc::new(
                tonic::Status::resource_exhausted("busy")
            ))),
            "OVERLOADED"
        );
        assert_eq!(
            ydb_status_code_label(&YdbError::DeadlineExceeded),
            "TIMEOUT"
        );
        assert_eq!(
            ydb_status_code_label(&YdbError::Custom("client".to_string())),
            "GENERIC_ERROR"
        );
        assert_eq!(
            ydb_status_code_label(&YdbError::YdbStatusError(YdbStatusError::new(
                "unknown",
                999_999_999,
                Vec::new(),
            ))),
            "STATUS_CODE_UNSPECIFIED"
        );
    }

    /// Test [`MetricsRecorder`] implementation: counts getter accesses, proving the
    /// trait is object safe and usable through `Arc<dyn MetricsRecorder>`.
    struct CountingRecorder {
        accesses: AtomicUsize,
        counter: CounterHandle,
        histogram: HistogramHandle,
    }

    impl CountingRecorder {
        fn new() -> Self {
            Self {
                accesses: AtomicUsize::new(0),
                counter: backend::noop_counter(),
                histogram: backend::noop_histogram(),
            }
        }

        fn counter_getter(&self) -> &CounterHandle {
            self.accesses.fetch_add(1, Ordering::SeqCst);
            &self.counter
        }

        fn histogram_getter(&self) -> &HistogramHandle {
            self.accesses.fetch_add(1, Ordering::SeqCst);
            &self.histogram
        }
    }

    impl MetricsRecorder for CountingRecorder {
        fn client_new_counter(&self) -> &CounterHandle {
            self.counter_getter()
        }

        fn client_new_table_client_counter(&self) -> &CounterHandle {
            self.counter_getter()
        }

        fn client_new_query_client_counter(&self) -> &CounterHandle {
            self.counter_getter()
        }

        fn client_new_scheme_client_counter(&self) -> &CounterHandle {
            self.counter_getter()
        }

        fn client_new_topic_client_counter(&self) -> &CounterHandle {
            self.counter_getter()
        }

        fn client_query_row_counter(&self) -> &CounterHandle {
            self.counter_getter()
        }

        fn client_transaction_query_row_counter(&self) -> &CounterHandle {
            self.counter_getter()
        }

        fn client_transaction_exec_counter(&self) -> &CounterHandle {
            self.counter_getter()
        }

        fn client_transaction_commit_counter(&self) -> &CounterHandle {
            self.counter_getter()
        }

        fn client_transaction_rollback_counter(&self) -> &CounterHandle {
            self.counter_getter()
        }

        fn client_row_query_time_histogram(&self) -> &HistogramHandle {
            self.histogram_getter()
        }

        fn client_transaction_row_query_time_histogram(&self) -> &HistogramHandle {
            self.histogram_getter()
        }
    }

    #[test]
    fn custom_recorder_works_through_trait_object() {
        let counting = Arc::new(CountingRecorder::new());
        let recorder: Arc<dyn MetricsRecorder> = counting.clone();

        recorder.client_new_counter().increment(1);
        recorder.client_transaction_commit_counter().increment(1);
        recorder.client_row_query_time_histogram().record(1.0);

        assert_eq!(counting.accesses.load(Ordering::SeqCst), 3);
    }
}
