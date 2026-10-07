use std::sync::Arc;
use std::time::Duration;

use metrics::{Counter, Histogram, counter, gauge, histogram};

pub(crate) mod dynamic;
pub(crate) mod interning;
pub(crate) mod names;
#[cfg(test)]
pub(crate) mod test_recorder;

use dynamic::GrpcCodeKey;
use interning::{intern, resolve};
use metrics::Label;

/// Connection state label for the `ydb_grpc_connections` gauge.
///
/// The `idle` state from the metrics spec is not emitted: pool channels are created
/// with `connect_lazy`, so an idle live-socket state cannot be observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GrpcConnectionState {
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
pub enum StreamDirection {
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
pub enum SessionPoolAcquireResult {
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
pub enum SessionPoolCloseReason {
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

/// Synchronous session pool gauge values, re-emitted by the pool at every mutation
/// site (never sampled by a timer).
///
/// The values are derived from the pool's own counters, so they are always consistent
/// with `SessionPool::stats()`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SessionPoolGaugeSnapshot {
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

/// Access to the SDK's individual metric handles.
///
/// SDK components record through the read-only handle getters only; they never touch
/// metric names or label sets directly. The default implementation,
/// [`DefaultMetricsRecorder`], registers its handles into a `metrics` backend — the
/// ambient/global recorder or a caller-provided one — at construction time.
///
/// Alternative implementations may map the getters onto their own metric names,
/// labels, and buckets without changing component code. Handles are internally
/// atomic, so `&self` getters need no interior mutability and components can share
/// an `Arc<dyn MetricsRecorder>` without locks.
pub trait MetricsRecorder: Send + Sync {
    fn client_new_counter(&self) -> &Counter;
    fn client_new_table_client_counter(&self) -> &Counter;
    fn client_new_query_client_counter(&self) -> &Counter;
    fn client_new_scheme_client_counter(&self) -> &Counter;
    fn client_new_topic_client_counter(&self) -> &Counter;
    fn client_query_row_counter(&self) -> &Counter;
    fn client_transaction_query_row_counter(&self) -> &Counter;
    fn client_transaction_exec_counter(&self) -> &Counter;
    fn client_transaction_commit_counter(&self) -> &Counter;
    fn client_transaction_rollback_counter(&self) -> &Counter;
    fn client_row_query_time_histogram(&self) -> &Histogram;
    fn client_transaction_row_query_time_histogram(&self) -> &Histogram;

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
    ///
    /// All values are recomputed from the pool's own counters at the mutation site —
    /// never sampled by a timer, so state between mutations is never stale.
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
}

/// Default [`MetricsRecorder`] implementation.
///
/// Owns the metric handles created from a driver name and static labels (see
/// [`crate::ClientBuilder::with_driver_name`] and
/// [`crate::ClientBuilder::with_metrics_label`]), optionally registering them into a
/// caller-provided `metrics::Recorder` backend which is kept alive here. Metric
/// names, labels, and buckets are an internal detail: components only reach the
/// handles through the [`MetricsRecorder`] getters.
///
/// Metric access is allocation-free on the hot path. Series with closed enum
/// label sets are pre-registered at construction and indexed by the enum.
/// Dynamic series (gRPC transport metrics) intern their label strings
/// ([`interning`]) and cache one handle per label combination; labels are
/// materialized and the handle registered into the backend only on the first
/// emission of a combination. With no caller-provided backend, handles bind to
/// the ambient/global recorder at first emission, so that recorder must be
/// installed before the first emission of a series (the same holds for the
/// pre-registered series, which bind at construction).
pub struct DefaultMetricsRecorder {
    names: names::MetricsNames,
    /// Keeps a caller-provided backend recorder alive: handles above reference it.
    _backend: Option<Arc<dyn metrics::Recorder + Send + Sync>>,
}

impl DefaultMetricsRecorder {
    /// Recorder over the ambient/global `metrics` recorder with default settings
    /// (driver name `main`, no extra labels).
    pub fn new() -> Self {
        Self::from_parts(None, Vec::new(), None)
    }

    /// Recorder registering SDK metrics into a caller-provided `metrics` backend.
    ///
    /// Use [`Self::from_parts`] to also set the driver name and static labels.
    pub fn with_backend(recorder: Arc<dyn metrics::Recorder + Send + Sync>) -> Self {
        Self::from_parts(None, Vec::new(), Some(recorder))
    }

    /// Recorder with explicit driver name, extra static labels, and optional backend.
    ///
    /// This is the constructor used by [`crate::ClientBuilder::build`]; the parts
    /// mirror the builder's `driver_name`, metrics labels, and metrics recorder
    /// settings. Without a backend, handles bind to the ambient/global recorder.
    pub fn from_parts(
        driver_name: Option<String>,
        extra_labels: Vec<(String, String)>,
        backend: Option<Arc<dyn metrics::Recorder + Send + Sync>>,
    ) -> Self {
        let base = names::base_labels(driver_name, extra_labels);
        Self {
            names: names::MetricsNames::new(base, backend.as_deref()),
            _backend: backend,
        }
    }
}

impl Default for DefaultMetricsRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultMetricsRecorder {
    /// Run a handle registration either through the caller-provided backend or
    /// through the ambient/global recorder.
    ///
    /// Called only on dynamic-series cache misses; hits write into already
    /// registered handles and skip this entirely.
    fn record<T>(&self, record: impl FnOnce() -> T) -> T {
        match self._backend.as_deref() {
            Some(recorder) => metrics::with_local_recorder(recorder, record),
            None => record(),
        }
    }
}

impl MetricsRecorder for DefaultMetricsRecorder {
    fn client_new_counter(&self) -> &Counter {
        &self.names.client_new_counter
    }

    fn client_new_table_client_counter(&self) -> &Counter {
        &self.names.client_new_table_client_counter
    }

    fn client_new_query_client_counter(&self) -> &Counter {
        &self.names.client_new_query_client_counter
    }

    fn client_new_scheme_client_counter(&self) -> &Counter {
        &self.names.client_new_scheme_client_counter
    }

    fn client_new_topic_client_counter(&self) -> &Counter {
        &self.names.client_new_topic_client_counter
    }

    fn client_query_row_counter(&self) -> &Counter {
        &self.names.client_query_row_counter
    }

    fn client_transaction_query_row_counter(&self) -> &Counter {
        &self.names.client_transaction_query_row_counter
    }

    fn client_transaction_exec_counter(&self) -> &Counter {
        &self.names.client_transaction_exec_counter
    }

    fn client_transaction_commit_counter(&self) -> &Counter {
        &self.names.client_transaction_commit_counter
    }

    fn client_transaction_rollback_counter(&self) -> &Counter {
        &self.names.client_transaction_rollback_counter
    }

    fn client_row_query_time_histogram(&self) -> &Histogram {
        &self.names.client_row_query_time_histogram
    }

    fn client_transaction_row_query_time_histogram(&self) -> &Histogram {
        &self.names.client_transaction_row_query_time_histogram
    }

    fn grpc_connections_add(&self, endpoint: &str, state: GrpcConnectionState, delta: f64) {
        let endpoint = intern(endpoint);
        let handle = dynamic::cached(
            &self.names.dynamic.grpc_connections,
            (endpoint, state.index()),
            || {
                let labels = self.names.dynamic_labels(vec![
                    Label::new("endpoint", resolve(endpoint)),
                    Label::new("state", state.as_label()),
                ]);
                self.record(|| gauge!("ydb_grpc_connections", labels.iter()))
            },
        );
        handle.increment(delta);
    }

    fn grpc_connection_establish(&self, endpoint: &str, ok: bool, duration: Duration) {
        let endpoint = intern(endpoint);
        let handle = dynamic::cached(
            &self.names.dynamic.grpc_connection_establish,
            (endpoint, ok),
            || {
                let labels = self.names.dynamic_labels(vec![
                    Label::new("endpoint", resolve(endpoint)),
                    Label::new("result", if ok { "ok" } else { "error" }),
                ]);
                self.record(|| {
                    histogram!("ydb_grpc_connection_establish_milliseconds", labels.iter())
                })
            },
        );
        handle.record(duration.as_secs_f64() * 1000.0);
    }

    fn grpc_request(
        &self,
        endpoint: &str,
        service: &str,
        method: &str,
        grpc_code: &str,
        duration: Duration,
    ) {
        let endpoint = intern(endpoint);
        let service = intern(service);
        let method = intern(method);
        let code = GrpcCodeKey::from_label(grpc_code);

        dynamic::cached(
            &self.names.dynamic.grpc_requests,
            (endpoint, service, method, code),
            || {
                let labels = self.names.dynamic_labels(vec![
                    Label::new("endpoint", resolve(endpoint)),
                    Label::new("service", resolve(service)),
                    Label::new("method", resolve(method)),
                    Label::new("grpc_code", code.label_value()),
                ]);
                self.record(|| counter!("ydb_grpc_requests_total", labels.iter()))
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
                let labels = self.names.dynamic_labels(vec![
                    Label::new("endpoint", resolve(endpoint)),
                    Label::new("service", resolve(service)),
                    Label::new("method", resolve(method)),
                ]);
                self.record(|| histogram!("ydb_grpc_request_duration_milliseconds", labels.iter()))
            },
        )
        .record(duration.as_secs_f64() * 1000.0);

        if !code.is_ok() {
            dynamic::cached(&self.names.dynamic.grpc_errors, (endpoint, code), || {
                let labels = self.names.dynamic_labels(vec![
                    Label::new("endpoint", resolve(endpoint)),
                    Label::new("grpc_code", code.label_value()),
                ]);
                self.record(|| counter!("ydb_grpc_errors_total", labels.iter()))
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
        let endpoint = intern(endpoint);
        let service = intern(service);
        let method = intern(method);
        dynamic::cached(
            &self.names.dynamic.grpc_stream_messages,
            (endpoint, service, method, direction.index()),
            || {
                let labels = self.names.dynamic_labels(vec![
                    Label::new("endpoint", resolve(endpoint)),
                    Label::new("service", resolve(service)),
                    Label::new("method", resolve(method)),
                    Label::new("direction", direction.as_label()),
                ]);
                self.record(|| counter!("ydb_grpc_stream_messages_total", labels.iter()))
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
        self.names.session_pool_acquire_milliseconds[result.index()]
            .record(duration.as_secs_f64() * 1000.0);
    }

    fn session_pool_session_create(&self, duration: Duration) {
        self.names
            .session_pool_session_create_milliseconds
            .record(duration.as_secs_f64() * 1000.0);
    }

    fn session_pool_session_created(&self) {
        self.names.session_pool_sessions_created_total.increment(1);
    }

    fn session_pool_session_closed(&self, reason: SessionPoolCloseReason) {
        self.names.session_pool_sessions_closed_total[reason.index()].increment(1);
    }

    fn session_pool_session_use(&self, duration: Duration) {
        self.names
            .session_pool_session_use_milliseconds
            .record(duration.as_secs_f64() * 1000.0);
    }

    fn session_pool_keepalive(&self, ok: bool) {
        self.names.session_pool_keepalive_total[usize::from(!ok)].increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Default)]
    struct CapturedMetric {
        name: String,
        labels: Vec<(String, String)>,
    }

    /// Minimal `metrics::Recorder` capturing counter/histogram handle registrations.
    #[derive(Default)]
    struct CaptureRecorder {
        registered: Mutex<Vec<CapturedMetric>>,
    }

    impl CaptureRecorder {
        fn push(&self, key: &metrics::Key) {
            let labels = key
                .labels()
                .map(|label| (label.key().to_string(), label.value().to_string()))
                .collect();
            self.registered.lock().unwrap().push(CapturedMetric {
                name: key.name().to_string(),
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

    impl metrics::Recorder for CaptureRecorder {
        fn describe_counter(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn describe_gauge(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn describe_histogram(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn register_counter(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> Counter {
            self.push(key);
            Counter::noop()
        }

        fn register_gauge(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Gauge {
            self.push(key);
            metrics::Gauge::noop()
        }

        fn register_histogram(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> Histogram {
            self.push(key);
            Histogram::noop()
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
    fn default_recorder_registers_part1_series_with_labels() {
        let capture = Arc::new(CaptureRecorder::default());
        let recorder = DefaultMetricsRecorder::from_parts(
            Some("driver-a".to_string()),
            vec![("env".to_string(), "prod".to_string())],
            Some(Arc::clone(&capture) as Arc<dyn metrics::Recorder + Send + Sync>),
        );

        let _ = Arc::new(recorder);

        // The pool series are pre-registered too; this test pins part1 only.
        let part1 = expected_metric_names();
        let mut registered: Vec<CapturedMetric> = capture
            .registered()
            .into_iter()
            .filter(|metric| part1.contains(&metric.name.as_str()))
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
            "all part1 series must register unchanged"
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
            Some(Arc::clone(&capture) as Arc<dyn metrics::Recorder + Send + Sync>),
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
            Some(Arc::clone(&capture) as Arc<dyn metrics::Recorder + Send + Sync>),
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
                "ydb_session_pool_acquire_milliseconds",
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
    fn dynamic_series_register_once_per_label_combination() {
        let capture = Arc::new(CaptureRecorder::default());
        let recorder = DefaultMetricsRecorder::with_backend(
            Arc::clone(&capture) as Arc<dyn metrics::Recorder + Send + Sync>
        );

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
        assert_eq!(capture.count("ydb_grpc_request_duration_milliseconds"), 1);
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
        assert_eq!(capture.count("ydb_grpc_request_duration_milliseconds"), 2);

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
            Arc::clone(&capture) as Arc<dyn metrics::Recorder + Send + Sync>
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
        assert_eq!(capture.count("ydb_grpc_request_duration_milliseconds"), 1);
    }

    #[test]
    fn default_recorder_emits_dynamic_series_values_into_backend() {
        let test = Arc::new(crate::client_metrics::test_recorder::TestRecorder::new());
        let recorder: Arc<dyn MetricsRecorder> = Arc::new(DefaultMetricsRecorder::with_backend(
            Arc::clone(&test) as Arc<dyn metrics::Recorder + Send + Sync>,
        ));

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
        assert_eq!(
            test.histogram_count("ydb_grpc_request_duration_milliseconds"),
            2
        );
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
            test.histogram_count("ydb_grpc_connection_establish_milliseconds"),
            1
        );
    }

    #[test]
    fn default_recorder_emits_pool_series_values_into_backend() {
        let test = Arc::new(crate::client_metrics::test_recorder::TestRecorder::new());
        let recorder: Arc<dyn MetricsRecorder> = Arc::new(DefaultMetricsRecorder::with_backend(
            Arc::clone(&test) as Arc<dyn metrics::Recorder + Send + Sync>,
        ));

        recorder.session_pool_acquire(SessionPoolAcquireResult::Ok, Duration::from_millis(3));
        assert_eq!(
            test.counter_value_with_label("ydb_session_pool_acquire_total", "result", "ok"),
            1
        );
        assert_eq!(
            test.histogram_count("ydb_session_pool_acquire_milliseconds"),
            1
        );

        recorder.session_pool_session_created();
        recorder.session_pool_session_create(Duration::from_millis(30));
        assert_eq!(
            test.counter_value("ydb_session_pool_sessions_created_total"),
            1
        );
        assert_eq!(
            test.histogram_count("ydb_session_pool_session_create_milliseconds"),
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
            test.histogram_count("ydb_session_pool_session_use_milliseconds"),
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
    fn default_recorder_with_backend_registers_into_backend() {
        let capture = Arc::new(CaptureRecorder::default());
        let recorder: Arc<dyn MetricsRecorder> = Arc::new(DefaultMetricsRecorder::with_backend(
            Arc::clone(&capture) as Arc<dyn metrics::Recorder + Send + Sync>,
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

    /// Test [`MetricsRecorder`] implementation: counts getter accesses, proving the
    /// trait is object safe and usable through `Arc<dyn MetricsRecorder>`.
    struct CountingRecorder {
        accesses: AtomicUsize,
        counter: Counter,
        histogram: Histogram,
    }

    impl CountingRecorder {
        fn new() -> Self {
            Self {
                accesses: AtomicUsize::new(0),
                counter: Counter::noop(),
                histogram: Histogram::noop(),
            }
        }

        fn counter_getter(&self) -> &Counter {
            self.accesses.fetch_add(1, Ordering::SeqCst);
            &self.counter
        }

        fn histogram_getter(&self) -> &Histogram {
            self.accesses.fetch_add(1, Ordering::SeqCst);
            &self.histogram
        }
    }

    impl MetricsRecorder for CountingRecorder {
        fn client_new_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_new_table_client_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_new_query_client_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_new_scheme_client_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_new_topic_client_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_query_row_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_transaction_query_row_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_transaction_exec_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_transaction_commit_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_transaction_rollback_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_row_query_time_histogram(&self) -> &Histogram {
            self.histogram_getter()
        }

        fn client_transaction_row_query_time_histogram(&self) -> &Histogram {
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
