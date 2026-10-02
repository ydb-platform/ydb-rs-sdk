use std::sync::Arc;
use std::time::Duration;

use metrics::{Counter, Histogram, counter, gauge, histogram};

pub(crate) mod names;
#[cfg(test)]
pub(crate) mod test_recorder;

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
    pub(crate) fn as_label(self) -> &'static str {
        match self {
            Self::Connecting => "connecting",
            Self::Active => "active",
            Self::Failed => "failed",
        }
    }
}

/// Direction of a gRPC stream message (`ydb_grpc_stream_messages_total`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamDirection {
    Sent,
    Received,
}

impl StreamDirection {
    pub(crate) fn as_label(self) -> &'static str {
        match self {
            Self::Sent => "sent",
            Self::Received => "received",
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
}

/// Default [`MetricsRecorder`] implementation.
///
/// Owns the metric handles created from a driver name and static labels (see
/// [`crate::ClientBuilder::with_driver_name`] and
/// [`crate::ClientBuilder::with_metrics_label`]), optionally registering them into a
/// caller-provided `metrics::Recorder` backend which is kept alive here. Metric
/// names, labels, and buckets are an internal detail: components only reach the
/// handles through the [`MetricsRecorder`] getters.
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
        Self {
            names: names::MetricsNames::new(driver_name, extra_labels, backend.as_deref()),
            _backend: backend,
        }
    }
}

impl Default for DefaultMetricsRecorder {
    fn default() -> Self {
        Self::new()
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
        // The facade macros require 'static label values, so borrowed labels are
        // materialized here; the emission macros own them per recorded series.
        let endpoint = endpoint.to_string();
        let state = state.as_label();
        let record = || {
            gauge!("ydb_grpc_connections", "endpoint" => endpoint, "state" => state)
                .increment(delta);
        };
        match self._backend.as_deref() {
            Some(recorder) => metrics::with_local_recorder(recorder, record),
            None => record(),
        }
    }

    fn grpc_connection_establish(&self, endpoint: &str, ok: bool, duration: Duration) {
        let endpoint = endpoint.to_string();
        let result = if ok { "ok" } else { "error" };
        let record = || {
            histogram!("ydb_grpc_connection_establish_milliseconds", "endpoint" => endpoint, "result" => result)
                .record(duration.as_secs_f64() * 1000.0);
        };
        match self._backend.as_deref() {
            Some(recorder) => metrics::with_local_recorder(recorder, record),
            None => record(),
        }
    }

    fn grpc_request(
        &self,
        endpoint: &str,
        service: &str,
        method: &str,
        grpc_code: &str,
        duration: Duration,
    ) {
        let endpoint = endpoint.to_string();
        let service = service.to_string();
        let method = method.to_string();
        let grpc_code = grpc_code.to_string();
        let record = || {
            counter!("ydb_grpc_requests_total", "endpoint" => endpoint.clone(), "service" => service.clone(), "method" => method.clone(), "grpc_code" => grpc_code.clone())
                .increment(1);
            histogram!("ydb_grpc_request_duration_milliseconds", "endpoint" => endpoint.clone(), "service" => service.clone(), "method" => method.clone())
                .record(duration.as_secs_f64() * 1000.0);
            if grpc_code != "ok" {
                counter!("ydb_grpc_errors_total", "endpoint" => endpoint, "grpc_code" => grpc_code)
                    .increment(1);
            }
        };
        match self._backend.as_deref() {
            Some(recorder) => metrics::with_local_recorder(recorder, record),
            None => record(),
        }
    }

    fn grpc_stream_message(
        &self,
        endpoint: &str,
        service: &str,
        method: &str,
        direction: StreamDirection,
    ) {
        let endpoint = endpoint.to_string();
        let service = service.to_string();
        let method = method.to_string();
        let direction = direction.as_label();
        let record = || {
            counter!("ydb_grpc_stream_messages_total", "endpoint" => endpoint, "service" => service, "method" => method, "direction" => direction)
                .increment(1);
        };
        match self._backend.as_deref() {
            Some(recorder) => metrics::with_local_recorder(recorder, record),
            None => record(),
        }
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
            _key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Gauge {
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

        let mut registered = capture.registered();
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
                metric.labels,
                vec![("driver_name".to_string(), "main".to_string())],
                "metric {} must default to driver_name=main",
                metric.name
            );
        }
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
