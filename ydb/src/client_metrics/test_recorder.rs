//! Stateful in-memory [`MetricsBackend`] for unit tests.
//!
//! The recorder keeps per-key state: counter totals, gauge values, and
//! histogram observations, so unit tests can assert recorded values and labels
//! without depending on the external `metrics` crate.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::backend::{
    Counter, CounterHandle, Gauge, Histogram, HistogramHandle, MetricDescription, MetricLabel,
    MetricsBackend, noop_counter, noop_histogram,
};

/// Per-key metric state shared between the recorder and emitted handles.
#[derive(Default)]
struct MetricState {
    counter: u64,
    gauge: f64,
    gauge_observations: Vec<f64>,
    observations: Vec<f64>,
}

impl MetricState {
    fn change_gauge(&mut self, delta: f64) {
        self.gauge += delta;
        self.gauge_observations.push(self.gauge);
    }

    fn set_gauge(&mut self, value: f64) {
        self.gauge = value;
        self.gauge_observations.push(self.gauge);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TestKey {
    name: String,
    labels: Vec<(String, String)>,
}

impl TestKey {
    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Default)]
struct Inner {
    states: Mutex<HashMap<TestKey, Arc<Mutex<MetricState>>>>,
    /// Insertion order of keys, for label assertions on the latest series.
    order: Mutex<Vec<TestKey>>,
}

impl Inner {
    fn state_for(&self, key: &TestKey) -> Arc<Mutex<MetricState>> {
        let mut states = self.states.lock().unwrap();
        states
            .entry(key.clone())
            .or_insert_with(|| {
                self.order.lock().unwrap().push(key.clone());
                Arc::new(Mutex::new(MetricState::default()))
            })
            .clone()
    }
}

struct CounterHandleImpl(Arc<Mutex<MetricState>>);

impl Counter for CounterHandleImpl {
    fn increment(&self, value: u64) {
        self.0.lock().unwrap().counter += value;
    }
}

struct GaugeHandleImpl(Arc<Mutex<MetricState>>);

impl Gauge for GaugeHandleImpl {
    fn increment(&self, value: f64) {
        self.0.lock().unwrap().change_gauge(value);
    }

    fn decrement(&self, value: f64) {
        self.0.lock().unwrap().change_gauge(-value);
    }

    fn set(&self, value: f64) {
        self.0.lock().unwrap().set_gauge(value);
    }
}

struct HistogramHandleImpl(Arc<Mutex<MetricState>>);

impl Histogram for HistogramHandleImpl {
    fn record(&self, value: f64) {
        self.0.lock().unwrap().observations.push(value);
    }
}

/// No-op handles backing the client-metric getters, which tests never assert on.
#[derive(Clone)]
struct NoopHandles {
    counter: CounterHandle,
    histogram: HistogramHandle,
}

/// In-memory backend capturing counters, gauges, and histograms by key.
#[derive(Clone)]
pub struct TestRecorder {
    inner: Arc<Inner>,
    noop: NoopHandles,
}

impl Default for TestRecorder {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
            noop: NoopHandles {
                counter: noop_counter(),
                histogram: noop_histogram(),
            },
        }
    }
}

impl TestRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    fn state_for(&self, key: &TestKey) -> Arc<Mutex<MetricState>> {
        self.inner.state_for(key)
    }

    fn increment_counter_key(&self, name: &str, labels: &[(&str, &str)], value: u64) {
        let key = owned_key(name, labels);
        self.state_for(&key).lock().unwrap().counter += value;
    }

    fn record_observation(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        let key = owned_key(name, labels);
        self.state_for(&key)
            .lock()
            .unwrap()
            .observations
            .push(value);
    }

    fn change_gauge_key(&self, name: &str, labels: &[(&str, &str)], delta: f64) {
        let key = owned_key(name, labels);
        self.state_for(&key).lock().unwrap().change_gauge(delta);
    }

    fn last_key(&self, name: &str) -> Option<TestKey> {
        self.inner
            .order
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|key| key.name() == name)
            .cloned()
    }

    /// Total recorded for `name`, summed over all label combinations.
    pub fn counter_value(&self, name: &str) -> u64 {
        self.inner
            .states
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.name() == name)
            .map(|(_, state)| state.lock().unwrap().counter)
            .sum()
    }

    /// Counter total for `name` with a specific label value, e.g. `result=ok`.
    pub fn counter_value_with_label(&self, name: &str, label: &str, value: &str) -> u64 {
        self.inner
            .states
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.name() == name && labels_contain(key, label, value))
            .map(|(_, state)| state.lock().unwrap().counter)
            .sum()
    }

    /// Recorded histogram observation count for `name`, over all label combinations.
    pub fn histogram_count(&self, name: &str) -> usize {
        self.inner
            .states
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.name() == name)
            .map(|(_, state)| state.lock().unwrap().observations.len())
            .sum()
    }

    /// All recorded observations for `name`.
    pub fn histogram_observations(&self, name: &str) -> Vec<f64> {
        self.inner
            .states
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.name() == name)
            .flat_map(|(_, state)| state.lock().unwrap().observations.clone())
            .collect()
    }

    /// Current gauge value for `name`, summed over all label combinations.
    #[allow(dead_code)]
    pub fn gauge_value(&self, name: &str) -> f64 {
        self.inner
            .states
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.name() == name)
            .map(|(_, state)| state.lock().unwrap().gauge)
            .sum()
    }

    /// Gauge values observed after every change, across all label combinations.
    pub fn gauge_observations(&self, name: &str) -> Vec<f64> {
        self.inner
            .states
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.name() == name)
            .flat_map(|(_, state)| state.lock().unwrap().gauge_observations.clone())
            .collect()
    }

    /// Gauge value for `name` with a specific label value, e.g. `state=active`.
    pub fn gauge_value_with_label(&self, name: &str, label: &str, value: &str) -> f64 {
        self.inner
            .states
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.name() == name && labels_contain(key, label, value))
            .map(|(_, state)| state.lock().unwrap().gauge)
            .sum()
    }

    /// Label value of the most recently recorded series of `name`.
    pub fn label_of_last(&self, name: &str, label: &str) -> Option<String> {
        let key = self.last_key(name)?;
        key.labels
            .iter()
            .find(|(key, _)| key == label)
            .map(|(_, value)| value.clone())
    }

    /// All keys recorded for `name` (used for cardinality assertions).
    #[allow(dead_code)]
    pub fn keys(&self, name: &str) -> Vec<TestKey> {
        self.inner
            .order
            .lock()
            .unwrap()
            .iter()
            .filter(|key| key.name() == name)
            .cloned()
            .collect()
    }
}

fn backend_key(description: &MetricDescription, labels: &[MetricLabel]) -> TestKey {
    TestKey {
        name: description.name().to_string(),
        labels: labels
            .iter()
            .map(|label| (label.key().to_string(), label.value().to_string()))
            .collect(),
    }
}

fn owned_key(name: &str, labels: &[(&str, &str)]) -> TestKey {
    TestKey {
        name: name.to_string(),
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
    }
}

fn labels_contain(key: &TestKey, name: &str, value: &str) -> bool {
    key.labels
        .iter()
        .any(|(key, current)| key == name && current == value)
}

impl MetricsBackend for TestRecorder {
    fn register_counter(
        &self,
        description: &MetricDescription,
        labels: &[MetricLabel],
    ) -> Arc<dyn Counter> {
        Arc::new(CounterHandleImpl(
            self.state_for(&backend_key(description, labels)),
        ))
    }

    fn register_gauge(
        &self,
        description: &MetricDescription,
        labels: &[MetricLabel],
    ) -> Arc<dyn Gauge> {
        Arc::new(GaugeHandleImpl(
            self.state_for(&backend_key(description, labels)),
        ))
    }

    fn register_histogram(
        &self,
        description: &MetricDescription,
        labels: &[MetricLabel],
    ) -> Arc<dyn Histogram> {
        Arc::new(HistogramHandleImpl(
            self.state_for(&backend_key(description, labels)),
        ))
    }
}

impl crate::client_metrics::MetricsRecorder for TestRecorder {
    fn client_new_counter(&self) -> &CounterHandle {
        &self.noop.counter
    }

    fn client_new_table_client_counter(&self) -> &CounterHandle {
        &self.noop.counter
    }

    fn client_new_query_client_counter(&self) -> &CounterHandle {
        &self.noop.counter
    }

    fn client_new_scheme_client_counter(&self) -> &CounterHandle {
        &self.noop.counter
    }

    fn client_new_topic_client_counter(&self) -> &CounterHandle {
        &self.noop.counter
    }

    fn client_query_row_counter(&self) -> &CounterHandle {
        &self.noop.counter
    }

    fn client_transaction_query_row_counter(&self) -> &CounterHandle {
        &self.noop.counter
    }

    fn client_transaction_exec_counter(&self) -> &CounterHandle {
        &self.noop.counter
    }

    fn client_transaction_commit_counter(&self) -> &CounterHandle {
        &self.noop.counter
    }

    fn client_transaction_rollback_counter(&self) -> &CounterHandle {
        &self.noop.counter
    }

    fn client_row_query_time_histogram(&self) -> &HistogramHandle {
        &self.noop.histogram
    }

    fn client_transaction_row_query_time_histogram(&self) -> &HistogramHandle {
        &self.noop.histogram
    }

    fn grpc_connections_add(
        &self,
        endpoint: &str,
        state: crate::client_metrics::GrpcConnectionState,
        delta: f64,
    ) {
        self.change_gauge_key(
            "ydb_grpc_connections",
            &[("endpoint", endpoint), ("state", state.as_label())],
            delta,
        );
    }

    fn grpc_connection_establish(&self, endpoint: &str, ok: bool, duration: Duration) {
        self.record_observation(
            "ydb_grpc_connection_establish_seconds",
            &[
                ("endpoint", endpoint),
                ("result", if ok { "ok" } else { "error" }),
            ],
            duration.as_secs_f64(),
        );
    }

    fn grpc_request(
        &self,
        endpoint: &str,
        service: &str,
        method: &str,
        grpc_code: &str,
        duration: Duration,
    ) {
        self.increment_counter_key(
            "ydb_grpc_requests_total",
            &[
                ("endpoint", endpoint),
                ("service", service),
                ("method", method),
                ("grpc_code", grpc_code),
            ],
            1,
        );
        self.record_observation(
            "ydb_grpc_request_duration_seconds",
            &[
                ("endpoint", endpoint),
                ("service", service),
                ("method", method),
            ],
            duration.as_secs_f64(),
        );
        if grpc_code != "ok" {
            self.increment_counter_key(
                "ydb_grpc_errors_total",
                &[("endpoint", endpoint), ("grpc_code", grpc_code)],
                1,
            );
        }
    }

    fn grpc_stream_message(
        &self,
        endpoint: &str,
        service: &str,
        method: &str,
        direction: crate::client_metrics::StreamDirection,
    ) {
        self.increment_counter_key(
            "ydb_grpc_stream_messages_total",
            &[
                ("endpoint", endpoint),
                ("service", service),
                ("method", method),
                ("direction", direction.as_label()),
            ],
            1,
        );
    }

    fn session_pool_gauges(
        &self,
        previous: crate::client_metrics::SessionPoolGaugeSnapshot,
        current: crate::client_metrics::SessionPoolGaugeSnapshot,
    ) {
        self.change_gauge_key(
            "ydb_session_pool_sessions",
            &[("state", "idle")],
            current.idle - previous.idle,
        );
        self.change_gauge_key(
            "ydb_session_pool_sessions",
            &[("state", "active")],
            current.active - previous.active,
        );
        self.change_gauge_key(
            "ydb_session_pool_sessions",
            &[("state", "creating")],
            current.creating - previous.creating,
        );
        self.change_gauge_key(
            "ydb_session_pool_size_limit",
            &[],
            current.limit - previous.limit,
        );
        self.change_gauge_key(
            "ydb_session_pool_pending_requests",
            &[],
            current.pending - previous.pending,
        );
    }

    fn session_pool_acquire(
        &self,
        result: crate::client_metrics::SessionPoolAcquireResult,
        duration: Duration,
    ) {
        self.increment_counter_key(
            "ydb_session_pool_acquire_total",
            &[("result", result.as_label())],
            1,
        );
        self.record_observation(
            "ydb_session_pool_acquire_seconds",
            &[("result", result.as_label())],
            duration.as_secs_f64(),
        );
    }

    fn session_pool_session_create(&self, duration: Duration) {
        self.record_observation(
            "ydb_session_pool_session_create_seconds",
            &[],
            duration.as_secs_f64(),
        );
    }

    fn session_pool_session_created(&self) {
        self.increment_counter_key("ydb_session_pool_sessions_created_total", &[], 1);
    }

    fn session_pool_session_closed(&self, reason: crate::client_metrics::SessionPoolCloseReason) {
        self.increment_counter_key(
            "ydb_session_pool_sessions_closed_total",
            &[("reason", reason.as_label())],
            1,
        );
    }

    fn session_pool_session_use(&self, duration: Duration) {
        self.record_observation(
            "ydb_session_pool_session_use_seconds",
            &[],
            duration.as_secs_f64(),
        );
    }

    fn session_pool_attach_stream_closed(
        &self,
        reason: crate::client_metrics::SessionPoolAttachStreamCloseReason,
    ) {
        self.increment_counter_key(
            "ydb_session_pool_attach_stream_closed_total",
            &[("reason", reason.as_label())],
            1,
        );
    }
}
