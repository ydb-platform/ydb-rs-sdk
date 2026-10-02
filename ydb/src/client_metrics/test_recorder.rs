//! Stateful in-memory [`metrics::Recorder`] for unit tests.
//!
//! Unlike the capture-only recorder used in [`crate::client_metrics`] registration
//! tests, this recorder keeps per-key state: counter totals, gauge values, and
//! histogram observations, so unit tests can assert recorded values and labels.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Per-key metric state shared between the recorder and emitted handles.
#[derive(Default)]
struct MetricState {
    counter: u64,
    gauge: f64,
    observations: Vec<f64>,
}

#[derive(Default)]
struct Inner {
    states: Mutex<HashMap<metrics::Key, Arc<Mutex<MetricState>>>>,
    /// Insertion order of keys, for label assertions on the latest series.
    order: Mutex<Vec<metrics::Key>>,
}

impl Inner {
    fn state_for(&self, key: &metrics::Key) -> Arc<Mutex<MetricState>> {
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

struct CounterHandle(Arc<Mutex<MetricState>>);

impl metrics::CounterFn for CounterHandle {
    fn increment(&self, value: u64) {
        self.0.lock().unwrap().counter += value;
    }

    fn absolute(&self, value: u64) {
        self.0.lock().unwrap().counter = value;
    }
}

struct GaugeHandle(Arc<Mutex<MetricState>>);

impl metrics::GaugeFn for GaugeHandle {
    fn increment(&self, value: f64) {
        self.0.lock().unwrap().gauge += value;
    }

    fn decrement(&self, value: f64) {
        self.0.lock().unwrap().gauge -= value;
    }

    fn set(&self, value: f64) {
        self.0.lock().unwrap().gauge = value;
    }
}

struct HistogramHandle(Arc<Mutex<MetricState>>);

impl metrics::HistogramFn for HistogramHandle {
    fn record(&self, value: f64) {
        self.0.lock().unwrap().observations.push(value);
    }
}

/// No-op handles backing the client-metric getters, which tests never assert on.
#[derive(Clone)]
struct NoopHandles {
    counter: metrics::Counter,
    histogram: metrics::Histogram,
}

/// In-memory recorder capturing counters, gauges, and histograms by key.
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
                counter: metrics::Counter::noop(),
                histogram: metrics::Histogram::noop(),
            },
        }
    }
}

impl TestRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    fn state_for(&self, key: &metrics::Key) -> Arc<Mutex<MetricState>> {
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
        self.state_for(&key).lock().unwrap().gauge += delta;
    }

    fn last_key(&self, name: &str) -> Option<metrics::Key> {
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

    /// All recorded observations for `name`, over all label combinations.
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

    /// Gauge value for `name` with a specific label value, e.g. `state=active`.
    pub fn gauge_value_with_label(&self, name: &str, label: &str, value: &str) -> f64 {
        self.inner
            .states
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.name() == name && labels_contain(key.labels(), label, value))
            .map(|(_, state)| state.lock().unwrap().gauge)
            .sum()
    }

    /// Label value of the most recently recorded series of `name`.
    pub fn label_of_last(&self, name: &str, label: &str) -> Option<String> {
        let key = self.last_key(name)?;
        key.labels()
            .find(|item| item.key() == label)
            .map(|item| item.value().to_string())
    }

    /// All keys recorded for `name` (used for cardinality assertions).
    #[allow(dead_code)]
    pub fn keys(&self, name: &str) -> Vec<metrics::Key> {
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

fn owned_key(name: &str, labels: &[(&str, &str)]) -> metrics::Key {
    let owned: Vec<metrics::Label> = labels
        .iter()
        .map(|(key, value)| metrics::Label::new((*key).to_string(), (*value).to_string()))
        .collect();
    metrics::Key::from_parts(name.to_string(), owned)
}

fn labels_contain<'a>(
    mut labels: impl Iterator<Item = &'a metrics::Label>,
    name: &str,
    value: &str,
) -> bool {
    labels.any(|label| label.key() == name && label.value() == value)
}

/// Facade backend path: handles created through the emission macros write into the
/// same per-key state as the direct [`MetricsRecorder`] implementation below.
impl metrics::Recorder for TestRecorder {
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
    ) -> metrics::Counter {
        metrics::Counter::from_arc(Arc::new(CounterHandle(self.state_for(key))))
    }

    fn register_gauge(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Gauge {
        metrics::Gauge::from_arc(Arc::new(GaugeHandle(self.state_for(key))))
    }

    fn register_histogram(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::from_arc(Arc::new(HistogramHandle(self.state_for(key))))
    }
}

impl crate::client_metrics::MetricsRecorder for TestRecorder {
    fn client_new_counter(&self) -> &metrics::Counter {
        &self.noop.counter
    }

    fn client_new_table_client_counter(&self) -> &metrics::Counter {
        &self.noop.counter
    }

    fn client_new_query_client_counter(&self) -> &metrics::Counter {
        &self.noop.counter
    }

    fn client_new_scheme_client_counter(&self) -> &metrics::Counter {
        &self.noop.counter
    }

    fn client_new_topic_client_counter(&self) -> &metrics::Counter {
        &self.noop.counter
    }

    fn client_query_row_counter(&self) -> &metrics::Counter {
        &self.noop.counter
    }

    fn client_transaction_query_row_counter(&self) -> &metrics::Counter {
        &self.noop.counter
    }

    fn client_transaction_exec_counter(&self) -> &metrics::Counter {
        &self.noop.counter
    }

    fn client_transaction_commit_counter(&self) -> &metrics::Counter {
        &self.noop.counter
    }

    fn client_transaction_rollback_counter(&self) -> &metrics::Counter {
        &self.noop.counter
    }

    fn client_row_query_time_histogram(&self) -> &metrics::Histogram {
        &self.noop.histogram
    }

    fn client_transaction_row_query_time_histogram(&self) -> &metrics::Histogram {
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
            "ydb_grpc_connection_establish_milliseconds",
            &[
                ("endpoint", endpoint),
                ("result", if ok { "ok" } else { "error" }),
            ],
            duration.as_secs_f64() * 1000.0,
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
            "ydb_grpc_request_duration_milliseconds",
            &[
                ("endpoint", endpoint),
                ("service", service),
                ("method", method),
            ],
            duration.as_secs_f64() * 1000.0,
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
}
