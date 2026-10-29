//! Shared helpers for mock-server metric tests: isolated recorder construction,
//! client wiring, and registry assertions.
//!
//! Every test builds its own `prometheus::Registry` + `metrics_prometheus::Recorder`
//! and injects it via `ClientBuilder::with_metrics_recorder`, so tests run in
//! parallel and never touch the process-global recorder.

use std::sync::Arc;
use std::time::Duration;

use metrics::Recorder as _;
use prometheus::{Registry, proto::MetricFamily};
use ydb::{
    Client, ClientBuilder, Counter, Gauge, Histogram, MetricDescription, MetricLabel, MetricUnit,
    MetricsBackend, YdbResult,
};

use crate::mock_server::server::MockServer;

pub const DATABASE: &str = "/local";

/// Assertion polling deadline for asynchronous emissions (watchers, background
/// release, stream delivery). One short sleep inside `wait_for` expresses the
/// poll interval only; the deadline bounds the total wait.
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Poll `condition` until it returns `true` or the deadline expires.
pub async fn wait_for(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(WAIT_TIMEOUT, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition was not met before the deadline");
}

/// Isolated recorder: a private registry plus the recorder bound to it.
pub fn test_recorder() -> (Registry, metrics_prometheus::Recorder) {
    let registry = Registry::new();
    let recorder = metrics_prometheus::Recorder::builder()
        .with_registry(&registry)
        .build();
    (registry, recorder)
}

pub struct PrometheusBackend(metrics_prometheus::Recorder);

struct CounterHandle(metrics::Counter);

impl Counter for CounterHandle {
    fn increment(&self, value: u64) {
        self.0.increment(value);
    }
}

struct GaugeHandle(metrics::Gauge);

impl Gauge for GaugeHandle {
    fn increment(&self, value: f64) {
        self.0.increment(value);
    }

    fn decrement(&self, value: f64) {
        self.0.decrement(value);
    }

    fn set(&self, value: f64) {
        self.0.set(value);
    }
}

struct HistogramHandle(metrics::Histogram);

impl Histogram for HistogramHandle {
    fn record(&self, value: f64) {
        self.0.record(value);
    }
}

fn unit(unit: MetricUnit) -> Option<metrics::Unit> {
    match unit {
        MetricUnit::Count => Some(metrics::Unit::Count),
        MetricUnit::Milliseconds => Some(metrics::Unit::Milliseconds),
        MetricUnit::Seconds => Some(metrics::Unit::Seconds),
        MetricUnit::Bytes => Some(metrics::Unit::Bytes),
        _ => panic!("unsupported metric unit in the test backend"),
    }
}

fn key(description: &MetricDescription, labels: &[MetricLabel]) -> metrics::Key {
    metrics::Key::from_parts(
        description.name().to_string(),
        labels
            .iter()
            .map(|label| metrics::Label::new(label.key().to_string(), label.value().to_string()))
            .collect::<Vec<_>>(),
    )
}

impl MetricsBackend for PrometheusBackend {
    fn register_counter(
        &self,
        description: &MetricDescription,
        labels: &[MetricLabel],
    ) -> Arc<dyn Counter> {
        self.0.describe_counter(
            metrics::KeyName::from(description.name().to_string()),
            unit(description.unit()),
            metrics::SharedString::from(description.description().to_string()),
        );
        Arc::new(CounterHandle(self.0.register_counter(
            &key(description, labels),
            &metrics::Metadata::new(module_path!(), metrics::Level::INFO, None),
        )))
    }

    fn register_gauge(
        &self,
        description: &MetricDescription,
        labels: &[MetricLabel],
    ) -> Arc<dyn Gauge> {
        self.0.describe_gauge(
            metrics::KeyName::from(description.name().to_string()),
            unit(description.unit()),
            metrics::SharedString::from(description.description().to_string()),
        );
        Arc::new(GaugeHandle(self.0.register_gauge(
            &key(description, labels),
            &metrics::Metadata::new(module_path!(), metrics::Level::INFO, None),
        )))
    }

    fn register_histogram(
        &self,
        description: &MetricDescription,
        labels: &[MetricLabel],
    ) -> Arc<dyn Histogram> {
        self.0.describe_histogram(
            metrics::KeyName::from(description.name().to_string()),
            unit(description.unit()),
            metrics::SharedString::from(description.description().to_string()),
        );
        Arc::new(HistogramHandle(self.0.register_histogram(
            &key(description, labels),
            &metrics::Metadata::new(module_path!(), metrics::Level::INFO, None),
        )))
    }
}

/// Adapter for `ClientBuilder::with_metrics_recorder`.
pub fn backend(recorder: metrics_prometheus::Recorder) -> PrometheusBackend {
    PrometheusBackend(recorder)
}

/// Client against `server` with `use_discovery=false` and the given recorder.
pub async fn make_client(
    server: &MockServer,
    recorder: metrics_prometheus::Recorder,
) -> YdbResult<Client> {
    make_client_with_labels(server, recorder, Vec::<(String, String)>::new()).await
}

/// Like [`make_client`], plus custom static metric labels.
pub async fn make_client_with_labels<I, K, V>(
    server: &MockServer,
    recorder: metrics_prometheus::Recorder,
    labels: I,
) -> YdbResult<Client>
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .with_metrics_labels(labels)
    .with_metrics_recorder(backend(recorder))
    .build()
    .await
}

/// gRPC authority label of the mock server (`server.endpoint()` without the
/// `grpc://` scheme), as emitted by the transport metrics interceptor.
pub fn endpoint_authority(server: &MockServer) -> &str {
    server
        .endpoint()
        .strip_prefix("grpc://")
        .expect("mock server endpoint always carries the grpc:// scheme")
}

fn metric_family<'a>(gathered: &'a [MetricFamily], name: &str) -> Option<&'a MetricFamily> {
    gathered.iter().find(|mf| mf.name() == name)
}

fn has_label(metric: &prometheus::proto::Metric, key: &str, value: &str) -> bool {
    metric
        .label
        .iter()
        .any(|label| label.name() == key && label.value() == value)
}

/// Sum of a counter across all its series; 0 when the metric is absent.
pub fn counter_value(registry: &Registry, name: &str) -> u64 {
    metric_family(&registry.gather(), name)
        .map(|mf| mf.metric.iter().map(|m| m.counter.value() as u64).sum())
        .unwrap_or(0)
}

/// A label value from the first series of a counter; `None` when absent.
pub fn counter_label_value(registry: &Registry, name: &str, key: &str) -> Option<String> {
    metric_family(&registry.gather(), name)
        .and_then(|mf| mf.metric.first())
        .and_then(|m| {
            m.label
                .iter()
                .find(|l| l.name() == key)
                .map(|l| l.value().to_string())
        })
}

/// Sum of a counter over the series carrying `key=value`.
pub fn counter_value_with_label(registry: &Registry, name: &str, key: &str, value: &str) -> u64 {
    metric_family(&registry.gather(), name)
        .map(|mf| {
            mf.metric
                .iter()
                .filter(|m| has_label(m, key, value))
                .map(|m| m.counter.value() as u64)
                .sum()
        })
        .unwrap_or(0)
}

/// Sample count of a histogram summed over all its series; 0 when absent.
pub fn histogram_sample_count(registry: &Registry, name: &str) -> u64 {
    metric_family(&registry.gather(), name)
        .map(|mf| mf.metric.iter().map(|m| m.histogram.sample_count()).sum())
        .unwrap_or(0)
}

/// Sample count of a histogram over the series carrying `key=value`.
pub fn histogram_sample_count_with_label(
    registry: &Registry,
    name: &str,
    key: &str,
    value: &str,
) -> u64 {
    metric_family(&registry.gather(), name)
        .map(|mf| {
            mf.metric
                .iter()
                .filter(|m| has_label(m, key, value))
                .map(|m| m.histogram.sample_count())
                .sum()
        })
        .unwrap_or(0)
}

/// Sum of observations of a histogram over all its series; 0.0 when absent.
pub fn histogram_sum(registry: &Registry, name: &str) -> f64 {
    metric_family(&registry.gather(), name)
        .map(|mf| mf.metric.iter().map(|m| m.histogram.sample_sum()).sum())
        .unwrap_or(0.0)
}

/// Current value of a gauge over the series carrying `key=value`; 0.0 when absent.
pub fn gauge_value_with_label(registry: &Registry, name: &str, key: &str, value: &str) -> f64 {
    metric_family(&registry.gather(), name)
        .and_then(|mf| {
            mf.metric
                .iter()
                .find(|m| has_label(m, key, value))
                .map(|m| m.gauge.value())
        })
        .unwrap_or(0.0)
}

/// Current value of the first gauge series of a metric; 0.0 when absent.
pub fn gauge_value(registry: &Registry, name: &str) -> f64 {
    metric_family(&registry.gather(), name)
        .and_then(|mf| mf.metric.first())
        .map(|m| m.gauge.value())
        .unwrap_or(0.0)
}

/// Whether the metric is registered with at least one series.
pub fn metric_present(registry: &Registry, name: &str) -> bool {
    metric_family(&registry.gather(), name).is_some()
}

/// Whether the metric has a series carrying `key=value`.
pub fn metric_series_present(registry: &Registry, name: &str, key: &str, value: &str) -> bool {
    metric_family(&registry.gather(), name)
        .map(|mf| mf.metric.iter().any(|m| has_label(m, key, value)))
        .unwrap_or(false)
}
