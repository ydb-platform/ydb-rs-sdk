//! CPU cost of dynamic metric series access: backend registration on every
//! emission versus cached SDK counter handles.
//!
//! Both paths emit the same counter series — `ydb_grpc_stream_messages_total`
//! with `driver_name + endpoint + service + method + direction` labels — into
//! the same minimal in-memory [`MetricsBackend`], round-robin over a growing
//! number of unique label combinations (unique endpoints).
//!
//! * `register_per_emission` — every emission materializes owned label values
//!   and calls `register_counter` on the backend.
//! * `cached_handles` — one counter handle per unique endpoint is registered
//!   up front and retained in a map; each emission performs only the map lookup
//!   and atomic increment. The backend sees exactly one registration per unique
//!   label combination (asserted below before timing).
//!
//! The backend hands out atomic-backed counters behind a map lookup, modeling
//! what an exporter does per `register_counter`; per-increment work is identical
//! for both paths, so the measured difference is the access strategy. The
//! benchmark uses only the public backend API and does not expose SDK-internal
//! recorder types.
//!
//! Run: `cargo bench -p ydb --bench metrics_dynamic_cache`

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ydb::{Counter, Gauge, Histogram, MetricDescription, MetricLabel, MetricUnit, MetricsBackend};

const METRIC_NAME: &str = "ydb_grpc_stream_messages_total";
const DRIVER_NAME: &str = "bench";
const SERVICE: &str = "ydb.api.v1.QueryService";
const METHOD: &str = "ExecuteQuery";
const UNIQUE_LABEL_COUNTS: [usize; 5] = [100, 200, 500, 1000, 5000];

fn unique_endpoints(count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("vm-{index:05}.bench.ydb:2135"))
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct BenchKey {
    name: String,
    labels: Vec<(String, String)>,
}

impl BenchKey {
    fn new(description: &MetricDescription, labels: &[MetricLabel]) -> Self {
        Self {
            name: description.name().to_string(),
            labels: labels
                .iter()
                .map(|label| (label.key().to_string(), label.value().to_string()))
                .collect(),
        }
    }
}

struct CounterState(Arc<AtomicU64>);

impl Counter for CounterState {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }
}

struct NoopHandle;

impl Gauge for NoopHandle {
    fn increment(&self, _value: f64) {}

    fn decrement(&self, _value: f64) {}

    fn set(&self, _value: f64) {}
}

impl Histogram for NoopHandle {
    fn record(&self, _value: f64) {}
}

/// Minimal in-memory backend: one atomic counter per key.
#[derive(Default)]
struct BenchBackend {
    counters: RwLock<HashMap<BenchKey, Arc<AtomicU64>>>,
    stream_registrations: AtomicUsize,
}

impl BenchBackend {
    fn stream_registrations(&self) -> usize {
        self.stream_registrations.load(Ordering::Relaxed)
    }

    fn shared_counter(
        &self,
        description: &MetricDescription,
        labels: &[MetricLabel],
    ) -> Arc<dyn Counter> {
        let key = BenchKey::new(description, labels);
        if let Some(state) = self.counters.read().unwrap().get(&key).cloned() {
            return Arc::new(CounterState(state));
        }
        let state = self
            .counters
            .write()
            .unwrap()
            .entry(key)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        Arc::new(CounterState(state))
    }
}

impl MetricsBackend for BenchBackend {
    fn register_counter(
        &self,
        description: &MetricDescription,
        labels: &[MetricLabel],
    ) -> Arc<dyn Counter> {
        if description.name() == METRIC_NAME {
            self.stream_registrations.fetch_add(1, Ordering::Relaxed);
        }
        self.shared_counter(description, labels)
    }

    fn register_gauge(
        &self,
        _description: &MetricDescription,
        _labels: &[MetricLabel],
    ) -> Arc<dyn Gauge> {
        Arc::new(NoopHandle)
    }

    fn register_histogram(
        &self,
        _description: &MetricDescription,
        _labels: &[MetricLabel],
    ) -> Arc<dyn Histogram> {
        Arc::new(NoopHandle)
    }
}

fn labels(endpoint: &str, service: &str, method: &str) -> Vec<MetricLabel> {
    vec![
        MetricLabel::new("driver_name", DRIVER_NAME),
        MetricLabel::new("endpoint", endpoint.to_string()),
        MetricLabel::new("service", service.to_string()),
        MetricLabel::new("method", method.to_string()),
        MetricLabel::new("direction", "sent"),
    ]
}

fn bench_counter_emission(c: &mut Criterion) {
    let mut group = c.benchmark_group("counter_emission");
    group
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .sample_size(50);

    for &unique in &UNIQUE_LABEL_COUNTS {
        let endpoints = unique_endpoints(unique);
        let service = SERVICE.to_string();
        let method = METHOD.to_string();
        let description = MetricDescription::new(
            METRIC_NAME,
            MetricUnit::Count,
            "Number of gRPC stream messages.",
        );
        group.throughput(Throughput::Elements(unique as u64));

        let registering_backend = BenchBackend::default();
        group.bench_function(
            format!("register_per_emission/unique_labels={unique}"),
            |b| {
                b.iter(|| {
                    for endpoint in &endpoints {
                        registering_backend
                            .register_counter(&description, &labels(endpoint, &service, &method))
                            .increment(1);
                    }
                })
            },
        );

        let cached_backend = BenchBackend::default();
        let handles: HashMap<&str, Arc<dyn Counter>> = endpoints
            .iter()
            .map(|endpoint| {
                (
                    endpoint.as_str(),
                    cached_backend
                        .register_counter(&description, &labels(endpoint, &service, &method)),
                )
            })
            .collect();
        assert_eq!(
            cached_backend.stream_registrations(),
            unique,
            "cached access must register each series exactly once"
        );

        group.bench_function(format!("cached_handles/unique_labels={unique}"), |b| {
            b.iter(|| {
                for endpoint in &endpoints {
                    handles[endpoint.as_str()].increment(1);
                }
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_counter_emission);
criterion_main!(benches);
