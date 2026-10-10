//! CPU cost of dynamic metric series access: per-call `counter!` facade
//! emission vs the SDK's interned handle caches.
//!
//! Both paths emit the same counter series — `ydb_grpc_stream_messages_total`
//! with `driver_name + endpoint + service + method + direction` labels — into
//! the same kind of minimal in-memory backend, round-robin over a growing
//! number of unique label combinations (unique endpoints).
//!
//! * `facade_macro` — the pre-cache approach: every emission materializes
//!   owned label values, builds a `metrics::Key`, and calls `register_counter`
//!   on the backend. Macros resolve through the ambient global recorder, the
//!   cheapest pre-cache variant (a caller-provided backend additionally paid
//!   `with_local_recorder` per emission).
//! * `interned_cache` — `DefaultMetricsRecorder` with the same kind of
//!   backend: `grpc_stream_message` interns the label strings and increments
//!   a cached handle; the backend sees exactly one registration per unique
//!   label combination (asserted below before timing).
//!
//! The backend hands out atomic-backed counters behind a map lookup, modeling
//! what any real exporter does per `register_counter`; per-increment work is
//! identical for both paths, so the measured difference is the access
//! strategy.
//!
//! Reference run (2026-10-07, aarch64 Apple Silicon, release, criterion 0.8;
//! mean time per single emission, i.e. routine time divided by the number of
//! unique labels — machine-specific, indicative only):
//!
//! ```text
//! unique labels   facade_macro   interned_cache   speedup
//! 100             160.8 ns       53.8 ns          2.99x
//! 200             155.8 ns       53.0 ns          2.94x
//! 500             149.2 ns       53.0 ns          2.81x
//! 1000            152.3 ns       52.8 ns          2.88x
//! 5000            151.5 ns       54.0 ns          2.81x
//! ```
//!
//! Run: `cargo bench -p ydb --bench metrics_dynamic_cache`

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use metrics::{CounterFn, Key, Recorder, counter};
use ydb::{DefaultMetricsRecorder, MetricsRecorder, StreamDirection};

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

struct CounterState(Arc<AtomicU64>);

impl CounterFn for CounterState {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }

    fn absolute(&self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }
}

/// Minimal in-memory backend: one atomic counter per key.
#[derive(Default)]
struct BenchBackend {
    counters: RwLock<HashMap<Key, Arc<AtomicU64>>>,
    stream_registrations: AtomicUsize,
}

impl BenchBackend {
    fn stream_registrations(&self) -> usize {
        self.stream_registrations.load(Ordering::Relaxed)
    }

    fn shared_counter(&self, key: &Key) -> metrics::Counter {
        if let Some(state) = self.counters.read().unwrap().get(key).cloned() {
            return metrics::Counter::from_arc(Arc::new(CounterState(state)));
        }
        let state = self
            .counters
            .write()
            .unwrap()
            .entry(key.clone())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        metrics::Counter::from_arc(Arc::new(CounterState(state)))
    }
}

impl Recorder for BenchBackend {
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

    fn register_counter(&self, key: &Key, _metadata: &metrics::Metadata<'_>) -> metrics::Counter {
        if key.name() == METRIC_NAME {
            self.stream_registrations.fetch_add(1, Ordering::Relaxed);
        }
        self.shared_counter(key)
    }

    fn register_gauge(&self, _key: &Key, _metadata: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        _key: &Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

/// The facade macros resolve through the ambient global recorder; install the
/// macro-path backend once per process.
fn install_global_backend() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        // An already-installed recorder is kept: the macro path then measures
        // against it, which does not change its cost profile.
        let _ = metrics::set_global_recorder(BenchBackend::default());
    });
}

fn bench_counter_emission(c: &mut Criterion) {
    install_global_backend();

    let mut group = c.benchmark_group("counter_emission");
    group
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .sample_size(50);

    for &unique in &UNIQUE_LABEL_COUNTS {
        let endpoints = unique_endpoints(unique);
        let service = SERVICE.to_string();
        let method = METHOD.to_string();
        group.throughput(Throughput::Elements(unique as u64));

        group.bench_function(format!("facade_macro/unique_labels={unique}"), |b| {
            b.iter(|| {
                for endpoint in &endpoints {
                    // The pre-cache code materialized owned label values and
                    // re-registered the key on every emission.
                    counter!(
                        METRIC_NAME,
                        "driver_name" => DRIVER_NAME,
                        "endpoint" => endpoint.clone(),
                        "service" => service.clone(),
                        "method" => method.clone(),
                        "direction" => "sent",
                    )
                    .increment(1);
                }
            })
        });

        let backend = Arc::new(BenchBackend::default());
        let recorder: Arc<dyn MetricsRecorder> = Arc::new(DefaultMetricsRecorder::with_backend(
            Arc::clone(&backend) as Arc<dyn Recorder + Send + Sync>,
        ));

        // Warm every series once and prove the point of the caches: one
        // registration per unique label combination, not per emission.
        for endpoint in &endpoints {
            recorder.grpc_stream_message(endpoint, &service, &method, StreamDirection::Sent);
        }
        assert_eq!(
            backend.stream_registrations(),
            unique,
            "cached access must register each series exactly once"
        );

        group.bench_function(format!("interned_cache/unique_labels={unique}"), |b| {
            b.iter(|| {
                for endpoint in &endpoints {
                    recorder.grpc_stream_message(
                        endpoint,
                        &service,
                        &method,
                        StreamDirection::Sent,
                    );
                }
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_counter_emission);
criterion_main!(benches);
