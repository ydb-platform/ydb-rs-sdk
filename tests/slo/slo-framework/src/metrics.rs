use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, MeterProvider as _, UpDownCounter};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use opentelemetry_sdk::runtime;

use crate::config::Config;

use self::latency::LatencySeries;

mod latency;

pub type OperationType = &'static str;
pub const OPERATION_READ: OperationType = "read";
pub const OPERATION_WRITE: OperationType = "write";

const STATUS_SUCCESS: &str = "success";
const STATUS_FAILURE: &str = "failure";

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    ref_name: String,
    operation_latency: Arc<Mutex<LatencySeries>>,
    topic_e2e_latency: Arc<Mutex<LatencySeries>>,
    operations_total: Counter<u64>,
    retry_attempts_total: Counter<u64>,
    retry_attempts: Gauge<u64>,
    pending_operations: UpDownCounter<i64>,
    errors_total: Counter<u64>,
    timeouts_total: Counter<u64>,
    // Keeps the provider alive until all metric instruments are dropped.
    _provider: SdkMeterProvider,
}

pub struct Span {
    metrics: Metrics,
    operation_type: OperationType,
    started: Instant,
    pending: bool,
}

impl Metrics {
    pub fn new(cfg: &Config) -> Result<Self, String> {
        let operation_latency = Arc::new(Mutex::new(LatencySeries::new()));
        let topic_e2e_latency = Arc::new(Mutex::new(LatencySeries::new()));
        let ref_name = cfg.ref_name.clone();

        let resource = Resource::new(vec![
            KeyValue::new("service.name", cfg.label.clone()),
            KeyValue::new("ref", ref_name.clone()),
            KeyValue::new("sdk", "rust"),
            KeyValue::new("sdk_version", env!("CARGO_PKG_VERSION")),
        ]);

        let provider_builder = SdkMeterProvider::builder().with_resource(resource);
        let provider = if let Some(endpoint) = &cfg.otlp_endpoint {
            let exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_endpoint(endpoint.clone())
                .with_temporality(Temporality::Cumulative)
                .build()
                .map_err(|err| format!("failed to create OTLP exporter: {err}"))?;

            let reader = PeriodicReader::builder(exporter, runtime::Tokio)
                .with_interval(Duration::from_secs(1))
                .build();

            provider_builder.with_reader(reader).build()
        } else {
            provider_builder.build()
        };

        let meter = provider.meter("slo-workload");

        if cfg.otlp_endpoint.is_some() {
            latency::register_gauges(
                &meter,
                operation_latency.clone(),
                "sdk.operation.latency",
                "operation latency",
            );
            latency::register_gauges(
                &meter,
                topic_e2e_latency.clone(),
                "sdk.topic.e2e.latency",
                "topic end-to-end latency",
            );
        }

        Ok(Self {
            inner: Arc::new(MetricsInner {
                ref_name,
                operation_latency,
                topic_e2e_latency,
                operations_total: meter
                    .u64_counter("sdk.operations.total")
                    .with_description("Total number of operations, categorized by type")
                    .with_unit("{operation}")
                    .build(),
                retry_attempts_total: meter
                    .u64_counter("sdk.retry.attempts.total")
                    .with_description("Total number of retry attempts")
                    .with_unit("{attempt}")
                    .build(),
                retry_attempts: meter
                    .u64_gauge("sdk.retry.attempts")
                    .with_description("Current retry attempts")
                    .build(),
                pending_operations: meter
                    .i64_up_down_counter("sdk.pending.operations")
                    .with_description("Current number of pending operations")
                    .build(),
                errors_total: meter
                    .u64_counter("sdk.errors.total")
                    .with_description("Total number of errors encountered")
                    .with_unit("{error}")
                    .build(),
                timeouts_total: meter
                    .u64_counter("sdk.timeouts.total")
                    .with_description("Total number of timeout errors")
                    .with_unit("{timeout}")
                    .build(),
                _provider: provider,
            }),
        })
    }

    pub fn record_latency_with_attrs_key(&self, attrs_key: String, latency: Duration) {
        record_latency_series(&self.inner.operation_latency, latency, attrs_key);
    }

    pub fn record_topic_e2e_latency(&self, latency: Duration) {
        let attrs_key = format!("ref={}", self.inner.ref_name);
        record_latency_series(&self.inner.topic_e2e_latency, latency, attrs_key);
    }

    pub fn initialize_error_series(&self, operation_type: OperationType, error_name: &'static str) {
        self.inner.errors_total.add(
            0,
            &error_attributes(&self.inner.ref_name, operation_type, error_name),
        );
    }

    pub(crate) fn check(&self) -> Result<(), String> {
        check_latency_series(&self.inner.operation_latency, "operation latency")?;
        check_latency_series(&self.inner.topic_e2e_latency, "topic end-to-end latency")
    }

    pub fn start(&self, operation_type: OperationType) -> Span {
        self.inner.pending_operations.add(
            1,
            &[
                KeyValue::new("ref", self.inner.ref_name.clone()),
                KeyValue::new("operation_type", operation_type),
            ],
        );

        Span {
            metrics: self.clone(),
            operation_type,
            started: Instant::now(),
            pending: true,
        }
    }
}

fn record_latency_series(series: &Mutex<LatencySeries>, latency: Duration, attrs_key: String) {
    match series.lock() {
        Ok(mut series) => series.record(latency, attrs_key),
        Err(poisoned) => poisoned
            .into_inner()
            .fail("latency histogram lock is poisoned"),
    }
}

fn check_latency_series(series: &Mutex<LatencySeries>, name: &str) -> Result<(), String> {
    let series = series
        .lock()
        .map_err(|_| format!("{name} histogram lock is poisoned"))?;

    match series.recording_error() {
        Some(error) => Err(format!("{name} metrics failed: {error}")),
        None => Ok(()),
    }
}

impl Span {
    pub fn finish(mut self, err: Option<&str>, attempts: u64) {
        let status = if err.is_some() {
            STATUS_FAILURE
        } else {
            STATUS_SUCCESS
        };

        let attrs_key = format!(
            "ref={};operation_type={};operation_status={}",
            self.metrics.inner.ref_name, self.operation_type, status
        );
        let attrs = attrs_from_key(&attrs_key);

        self.metrics
            .record_latency_with_attrs_key(attrs_key, self.started.elapsed());

        self.metrics.inner.operations_total.add(1, &attrs);
        self.metrics
            .inner
            .retry_attempts_total
            .add(attempts, &attrs);
        self.metrics.inner.retry_attempts.record(attempts, &attrs);
        self.finish_pending();

        if let Some(err_msg) = err {
            if err_msg.contains("timeout") || err_msg.contains("deadline") {
                self.metrics.inner.timeouts_total.add(1, &attrs);
            }
            self.metrics.inner.errors_total.add(
                1,
                &error_attributes(&self.metrics.inner.ref_name, self.operation_type, err_msg),
            );
        }
    }

    pub fn cancel(mut self) {
        self.finish_pending();
    }

    fn finish_pending(&mut self) {
        if !self.pending {
            return;
        }
        self.pending = false;

        self.metrics.inner.pending_operations.add(
            -1,
            &[
                KeyValue::new("ref", self.metrics.inner.ref_name.clone()),
                KeyValue::new("operation_type", self.operation_type),
            ],
        );
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        self.finish_pending();
    }
}

fn attrs_from_key(key: &str) -> Vec<KeyValue> {
    key.split(';')
        .filter_map(|part| {
            let (k, v) = part.split_once('=')?;
            Some(KeyValue::new(k.to_string(), v.to_string()))
        })
        .collect()
}

fn error_attributes(
    ref_name: &str,
    operation_type: OperationType,
    error_name: &str,
) -> Vec<KeyValue> {
    vec![
        KeyValue::new("ref", ref_name.to_string()),
        KeyValue::new("operation_type", operation_type),
        KeyValue::new("operation_status", STATUS_FAILURE),
        KeyValue::new("error_category", "ydb"),
        KeyValue::new("error_name", error_name.to_string()),
    ]
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Weak};

    use opentelemetry::Value;
    use opentelemetry_sdk::metrics::data::{ResourceMetrics, Sum};
    use opentelemetry_sdk::metrics::reader::MetricReader;
    use opentelemetry_sdk::metrics::{InstrumentKind, ManualReader, MetricResult, Pipeline};

    use super::*;

    #[derive(Clone, Debug)]
    struct SharedReader(Arc<ManualReader>);

    impl MetricReader for SharedReader {
        fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
            self.0.register_pipeline(pipeline);
        }

        fn collect(&self, metrics: &mut ResourceMetrics) -> MetricResult<()> {
            self.0.collect(metrics)
        }

        fn force_flush(&self) -> MetricResult<()> {
            self.0.force_flush()
        }

        fn shutdown(&self) -> MetricResult<()> {
            self.0.shutdown()
        }

        fn temporality(&self, kind: InstrumentKind) -> Temporality {
            self.0.temporality(kind)
        }
    }

    #[test]
    fn otlp_metric_exporter_has_http_client() {
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint("http://localhost:4318/v1/metrics")
            .with_temporality(Temporality::Cumulative)
            .build();
        assert!(
            exporter.is_ok(),
            "OTLP metrics exporter must build with reqwest HTTP client features: {}",
            exporter.err().map(|e| e.to_string()).unwrap_or_default()
        );
    }

    #[test]
    fn zero_measurement_initializes_error_series() {
        let reader = SharedReader(Arc::new(ManualReader::builder().build()));
        let provider = SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .build();
        let counter = provider
            .meter("test")
            .u64_counter("sdk.errors.total")
            .build();
        counter.add(
            0,
            &error_attributes("test-ref", "transaction", "commit_phase_failure"),
        );

        let mut metrics = ResourceMetrics {
            resource: Resource::empty(),
            scope_metrics: Vec::new(),
        };
        reader.collect(&mut metrics).expect("collect metrics");

        let errors = metrics
            .scope_metrics
            .iter()
            .flat_map(|scope| &scope.metrics)
            .find(|metric| metric.name == "sdk.errors.total")
            .expect("error counter must be exported")
            .data
            .as_any()
            .downcast_ref::<Sum<u64>>()
            .expect("error counter must be an unsigned sum");
        let initialized = errors.data_points.iter().any(|point| {
            point.value == 0
                && point.attributes.iter().any(|attribute| {
                    attribute.key.as_str() == "error_name"
                        && matches!(
                            &attribute.value,
                            Value::String(value) if value.as_str() == "commit_phase_failure"
                        )
                })
        });
        assert!(initialized, "zero-valued error series was not exported");
    }
}
