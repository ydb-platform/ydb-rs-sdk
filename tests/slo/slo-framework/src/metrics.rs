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
        self.inner
            .operation_latency
            .lock()
            .unwrap()
            .record(latency.as_micros() as u64, attrs_key);
    }

    pub fn record_topic_e2e_latency(&self, latency: Duration) {
        let attrs_key = format!("ref={}", self.inner.ref_name);
        self.inner
            .topic_e2e_latency
            .lock()
            .unwrap()
            .record(latency.as_micros() as u64, attrs_key);
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
            let mut error_attrs = attrs;
            error_attrs.push(KeyValue::new("error_category", "ydb"));
            error_attrs.push(KeyValue::new("error_name", err_msg.to_string()));
            self.metrics.inner.errors_total.add(1, &error_attrs);
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
