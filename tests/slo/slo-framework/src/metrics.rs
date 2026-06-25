use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use opentelemetry::metrics::{Counter, Gauge, MeterProvider as _, UpDownCounter};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::Resource;

use crate::config::Config;

const HDR_MIN_MICROSECONDS: u64 = 1;
const HDR_MAX_MICROSECONDS: u64 = 60_000_000;
const HDR_SIGNIFICANT_DIGITS: u8 = 5;

pub type OperationType = &'static str;
pub const OPERATION_READ: OperationType = "read";
pub const OPERATION_WRITE: OperationType = "write";
pub const OPERATION_MESSAGE_RTT: OperationType = "message_rtt";

const STATUS_SUCCESS: &str = "success";
const STATUS_FAILURE: &str = "failure";

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    ref_name: String,
    provider: Option<SdkMeterProvider>,
    latency: Arc<Mutex<LatencyHistogram>>,
    operations_total: Option<Counter<u64>>,
    retry_attempts_total: Option<Counter<u64>>,
    retry_attempts: Option<Gauge<u64>>,
    pending_operations: Option<UpDownCounter<i64>>,
    errors_total: Option<Counter<u64>>,
    timeouts_total: Option<Counter<u64>>,
}

struct LatencyHistogram {
    by_attrs: HashMap<String, Histogram<u64>>,
    cached_percentiles: HashMap<String, [f64; 3]>,
}

impl LatencyHistogram {
    fn new() -> Self {
        Self {
            by_attrs: HashMap::new(),
            cached_percentiles: HashMap::new(),
        }
    }

    fn record(&mut self, latency_micros: u64, attrs_key: String) {
        let hist = self.by_attrs.entry(attrs_key.clone()).or_insert_with(|| {
            Histogram::new_with_bounds(
                HDR_MIN_MICROSECONDS,
                HDR_MAX_MICROSECONDS,
                HDR_SIGNIFICANT_DIGITS,
            )
            .expect("valid hdr bounds")
        });
        hist.record(latency_micros).ok();
        self.cached_percentiles.insert(
            attrs_key,
            [
                hist.value_at_quantile(0.5) as f64 / 1_000_000.0,
                hist.value_at_quantile(0.95) as f64 / 1_000_000.0,
                hist.value_at_quantile(0.99) as f64 / 1_000_000.0,
            ],
        );
    }

    fn percentiles(&self) -> &HashMap<String, [f64; 3]> {
        &self.cached_percentiles
    }
}

pub struct Span {
    metrics: Metrics,
    operation_type: OperationType,
    started: Instant,
}

impl Metrics {
    pub fn new(cfg: &Config) -> Result<Self, String> {
        let latency = Arc::new(Mutex::new(LatencyHistogram::new()));
        let ref_name = cfg.ref_name.clone();
        let label = cfg.label.clone();

        let Some(endpoint) = cfg.otlp_endpoint.as_ref() else {
            return Ok(Self {
                inner: Arc::new(MetricsInner {
                    ref_name,
                    provider: None,
                    latency,
                    operations_total: None,
                    retry_attempts_total: None,
                    retry_attempts: None,
                    pending_operations: None,
                    errors_total: None,
                    timeouts_total: None,
                }),
            });
        };

        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(endpoint.clone())
            .with_temporality(Temporality::Cumulative)
            .build()
            .map_err(|err| format!("failed to create OTLP exporter: {err}"))?;

        let reader = PeriodicReader::builder(exporter, runtime::Tokio)
            .with_interval(Duration::from_secs(1))
            .build();

        let resource = Resource::new(vec![
            KeyValue::new("service.name", label.clone()),
            KeyValue::new("ref", ref_name.clone()),
            KeyValue::new("sdk", "rust"),
            KeyValue::new("sdk_version", env!("CARGO_PKG_VERSION")),
        ]);

        let provider = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_reader(reader)
            .build();

        let meter = provider.meter("slo-workload");

        let latency_p50 = latency.clone();
        let _latency_p50 = meter
            .f64_observable_gauge("sdk.operation.latency.p50.seconds")
            .with_description("50th percentile latency of operations in seconds")
            .with_unit("s")
            .with_callback(move |observer| {
                for (attrs_key, vals) in latency_p50.lock().unwrap().percentiles() {
                    observer.observe(vals[0], &attrs_from_key(attrs_key));
                }
            })
            .build();

        let latency_p95 = latency.clone();
        let _latency_p95 = meter
            .f64_observable_gauge("sdk.operation.latency.p95.seconds")
            .with_description("95th percentile latency of operations in seconds")
            .with_unit("s")
            .with_callback(move |observer| {
                for (attrs_key, vals) in latency_p95.lock().unwrap().percentiles() {
                    observer.observe(vals[1], &attrs_from_key(attrs_key));
                }
            })
            .build();

        let latency_p99 = latency.clone();
        let _latency_p99 = meter
            .f64_observable_gauge("sdk.operation.latency.p99.seconds")
            .with_description("99th percentile latency of operations in seconds")
            .with_unit("s")
            .with_callback(move |observer| {
                for (attrs_key, vals) in latency_p99.lock().unwrap().percentiles() {
                    observer.observe(vals[2], &attrs_from_key(attrs_key));
                }
            })
            .build();

        Ok(Self {
            inner: Arc::new(MetricsInner {
                ref_name,
                provider: Some(provider),
                latency,
                operations_total: Some(
                    meter
                        .u64_counter("sdk.operations.total")
                        .with_description("Total number of operations, categorized by type")
                        .with_unit("{operation}")
                        .build(),
                ),
                retry_attempts_total: Some(
                    meter
                        .u64_counter("sdk.retry.attempts.total")
                        .with_description("Total number of retry attempts")
                        .with_unit("{attempt}")
                        .build(),
                ),
                retry_attempts: Some(
                    meter
                        .u64_gauge("sdk.retry.attempts")
                        .with_description("Current retry attempts")
                        .build(),
                ),
                pending_operations: Some(
                    meter
                        .i64_up_down_counter("sdk.pending.operations")
                        .with_description("Current number of pending operations")
                        .build(),
                ),
                errors_total: Some(
                    meter
                        .u64_counter("sdk.errors.total")
                        .with_description("Total number of errors encountered")
                        .with_unit("{error}")
                        .build(),
                ),
                timeouts_total: Some(
                    meter
                        .u64_counter("sdk.timeouts.total")
                        .with_description("Total number of timeout errors")
                        .with_unit("{timeout}")
                        .build(),
                ),
            }),
        })
    }

    pub fn record_latency_with_attrs_key(&self, attrs_key: String, latency: Duration) {
        self.inner
            .latency
            .lock()
            .unwrap()
            .record(latency.as_micros() as u64, attrs_key);
    }

    pub fn record_latency_with_operation(&self, operation_type: OperationType, latency: Duration) {
        let attrs_key = format!(
            "ref={};operation_type={};operation_status={}",
            self.inner.ref_name, operation_type, STATUS_SUCCESS
        );

        self.record_latency_with_attrs_key(attrs_key, latency);
    }

    pub fn start(&self, operation_type: OperationType) -> Span {
        if let Some(counter) = &self.inner.pending_operations {
            counter.add(
                1,
                &[
                    KeyValue::new("ref", self.inner.ref_name.clone()),
                    KeyValue::new("operation_type", operation_type),
                ],
            );
        }

        Span {
            metrics: self.clone(),
            operation_type,
            started: Instant::now(),
        }
    }

    pub async fn push(&self) {
        if let Some(provider) = &self.inner.provider {
            let _ = provider.force_flush();
        }
    }

    pub async fn close(&self) {
        if let Some(provider) = &self.inner.provider {
            let _ = provider.shutdown();
        }
    }
}

impl Span {
    pub fn finish(self, err: Option<&str>, attempts: u64) {
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

        if let Some(counter) = &self.metrics.inner.operations_total {
            counter.add(1, &attrs);
        }
        if let Some(counter) = &self.metrics.inner.retry_attempts_total {
            counter.add(attempts, &attrs);
        }
        if let Some(gauge) = &self.metrics.inner.retry_attempts {
            gauge.record(attempts, &attrs);
        }
        if let Some(counter) = &self.metrics.inner.pending_operations {
            counter.add(
                -1,
                &[
                    KeyValue::new("ref", self.metrics.inner.ref_name.clone()),
                    KeyValue::new("operation_type", self.operation_type),
                ],
            );
        }

        if let Some(err_msg) = err {
            if err_msg.contains("timeout") || err_msg.contains("deadline") {
                if let Some(counter) = &self.metrics.inner.timeouts_total {
                    counter.add(1, &attrs);
                }
            }
            if let Some(counter) = &self.metrics.inner.errors_total {
                let mut error_attrs = attrs;
                error_attrs.push(KeyValue::new("error_category", "ydb"));
                error_attrs.push(KeyValue::new("error_name", err_msg.to_string()));
                counter.add(1, &error_attrs);
            }
        }
    }

    pub fn cancel(self) {
        if let Some(counter) = &self.metrics.inner.pending_operations {
            counter.add(
                -1,
                &[
                    KeyValue::new("ref", self.metrics.inner.ref_name.clone()),
                    KeyValue::new("operation_type", self.operation_type),
                ],
            );
        }
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
