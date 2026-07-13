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
    provider: Option<SdkMeterProvider>,
    operation_latency: Arc<Mutex<LatencySeries>>,
    topic_e2e_latency: Arc<Mutex<LatencySeries>>,
    operations_total: Option<Counter<u64>>,
    retry_attempts_total: Option<Counter<u64>>,
    retry_attempts: Option<Gauge<u64>>,
    pending_operations: Option<UpDownCounter<i64>>,
    errors_total: Option<Counter<u64>>,
    timeouts_total: Option<Counter<u64>>,
}

pub struct Span {
    metrics: Metrics,
    operation_type: OperationType,
    started: Instant,
}

impl Metrics {
    pub fn new(cfg: &Config) -> Result<Self, String> {
        let operation_latency = Arc::new(Mutex::new(LatencySeries::new()));
        let topic_e2e_latency = Arc::new(Mutex::new(LatencySeries::new()));
        let ref_name = cfg.ref_name.clone();
        let label = cfg.label.clone();

        let Some(endpoint) = cfg.otlp_endpoint.as_ref() else {
            return Ok(Self {
                inner: Arc::new(MetricsInner {
                    ref_name,
                    provider: None,
                    operation_latency,
                    topic_e2e_latency,
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

        Ok(Self {
            inner: Arc::new(MetricsInner {
                ref_name,
                provider: Some(provider),
                operation_latency,
                topic_e2e_latency,
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
            if (err_msg.contains("timeout") || err_msg.contains("deadline"))
                && let Some(counter) = &self.metrics.inner.timeouts_total
            {
                counter.add(1, &attrs);
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
