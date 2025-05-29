pub mod labels;
pub mod span;

use crate::metrics::labels::{ErrorLabel, OperationLabel, OperationLatencyLabels};
use crate::metrics::span::Span;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use std::fmt;

#[derive(Debug)]
pub struct MetricsCollector {
    pub registry: Registry,
    pub prom_pgw: String,
    pub errors_total: Family<ErrorLabel, Counter>,
    pub operations_total: Family<OperationLabel, Counter>,
    pub operations_success_total: Family<OperationLabel, Counter>,
    pub operations_failure_total: Family<OperationLabel, Counter>,
    pub operation_latency_seconds: Family<OperationLatencyLabels, Histogram>,
    pub retry_attempts: Family<OperationLabel, Gauge>,
    pub retry_attempts_total: Family<OperationLabel, Counter>,
    pub retries_success_total: Family<OperationLabel, Counter>,
    pub retries_failure_total: Family<OperationLabel, Counter>,
    pub pending_operations: Family<OperationLabel, Gauge>,
}

impl MetricsCollector {
    pub fn new(prom_pgw: String) -> Self {
        let mut registry = Registry::default();

        let errors_total = Family::default();
        let operations_total = Family::default();
        let operations_success_total = Family::default();
        let operations_failure_total = Family::default();
        let operation_latency_seconds =
            Family::<OperationLatencyLabels, Histogram>::new_with_constructor(|| {
                Histogram::new([
                    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                ])
            });
        let retry_attempts = Family::default();
        let retry_attempts_total = Family::default();
        let retries_success_total = Family::default();
        let retries_failure_total = Family::default();
        let pending_operations = Family::default();

        registry.register(
            "sdk_errors_total",
            "Total number of errors encountered, categorized by error type.",
            errors_total.clone(),
        );
        registry.register(
            "sdk_operations_total",
            "Total number of operations, categorized by type attempted by the SDK.",
            operations_total.clone(),
        );
        registry.register(
            "sdk_operations_success_total",
            "Total number of successful operations, categorized by type.",
            operations_success_total.clone(),
        );
        registry.register(
            "sdk_operations_failure_total",
            "Total number of failed operations, categorized by type.",
            operations_failure_total.clone(),
        );
        registry.register(
            "sdk_operation_latency_seconds",
            "Latency of operations performed by the SDK in seconds, categorized by type and status.",
            operation_latency_seconds.clone(),
        );
        registry.register(
            "sdk_retry_attempts",
            "Current retry attempts, categorized by operation type.",
            retry_attempts.clone(),
        );
        registry.register(
            "sdk_retry_attempts_total",
            "Total number of retry attempts, categorized by operation type.",
            retry_attempts_total.clone(),
        );
        registry.register(
            "sdk_retries_success_total",
            "Total number of successful retries, categorized by operation type.",
            retries_success_total.clone(),
        );
        registry.register(
            "sdk_retries_failure_total",
            "Total number of failed retries, categorized by operation type.",
            retries_failure_total.clone(),
        );
        registry.register(
            "sdk_pending_operations",
            "Current number of pending operations, categorized by type.",
            pending_operations.clone(),
        );

        Self {
            registry,
            prom_pgw,

            errors_total,
            operations_total,
            operations_success_total,
            operations_failure_total,
            operation_latency_seconds,
            retry_attempts,
            retry_attempts_total,
            retries_success_total,
            retries_failure_total,
            pending_operations,
        }
    }

    pub fn start(&self, operation_type: OperationType) -> Span {
        Span::start(self, operation_type)
    }

    pub async fn push_to_gateway(&self) -> Result<(), MetricsPushError> {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry)?;
        reqwest::Client::new()
            .post(&self.prom_pgw)
            .body(buffer)
            .send()
            .await?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum OperationType {
    Read,
    Write,
}

impl fmt::Display for OperationType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug)]
pub enum MetricsPushError {
    Encode(std::fmt::Error),
    Push(reqwest::Error),
}

impl std::fmt::Display for MetricsPushError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            MetricsPushError::Encode(ref err) => {
                write!(f, "Failed to encode metrics in registry: {}", err)
            }
            MetricsPushError::Push(ref err) => {
                write!(f, "Failed to push metrics in prometheus gateway: {}", err)
            }
        }
    }
}

impl From<std::fmt::Error> for MetricsPushError {
    fn from(err: std::fmt::Error) -> MetricsPushError {
        MetricsPushError::Encode(err)
    }
}

impl From<reqwest::Error> for MetricsPushError {
    fn from(err: reqwest::Error) -> MetricsPushError {
        MetricsPushError::Push(err)
    }
}
