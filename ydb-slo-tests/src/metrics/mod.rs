pub mod span;

use crate::metrics::span::Span;
use prometheus::{
    labels, proto, BasicAuthentication, CounterVec, Encoder, Error, GaugeVec, HistogramVec,
    ProtobufEncoder, Registry, TextEncoder,
};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Client, StatusCode};
use std::collections::HashMap;
use std::fmt;
use std::hash::BuildHasher;
use std::time::Duration;

#[derive(Debug)]
pub struct MetricsCollector {
    pub registry: Registry,
    pub job_name: String,
    pub grouping: HashMap<String, String>,
    pub prom_pgw: String,
    pub errors_total: CounterVec,
    pub operations_total: CounterVec,
    pub operations_success_total: CounterVec,
    pub operations_failure_total: CounterVec,
    pub operation_latency_seconds: HistogramVec,
    pub retry_attempts: GaugeVec,
    pub retry_attempts_total: CounterVec,
    pub retries_success_total: CounterVec,
    pub retries_failure_total: CounterVec,
    pub pending_operations: GaugeVec,
}

impl MetricsCollector {
    pub fn new(prom_pgw: String, ref_id: String, label: String, job_name: String) -> Self {
        let registry = Registry::new();

        let errors_total = prometheus::register_counter_vec_with_registry!(
            "sdk_errors_total",
            "Total number of errors encountered, categorized by error type",
            &["error_type"],
            registry,
        )
        .unwrap();

        let operations_total = prometheus::register_counter_vec_with_registry!(
            "sdk_operations_total",
            "Total number of operations, categorized by type attempted by the SDK",
            &["operation_type"],
            registry,
        )
        .unwrap();

        let operations_success_total = prometheus::register_counter_vec_with_registry!(
            "sdk_operations_success_total",
            "Total number of successful operations, categorized by type",
            &["operation_type"],
            registry,
        )
        .unwrap();

        let operations_failure_total = prometheus::register_counter_vec_with_registry!(
            "sdk_operations_failure_total",
            "Total number of failed operations, categorized by type",
            &["operation_type"],
            registry,
        )
        .unwrap();

        let operation_latency_seconds = prometheus::register_histogram_vec_with_registry!(
            "sdk_operation_latency_seconds",
            "Latency of operations performed by the SDK in seconds, categorized by type and status",
            &["operation_type", "operation_status"],
            registry,
        )
        .unwrap();

        let retry_attempts = prometheus::register_gauge_vec_with_registry!(
            "sdk_retry_attempts",
            "Current retry attempts, categorized by operation type",
            &["operation_type"],
            registry,
        )
        .unwrap();

        let retry_attempts_total = prometheus::register_counter_vec_with_registry!(
            "sdk_retry_attempts_total",
            "Total number of retry attempts, categorized by operation type",
            &["operation_type"],
            registry,
        )
        .unwrap();

        let retries_success_total = prometheus::register_counter_vec_with_registry!(
            "sdk_retries_success_total",
            "Total number of successful retries, categorized by operation type",
            &["operation_type"],
            registry,
        )
        .unwrap();

        let retries_failure_total = prometheus::register_counter_vec_with_registry!(
            "sdk_retries_failure_total",
            "Total number of failed retries, categorized by operation type",
            &["operation_type"],
            registry,
        )
        .unwrap();

        let pending_operations = prometheus::register_gauge_vec_with_registry!(
            "sdk_pending_operations",
            "Current number of pending operations, categorized by type",
            &["operation_type"],
            registry,
        )
        .unwrap();

        let grouping = labels! {
            "ref".to_owned() => ref_id.to_owned(),
            "sdk".to_owned() => format!("{}-{}", "rust".to_owned(), label.to_owned()).to_owned(),
            "sdk_version".to_owned() => env!("CARGO_PKG_VERSION").to_owned(),
        };

        Self {
            registry,
            job_name: job_name.to_owned(),
            grouping,
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
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder.encode_utf8(&metric_families, &mut buffer)?;

        self.push(
            &self.job_name,
            self.grouping.clone(),
            &self.prom_pgw,
            metric_families.clone(),
            None,
        )
        .await?;

        Ok(())
    }

    // non-blocking variation of push fn from https://github.com/tikv/rust-prometheus/blob/v0.14.0/src/push.rs
    async fn push<S: BuildHasher>(
        &self,
        job: &str,
        grouping: HashMap<String, String, S>,
        url: &str,
        mfs: Vec<proto::MetricFamily>,
        basic_auth: Option<BasicAuthentication>,
    ) -> prometheus::Result<()> {
        // Suppress clippy warning needless_pass_by_value.
        let grouping = grouping;

        let mut push_url = if url.contains("://") {
            url.to_owned()
        } else {
            format!("http://{}", url)
        };

        if push_url.ends_with('/') {
            push_url.pop();
        }

        let mut url_components = Vec::new();
        if job.contains('/') {
            return Err(Error::Msg(format!("job contains '/': {}", job)));
        }

        // TODO: escape job
        url_components.push(job.to_owned());

        for (ln, lv) in &grouping {
            // TODO: check label name
            if lv.contains('/') {
                return Err(Error::Msg(format!(
                    "value of grouping label {} contains '/': {}",
                    ln, lv
                )));
            }
            url_components.push(ln.to_owned());
            url_components.push(lv.to_owned());
        }

        push_url = format!("{}/metrics/job/{}", push_url, url_components.join("/"));

        let encoder = ProtobufEncoder::new();
        let mut buf = Vec::new();

        for mf in mfs {
            // Check for pre-existing grouping labels:
            for m in mf.get_metric() {
                for lp in m.get_label() {
                    if lp.get_name() == "job" {
                        return Err(Error::Msg(format!(
                            "pushed metric {} already contains a \
                         job label",
                            mf.get_name()
                        )));
                    }
                    if grouping.contains_key(lp.get_name()) {
                        return Err(Error::Msg(format!(
                            "pushed metric {} already contains \
                         grouping label {}",
                            mf.get_name(),
                            lp.get_name()
                        )));
                    }
                }
            }
            // Ignore error, `no metrics` and `no name`.
            let _ = encoder.encode(&[mf], &mut buf);
        }

        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        let mut builder = client
            .put(push_url.as_str())
            .header(CONTENT_TYPE, encoder.format_type())
            .body(buf);

        if let Some(BasicAuthentication { username, password }) = basic_auth {
            builder = builder.basic_auth(username, Some(password));
        }

        let response = builder
            .send()
            .await
            .map_err(|e| Error::Msg(format!("{}", e)))?;

        match response.status() {
            StatusCode::ACCEPTED => Ok(()),
            StatusCode::OK => Ok(()),
            _ => Err(Error::Msg(format!(
                "unexpected status code {} while pushing to {}",
                response.status(),
                push_url
            ))),
        }
    }

    pub async fn reset(&self) -> Result<(), MetricsPushError> {
        self.errors_total.reset();
        self.operations_total.reset();
        self.operations_success_total.reset();
        self.operations_failure_total.reset();
        self.operation_latency_seconds.reset();
        self.retry_attempts.reset();
        self.retry_attempts_total.reset();
        self.retries_success_total.reset();
        self.retries_failure_total.reset();
        self.pending_operations.reset();

        self.push_to_gateway().await
    }
}

#[derive(Clone, Debug)]
pub enum OperationType {
    Read,
    Write,
}

impl fmt::Display for OperationType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

#[derive(Debug)]
pub struct MetricsPushError {
    value: prometheus::Error,
}

impl fmt::Display for MetricsPushError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Failed to push metrics to Pushgateway: {}", self.value)
    }
}

impl From<prometheus::Error> for MetricsPushError {
    fn from(err: prometheus::Error) -> Self {
        MetricsPushError { value: err }
    }
}
