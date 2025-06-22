use crate::metrics::{MetricsCollector, OperationType};
use crate::workers::Attempts;
use std::time::Instant;
use ydb::YdbOrCustomerError;

pub struct Span<'a> {
    name: OperationType,
    start: Instant,
    metrics: &'a MetricsCollector,
}

impl<'a> Span<'a> {
    pub fn start(metrics: &'a MetricsCollector, operation_type: OperationType) -> Self {
        metrics
            .pending_operations
            .with_label_values(&[&operation_type.to_string()])
            .inc();

        Self {
            name: operation_type,
            start: Instant::now(),
            metrics,
        }
    }

    pub fn finish(self, attempts: Attempts, err: Option<YdbOrCustomerError>) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let operation_type = self.name.to_string();

        self.metrics
            .pending_operations
            .with_label_values(&[operation_type.as_str()])
            .dec();

        self.metrics
            .retry_attempts
            .with_label_values(&[operation_type.as_str()])
            .set(attempts as f64);

        self.metrics
            .operations_total
            .with_label_values(&[operation_type.as_str()])
            .inc();

        self.metrics
            .retry_attempts_total
            .with_label_values(&[operation_type.as_str()])
            .inc_by(attempts as f64);

        match err {
            Some(e) => {
                self.metrics
                    .errors_total
                    .with_label_values(&[e.to_string().as_str()])
                    .inc();

                self.metrics
                    .retries_failure_total
                    .with_label_values(&[operation_type.as_str()])
                    .inc_by(attempts as f64);

                self.metrics
                    .operations_failure_total
                    .with_label_values(&[operation_type.as_str()])
                    .inc();

                self.metrics
                    .operation_latency_seconds
                    .with_label_values(&[operation_type.as_str(), "failure"])
                    .observe(elapsed);
            }
            None => {
                self.metrics
                    .retries_success_total
                    .with_label_values(&[operation_type.as_str()])
                    .inc_by(attempts as f64);

                self.metrics
                    .operations_success_total
                    .with_label_values(&[operation_type.as_str()])
                    .inc();

                self.metrics
                    .operation_latency_seconds
                    .with_label_values(&[operation_type.as_str(), "success"])
                    .observe(elapsed);
            }
        }
    }
}
