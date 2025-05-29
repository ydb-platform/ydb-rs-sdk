use crate::metrics::labels::{ErrorLabel, OperationLabel, OperationLatencyLabels, OperationStatus};
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
    pub fn start(metrics: &'a MetricsCollector, operation_type: OperationType) -> Span<'a> {
        metrics
            .pending_operations
            .get_or_create(&OperationLabel {
                operation_type: operation_type.to_string(),
            })
            .inc();

        Span {
            name: operation_type,
            start: Instant::now(),
            metrics,
        }
    }

    pub fn finish(self, attempts: Attempts, err: Option<YdbOrCustomerError>) {
        let elapsed = self.start.elapsed().as_secs_f64();

        self.metrics
            .pending_operations
            .get_or_create(&OperationLabel {
                operation_type: self.name.clone().to_string(),
            })
            .dec();
        self.metrics
            .retry_attempts
            .get_or_create(&OperationLabel {
                operation_type: self.name.clone().to_string(),
            })
            .set(attempts as i64);
        self.metrics
            .operations_total
            .get_or_create(&OperationLabel {
                operation_type: self.name.clone().to_string(),
            })
            .inc();
        self.metrics
            .retry_attempts_total
            .get_or_create(&OperationLabel {
                operation_type: self.name.clone().to_string(),
            })
            .inc_by(attempts as u64);

        if let Some(e) = err {
            self.metrics
                .errors_total
                .get_or_create(&ErrorLabel {
                    error_type: e.to_string(),
                })
                .inc();
            self.metrics
                .retries_failure_total
                .get_or_create(&OperationLabel {
                    operation_type: self.name.clone().to_string(),
                })
                .inc_by(attempts as u64);
            self.metrics
                .operations_failure_total
                .get_or_create(&OperationLabel {
                    operation_type: self.name.clone().to_string(),
                })
                .inc();
            self.metrics
                .operation_latency_seconds
                .get_or_create(&OperationLatencyLabels {
                    operation_type: self.name.clone().to_string(),
                    operation_status: OperationStatus::Failure,
                })
                .observe(elapsed);
        } else {
            self.metrics
                .retries_success_total
                .get_or_create(&OperationLabel {
                    operation_type: self.name.clone().to_string(),
                })
                .inc_by(attempts as u64);
            self.metrics
                .operations_success_total
                .get_or_create(&OperationLabel {
                    operation_type: self.name.clone().to_string(),
                })
                .inc();
            self.metrics
                .operation_latency_seconds
                .get_or_create(&OperationLatencyLabels {
                    operation_type: self.name.clone().to_string(),
                    operation_status: OperationStatus::Success,
                })
                .observe(elapsed);
        }
    }
}
