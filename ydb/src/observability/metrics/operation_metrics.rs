use crate::YdbError;
use crate::observability::telemetry::TelemetryMeta;
use metrics::{Counter, Histogram, Unit, counter, histogram};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::log;

#[derive(Clone)]
pub(crate) struct OperationMetrics {
    execute_query_duration: Histogram,
    commit_duration: Histogram,
    rollback_duration: Histogram,
    #[allow(dead_code)]
    create_session_duration: Histogram,
    failed_operations: Arc<Mutex<HashMap<String, Counter>>>,
}

impl OperationMetrics {
    pub(crate) fn init(meta: &TelemetryMeta) -> Self {
        OperationMetrics {
            execute_query_duration: Self::create_operation_histogram("ExecuteQuery", meta),
            commit_duration: Self::create_operation_histogram("Commit", meta),
            rollback_duration: Self::create_operation_histogram("Rollback", meta),
            create_session_duration: Self::create_operation_histogram("CreateSession", meta),
            failed_operations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn record_execute_query_duration(&self, duration: Duration) {
        self.record_operation_duration(&self.execute_query_duration, duration);
    }

    pub(crate) fn record_commit_duration(&self, duration: Duration) {
        self.record_operation_duration(&self.commit_duration, duration);
    }

    pub(crate) fn record_rollback_duration(&self, duration: Duration) {
        self.record_operation_duration(&self.rollback_duration, duration);
    }

    #[allow(dead_code)]
    pub(crate) fn record_create_session_duration(&self, duration: Duration) {
        self.record_operation_duration(&self.create_session_duration, duration);
    }

    pub(crate) fn record_operation_failure(
        &self,
        operation: &str,
        error: &YdbError,
        meta: &TelemetryMeta,
    ) {
        match self.failed_operations.try_lock() {
            Ok(mut guard) => {
                guard
                    .entry(error.status_code().to_string())
                    .or_insert_with(|| {
                        let labels = vec![
                            ("database".to_string(), meta.database.clone()),
                            ("endpoint".to_string(), meta.endpoint.clone()),
                            ("status_code".to_string(), error.status_code().to_string()),
                            ("operation".to_string(), operation.to_string()),
                        ];

                        counter!("ydb.client.operation.failed", &labels)
                    })
                    .increment(1);
            }
            Err(err) => log::error!("Failed to update ydb.client.operation.failed: {}", err),
        };
    }

    fn record_operation_duration(&self, operation: &Histogram, duration: Duration) {
        operation.record(duration.as_secs_f64());
    }

    fn create_operation_histogram(operation: &str, meta: &TelemetryMeta) -> Histogram {
        let labels = vec![
            ("database".to_string(), meta.database.clone()),
            ("endpoint".to_string(), meta.endpoint.clone()),
            ("operation".to_string(), operation.to_string()),
        ];

        histogram!(
            unit: Unit::Seconds,
            description: "Duration of YDB client operations",
            "ydb.client.operation.duration",
            &labels
        )
    }
}
