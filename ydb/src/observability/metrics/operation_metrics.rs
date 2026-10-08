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
    create_session_duration: Histogram,
    failed_operations: Arc<Mutex<HashMap<(String, String), Counter>>>,
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
                    .entry((operation.to_string(), error.status_code().to_string()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::YdbStatusError;
    use crate::observability::metrics::mocks::{Labels, TestRecorder};
    use ydb_grpc::ydb_proto::status_ids::StatusCode;

    fn meta() -> TelemetryMeta {
        TelemetryMeta::new("/local".to_string(), "localhost:2135".to_string())
    }

    fn operation_labels(operation: &str) -> Labels {
        vec![
            ("database".to_string(), "/local".to_string()),
            ("endpoint".to_string(), "localhost:2135".to_string()),
            ("operation".to_string(), operation.to_string()),
        ]
    }

    fn failure_labels(operation: &str, status_code: &str) -> Labels {
        let mut labels = operation_labels(operation);
        labels.push(("status_code".to_string(), status_code.to_string()));
        labels
    }

    fn status_error(code: StatusCode) -> YdbError {
        YdbError::YdbStatusError(YdbStatusError::new("test", code as i32, vec![]))
    }

    const DURATION: &str = "ydb.client.operation.duration";
    const FAILED: &str = "ydb.client.operation.failed";

    #[test]
    fn durations_are_recorded_in_seconds_per_operation() {
        let recorder = TestRecorder::default();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let metrics = OperationMetrics::init(&meta());

        metrics.record_execute_query_duration(Duration::from_millis(1500));
        metrics.record_commit_duration(Duration::from_millis(250));
        metrics.record_rollback_duration(Duration::from_secs(2));
        metrics.record_create_session_duration(Duration::from_millis(10));

        assert_eq!(
            recorder.histogram(DURATION, &operation_labels("ExecuteQuery")),
            vec![1.5]
        );
        assert_eq!(
            recorder.histogram(DURATION, &operation_labels("Commit")),
            vec![0.25]
        );
        assert_eq!(
            recorder.histogram(DURATION, &operation_labels("Rollback")),
            vec![2.0]
        );
        assert_eq!(
            recorder.histogram(DURATION, &operation_labels("CreateSession")),
            vec![0.01]
        );
    }

    #[test]
    fn repeated_durations_accumulate_in_the_same_histogram() {
        let recorder = TestRecorder::default();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let metrics = OperationMetrics::init(&meta());

        metrics.record_commit_duration(Duration::from_secs(1));
        metrics.record_commit_duration(Duration::from_secs(3));

        assert_eq!(
            recorder.histogram(DURATION, &operation_labels("Commit")),
            vec![1.0, 3.0]
        );
        assert!(
            recorder
                .histogram(DURATION, &operation_labels("ExecuteQuery"))
                .is_empty()
        );
    }

    #[test]
    fn cloned_metrics_share_the_same_series() {
        let recorder = TestRecorder::default();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let metrics = OperationMetrics::init(&meta());

        let clone = metrics.clone();
        metrics.record_commit_duration(Duration::from_secs(1));
        clone.record_commit_duration(Duration::from_secs(2));
        metrics.record_operation_failure("Commit", &YdbError::DeadlineExceeded, &meta());
        clone.record_operation_failure("Commit", &YdbError::DeadlineExceeded, &meta());

        assert_eq!(
            recorder.histogram(DURATION, &operation_labels("Commit")),
            vec![1.0, 2.0]
        );
        assert_eq!(
            recorder.counter(FAILED, &failure_labels("Commit", "DEADLINE_EXCEEDED")),
            2
        );
    }

    #[test]
    fn failure_is_counted_with_operation_and_status_labels() {
        let recorder = TestRecorder::default();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let metrics = OperationMetrics::init(&meta());

        metrics.record_operation_failure(
            "ExecuteQuery",
            &status_error(StatusCode::Unavailable),
            &meta(),
        );

        assert_eq!(
            recorder.counter(FAILED, &failure_labels("ExecuteQuery", "UNAVAILABLE")),
            1
        );
    }

    #[test]
    fn repeated_failures_with_same_status_increment_one_counter() {
        let recorder = TestRecorder::default();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let metrics = OperationMetrics::init(&meta());

        for _ in 0..3 {
            metrics.record_operation_failure("Commit", &YdbError::DeadlineExceeded, &meta());
        }

        assert_eq!(
            recorder.counter(FAILED, &failure_labels("Commit", "DEADLINE_EXCEEDED")),
            3
        );
    }

    #[test]
    fn failures_with_different_statuses_use_separate_counters() {
        let recorder = TestRecorder::default();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let metrics = OperationMetrics::init(&meta());

        metrics.record_operation_failure(
            "ExecuteQuery",
            &status_error(StatusCode::Unavailable),
            &meta(),
        );
        metrics.record_operation_failure(
            "ExecuteQuery",
            &status_error(StatusCode::Overloaded),
            &meta(),
        );
        metrics.record_operation_failure(
            "ExecuteQuery",
            &status_error(StatusCode::Overloaded),
            &meta(),
        );

        assert_eq!(
            recorder.counter(FAILED, &failure_labels("ExecuteQuery", "UNAVAILABLE")),
            1
        );
        assert_eq!(
            recorder.counter(FAILED, &failure_labels("ExecuteQuery", "OVERLOADED")),
            2
        );
    }

    #[test]
    fn failures_with_same_status_in_different_operations_use_separate_counters() {
        let recorder = TestRecorder::default();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let metrics = OperationMetrics::init(&meta());

        metrics.record_operation_failure("ExecuteQuery", &YdbError::DeadlineExceeded, &meta());
        metrics.record_operation_failure("Commit", &YdbError::DeadlineExceeded, &meta());

        assert_eq!(
            recorder.counter(FAILED, &failure_labels("ExecuteQuery", "DEADLINE_EXCEEDED")),
            1
        );
        assert_eq!(
            recorder.counter(FAILED, &failure_labels("Commit", "DEADLINE_EXCEEDED")),
            1
        );
    }

    #[test]
    fn unclassified_errors_are_counted_as_unknown() {
        let recorder = TestRecorder::default();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let metrics = OperationMetrics::init(&meta());

        metrics.record_operation_failure("Rollback", &YdbError::custom("boom"), &meta());

        assert_eq!(
            recorder.counter(FAILED, &failure_labels("Rollback", "UNKNOWN")),
            1
        );
    }
}
