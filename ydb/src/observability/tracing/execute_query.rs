use std::time::Instant;
use tracing::{Span, field, info_span};

use crate::errors::YdbError;
use crate::observability::telemetry::Telemetry;

pub(crate) struct ExecuteQueryCompositeSpan {
    operation_span: Span,
    attached_spans: Vec<Span>,
    telemetry: Telemetry,
    started_at: Instant,
    finished: bool,
}

impl ExecuteQueryCompositeSpan {
    pub(crate) fn new(telemetry: Telemetry, session_id: Option<&str>, query_text: &str) -> Self {
        let span = info_span!(
            "ydb.ExecuteQuery",
            db.system.name = "ydb",
            db.namespace = %telemetry.meta.database,
            server.address = %telemetry.meta.endpoint,
            db.query.text = %query_text,
            ydb.session.id = field::Empty,
            error.message = field::Empty,
            db.response.status_code = field::Empty,
        );
        if let Some(session_id) = session_id {
            span.record("ydb.session.id", session_id);
        }

        Self {
            operation_span: span,
            attached_spans: Vec::new(),
            telemetry,
            started_at: Instant::now(),
            finished: false,
        }
    }

    pub(crate) fn attach(&mut self, span: Span) {
        if !self.finished {
            self.attached_spans.push(span);
        }
    }

    pub(crate) fn operation_span(&self) -> Span {
        self.operation_span.clone()
    }

    pub(crate) fn finish_success(&mut self) {
        self.finish(None);
    }

    pub(crate) fn finish_error(&mut self, error: &YdbError) {
        self.finish(Some(error));
    }

    pub(crate) fn finish_cancelled(&mut self) {
        self.finish(None);
    }

    fn finish(&mut self, error: Option<&YdbError>) {
        if self.finished {
            return;
        }
        self.finished = true;

        if let Some(error) = error {
            for span in self.attached_spans.as_slice() {
                span.record("error.type", "ydb_error");
                span.record("db.response.status_code", error.status_code());
            }

            self.operation_span.record("error.type", "ydb_error");
            self.operation_span
                .record("db.response.status_code", error.status_code());
        }

        let duration = self.started_at.elapsed();
        self.telemetry
            .metrics
            .operation_metrics
            .record_execute_query_duration(duration);
        if let Some(error) = error {
            self.telemetry
                .metrics
                .operation_metrics
                .record_operation_failure("ExecuteQuery", error, &self.telemetry.meta);
        }

        touch(&self.operation_span);
        for span in &self.attached_spans {
            touch(span);
        }

        self.operation_span = Span::none();
        self.attached_spans.clear();
    }
}

fn touch(span: &Span) {
    let _entered = span.enter();
    drop(_entered);
}

impl Drop for ExecuteQueryCompositeSpan {
    fn drop(&mut self) {
        self.finish(None);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::YdbError;
    use crate::observability::metrics::mocks::{Labels, TestRecorder};
    use crate::observability::telemetry::{Telemetry, TelemetryMeta};
    use crate::observability::tracing::execute_query::ExecuteQueryCompositeSpan;
    use tracing::{Subscriber, info_span, span};
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::{Layer, Registry};

    #[derive(Clone, Default)]
    struct TestCloseTracker(Arc<Mutex<Vec<span::Id>>>);

    impl TestCloseTracker {
        fn close_times(&self, id: &span::Id) -> usize {
            self.0
                .lock()
                .unwrap()
                .iter()
                .filter(|s_id| *s_id == id)
                .count()
        }
    }

    impl<S: Subscriber> Layer<S> for TestCloseTracker {
        fn on_close(&self, id: span::Id, _ctx: Context<'_, S>) {
            self.0.lock().unwrap().push(id);
        }
    }

    const OPERATION_DURATION: &str = "ydb.client.operation.duration";
    const FAILED_OPERATIONS: &str = "ydb.client.operation.failed";

    fn meta() -> TelemetryMeta {
        TelemetryMeta::new("/local".to_string(), "localhost:2135".to_string())
    }

    #[test]
    fn should_close_attached_spans() {
        let tracker = TestCloseTracker::default();
        let _subscriber =
            tracing::subscriber::set_default(Registry::default().with(tracker.clone()));

        let telemetry = Telemetry::new_for_test();
        let mut composite_span =
            ExecuteQueryCompositeSpan::new(telemetry, None, "select * from ydb");

        let parent_span = info_span!("RunWithRetry");
        let child_span = info_span!(parent: &parent_span, "Try");
        composite_span.attach(parent_span.clone());
        composite_span.attach(child_span.clone());

        let operation_span = composite_span.operation_span();
        assert_eq!(
            operation_span.metadata().unwrap().name(),
            "ydb.ExecuteQuery"
        );

        let operation_id = operation_span.id().unwrap();
        let parent_id = parent_span.id().unwrap();
        let child_id = child_span.id().unwrap();

        drop(operation_span);
        drop(parent_span);
        drop(child_span);

        assert_eq!(tracker.close_times(&operation_id), 0);
        assert_eq!(tracker.close_times(&parent_id), 0);
        assert_eq!(tracker.close_times(&child_id), 0);

        composite_span.finish_success();

        assert_eq!(tracker.close_times(&operation_id), 1);
        assert_eq!(tracker.close_times(&parent_id), 1);
        assert_eq!(tracker.close_times(&child_id), 1);
    }

    fn operation_labels() -> Labels {
        vec![
            ("database".to_string(), "/local".to_string()),
            ("endpoint".to_string(), "localhost:2135".to_string()),
            ("operation".to_string(), "ExecuteQuery".to_string()),
        ]
    }

    fn failure_labels(status_code: &str) -> Labels {
        let mut labels = operation_labels();
        labels.push(("status_code".to_string(), status_code.to_string()));
        labels
    }

    fn new_composite_span() -> ExecuteQueryCompositeSpan {
        ExecuteQueryCompositeSpan::new(Telemetry::new(&meta()), None, "select * from ydb")
    }

    #[test]
    fn should_close_spans_and_record_duration_on_success() {
        let tracker = TestCloseTracker::default();
        let _subscriber =
            tracing::subscriber::set_default(Registry::default().with(tracker.clone()));
        let recorder = TestRecorder::default();
        let _recorder = metrics::set_default_local_recorder(&recorder);

        let mut composite_span = new_composite_span();
        let operation_id = composite_span.operation_span().id().unwrap();
        let attached_span = info_span!("Try");
        let attached_id = attached_span.id().unwrap();
        composite_span.attach(attached_span);

        assert_eq!(tracker.close_times(&operation_id), 0);
        assert_eq!(tracker.close_times(&attached_id), 0);

        composite_span.finish_success();

        assert_eq!(tracker.close_times(&operation_id), 1);
        assert_eq!(tracker.close_times(&attached_id), 1);
        assert_eq!(recorder.histogram(OPERATION_DURATION, &operation_labels()).len(), 1);
        assert_eq!(recorder.counter(FAILED_OPERATIONS, &failure_labels("UNKNOWN")), 0);
    }

    #[test]
    fn should_record_duration_and_failure_on_error() {
        let recorder = TestRecorder::default();
        let _recorder = metrics::set_default_local_recorder(&recorder);

        let mut composite_span = new_composite_span();
        composite_span.finish_error(&YdbError::DeadlineExceeded);

        assert_eq!(recorder.histogram(OPERATION_DURATION, &operation_labels()).len(), 1);
        assert_eq!(
            recorder.counter(FAILED_OPERATIONS, &failure_labels("DEADLINE_EXCEEDED")),
            1
        );
    }

    #[test]
    fn should_record_duration_without_failure_on_cancel() {
        let recorder = TestRecorder::default();
        let _recorder = metrics::set_default_local_recorder(&recorder);

        let mut composite_span = new_composite_span();
        composite_span.finish_cancelled();

        assert_eq!(recorder.histogram(OPERATION_DURATION, &operation_labels()).len(), 1);
        assert_eq!(
            recorder.counter(FAILED_OPERATIONS, &failure_labels("DEADLINE_EXCEEDED")),
            0
        );
    }

    #[test]
    fn should_ignore_repeated_finish() {
        let recorder = TestRecorder::default();
        let _recorder = metrics::set_default_local_recorder(&recorder);

        let mut composite_span = new_composite_span();
        composite_span.finish_success();
        composite_span.finish_error(&YdbError::DeadlineExceeded);
        drop(composite_span);

        assert_eq!(recorder.histogram(OPERATION_DURATION, &operation_labels()).len(), 1);
        assert_eq!(
            recorder.counter(FAILED_OPERATIONS, &failure_labels("DEADLINE_EXCEEDED")),
            0
        );
    }

    #[test]
    fn should_finish_on_drop() {
        let tracker = TestCloseTracker::default();
        let _subscriber =
            tracing::subscriber::set_default(Registry::default().with(tracker.clone()));
        let recorder = TestRecorder::default();
        let _recorder = metrics::set_default_local_recorder(&recorder);

        let composite_span = new_composite_span();
        let operation_id = composite_span.operation_span().id().unwrap();
        drop(composite_span);

        assert_eq!(tracker.close_times(&operation_id), 1);
        assert_eq!(recorder.histogram(OPERATION_DURATION, &operation_labels()).len(), 1);
    }

    #[test]
    fn should_not_keep_spans_attached_after_finish() {
        let tracker = TestCloseTracker::default();
        let _subscriber =
            tracing::subscriber::set_default(Registry::default().with(tracker.clone()));
        let recorder = TestRecorder::default();
        let _recorder = metrics::set_default_local_recorder(&recorder);

        let mut composite_span = new_composite_span();
        composite_span.finish_success();

        let late_span = info_span!("Try");
        let late_id = late_span.id().unwrap();
        composite_span.attach(late_span);

        assert_eq!(tracker.close_times(&late_id), 1);
    }
}
