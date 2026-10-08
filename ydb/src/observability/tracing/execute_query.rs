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
