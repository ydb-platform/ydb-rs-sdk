use std::sync::Arc;

use metrics::{Counter, Histogram};

pub(crate) mod names;

/// Access to the SDK's individual metric handles.
///
/// SDK components record through the read-only handle getters only; they never touch
/// metric names or label sets directly. The default implementation,
/// [`DefaultMetricsRecorder`], registers its handles into a `metrics` backend — the
/// ambient/global recorder or a caller-provided one — at construction time.
///
/// Alternative implementations may map the getters onto their own metric names,
/// labels, and buckets without changing component code. Handles are internally
/// atomic, so `&self` getters need no interior mutability and components can share
/// an `Arc<dyn MetricsRecorder>` without locks.
pub trait MetricsRecorder: Send + Sync {
    fn client_new_counter(&self) -> &Counter;
    fn client_new_table_client_counter(&self) -> &Counter;
    fn client_new_query_client_counter(&self) -> &Counter;
    fn client_new_scheme_client_counter(&self) -> &Counter;
    fn client_new_topic_client_counter(&self) -> &Counter;
    fn client_query_row_counter(&self) -> &Counter;
    fn client_transaction_query_row_counter(&self) -> &Counter;
    fn client_transaction_exec_counter(&self) -> &Counter;
    fn client_transaction_commit_counter(&self) -> &Counter;
    fn client_transaction_rollback_counter(&self) -> &Counter;
    fn client_row_query_time_histogram(&self) -> &Histogram;
    fn client_transaction_row_query_time_histogram(&self) -> &Histogram;
}

/// Default [`MetricsRecorder`] implementation.
///
/// Owns the metric handles created from a driver name and static labels (see
/// [`crate::ClientBuilder::with_driver_name`] and
/// [`crate::ClientBuilder::with_metrics_label`]), optionally registering them into a
/// caller-provided `metrics::Recorder` backend which is kept alive here. Metric
/// names, labels, and buckets are an internal detail: components only reach the
/// handles through the [`MetricsRecorder`] getters.
pub struct DefaultMetricsRecorder {
    names: names::MetricsNames,
    /// Keeps a caller-provided backend recorder alive: handles above reference it.
    _backend: Option<Arc<dyn metrics::Recorder + Send + Sync>>,
}

impl DefaultMetricsRecorder {
    /// Recorder over the ambient/global `metrics` recorder with default settings
    /// (driver name `main`, no extra labels).
    pub fn new() -> Self {
        Self::from_parts(None, Vec::new(), None)
    }

    /// Recorder registering SDK metrics into a caller-provided `metrics` backend.
    ///
    /// Use [`Self::from_parts`] to also set the driver name and static labels.
    pub fn with_backend(recorder: Arc<dyn metrics::Recorder + Send + Sync>) -> Self {
        Self::from_parts(None, Vec::new(), Some(recorder))
    }

    /// Recorder with explicit driver name, extra static labels, and optional backend.
    ///
    /// This is the constructor used by [`crate::ClientBuilder::build`]; the parts
    /// mirror the builder's `driver_name`, metrics labels, and metrics recorder
    /// settings. Without a backend, handles bind to the ambient/global recorder.
    pub fn from_parts(
        driver_name: Option<String>,
        extra_labels: Vec<(String, String)>,
        backend: Option<Arc<dyn metrics::Recorder + Send + Sync>>,
    ) -> Self {
        Self {
            names: names::MetricsNames::new(driver_name, extra_labels, backend.as_deref()),
            _backend: backend,
        }
    }
}

impl Default for DefaultMetricsRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRecorder for DefaultMetricsRecorder {
    fn client_new_counter(&self) -> &Counter {
        &self.names.client_new_counter
    }

    fn client_new_table_client_counter(&self) -> &Counter {
        &self.names.client_new_table_client_counter
    }

    fn client_new_query_client_counter(&self) -> &Counter {
        &self.names.client_new_query_client_counter
    }

    fn client_new_scheme_client_counter(&self) -> &Counter {
        &self.names.client_new_scheme_client_counter
    }

    fn client_new_topic_client_counter(&self) -> &Counter {
        &self.names.client_new_topic_client_counter
    }

    fn client_query_row_counter(&self) -> &Counter {
        &self.names.client_query_row_counter
    }

    fn client_transaction_query_row_counter(&self) -> &Counter {
        &self.names.client_transaction_query_row_counter
    }

    fn client_transaction_exec_counter(&self) -> &Counter {
        &self.names.client_transaction_exec_counter
    }

    fn client_transaction_commit_counter(&self) -> &Counter {
        &self.names.client_transaction_commit_counter
    }

    fn client_transaction_rollback_counter(&self) -> &Counter {
        &self.names.client_transaction_rollback_counter
    }

    fn client_row_query_time_histogram(&self) -> &Histogram {
        &self.names.client_row_query_time_histogram
    }

    fn client_transaction_row_query_time_histogram(&self) -> &Histogram {
        &self.names.client_transaction_row_query_time_histogram
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Default)]
    struct CapturedMetric {
        name: String,
        labels: Vec<(String, String)>,
    }

    /// Minimal `metrics::Recorder` capturing counter/histogram handle registrations.
    #[derive(Default)]
    struct CaptureRecorder {
        registered: Mutex<Vec<CapturedMetric>>,
    }

    impl CaptureRecorder {
        fn push(&self, key: &metrics::Key) {
            let labels = key
                .labels()
                .map(|label| (label.key().to_string(), label.value().to_string()))
                .collect();
            self.registered.lock().unwrap().push(CapturedMetric {
                name: key.name().to_string(),
                labels,
            });
        }

        fn registered(&self) -> Vec<CapturedMetric> {
            self.registered.lock().unwrap().clone()
        }
    }

    impl metrics::Recorder for CaptureRecorder {
        fn describe_counter(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn describe_gauge(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn describe_histogram(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn register_counter(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> Counter {
            self.push(key);
            Counter::noop()
        }

        fn register_gauge(
            &self,
            _key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Gauge {
            metrics::Gauge::noop()
        }

        fn register_histogram(
            &self,
            key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> Histogram {
            self.push(key);
            Histogram::noop()
        }
    }

    fn expected_metric_names() -> Vec<&'static str> {
        vec![
            "ydb_new_client_counter",
            "ydb_new_table_client_counter",
            "ydb_new_query_client_counter",
            "ydb_new_scheme_client_counter",
            "ydb_new_topic_client_counter",
            "ydb_client_query_row_counter",
            "ydb_client_transaction_query_row_counter",
            "ydb_client_transaction_exec_counter",
            "ydb_client_transaction_commit_counter",
            "ydb_client_transaction_rollback_counter",
            "ydb_row_query_time_histogram",
            "ydb_transaction_row_query_time_histogram",
        ]
    }

    #[test]
    fn default_recorder_registers_part1_series_with_labels() {
        let capture = Arc::new(CaptureRecorder::default());
        let recorder = DefaultMetricsRecorder::from_parts(
            Some("driver-a".to_string()),
            vec![("env".to_string(), "prod".to_string())],
            Some(Arc::clone(&capture) as Arc<dyn metrics::Recorder + Send + Sync>),
        );

        let _ = Arc::new(recorder);

        let mut registered = capture.registered();
        registered.sort_by(|a, b| a.name.cmp(&b.name));
        let mut expected = expected_metric_names();
        expected.sort_unstable();
        assert_eq!(
            registered
                .iter()
                .map(|m| m.name.as_str())
                .collect::<Vec<_>>(),
            expected,
            "all part1 series must register unchanged"
        );

        for metric in &registered {
            assert_eq!(
                metric.labels,
                vec![
                    ("driver_name".to_string(), "driver-a".to_string()),
                    ("env".to_string(), "prod".to_string()),
                ],
                "metric {} must keep the driver_name label first and static labels after",
                metric.name
            );
        }
    }

    #[test]
    fn default_recorder_without_driver_name_uses_main() {
        let capture = Arc::new(CaptureRecorder::default());
        let _recorder = DefaultMetricsRecorder::from_parts(
            None,
            Vec::new(),
            Some(Arc::clone(&capture) as Arc<dyn metrics::Recorder + Send + Sync>),
        );

        for metric in capture.registered() {
            assert_eq!(
                metric.labels,
                vec![("driver_name".to_string(), "main".to_string())],
                "metric {} must default to driver_name=main",
                metric.name
            );
        }
    }

    #[test]
    fn default_recorder_with_backend_registers_into_backend() {
        let capture = Arc::new(CaptureRecorder::default());
        let recorder: Arc<dyn MetricsRecorder> = Arc::new(DefaultMetricsRecorder::with_backend(
            Arc::clone(&capture) as Arc<dyn metrics::Recorder + Send + Sync>,
        ));

        recorder.client_new_counter().increment(1);

        assert_eq!(
            capture
                .registered()
                .iter()
                .filter(|m| m.name == "ydb_new_client_counter")
                .count(),
            1,
            "with_backend must register handles into the provided backend"
        );
    }

    /// Test [`MetricsRecorder`] implementation: counts getter accesses, proving the
    /// trait is object safe and usable through `Arc<dyn MetricsRecorder>`.
    struct CountingRecorder {
        accesses: AtomicUsize,
        counter: Counter,
        histogram: Histogram,
    }

    impl CountingRecorder {
        fn new() -> Self {
            Self {
                accesses: AtomicUsize::new(0),
                counter: Counter::noop(),
                histogram: Histogram::noop(),
            }
        }

        fn counter_getter(&self) -> &Counter {
            self.accesses.fetch_add(1, Ordering::SeqCst);
            &self.counter
        }

        fn histogram_getter(&self) -> &Histogram {
            self.accesses.fetch_add(1, Ordering::SeqCst);
            &self.histogram
        }
    }

    impl MetricsRecorder for CountingRecorder {
        fn client_new_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_new_table_client_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_new_query_client_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_new_scheme_client_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_new_topic_client_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_query_row_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_transaction_query_row_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_transaction_exec_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_transaction_commit_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_transaction_rollback_counter(&self) -> &Counter {
            self.counter_getter()
        }

        fn client_row_query_time_histogram(&self) -> &Histogram {
            self.histogram_getter()
        }

        fn client_transaction_row_query_time_histogram(&self) -> &Histogram {
            self.histogram_getter()
        }
    }

    #[test]
    fn custom_recorder_works_through_trait_object() {
        let counting = Arc::new(CountingRecorder::new());
        let recorder: Arc<dyn MetricsRecorder> = counting.clone();

        recorder.client_new_counter().increment(1);
        recorder.client_transaction_commit_counter().increment(1);
        recorder.client_row_query_time_histogram().record(1.0);

        assert_eq!(counting.accesses.load(Ordering::SeqCst), 3);
    }
}
