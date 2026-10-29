//! The only production module coupled to the external `metrics` crate.

use std::sync::Arc;

use super::backend::{
    Counter, CounterHandle, Gauge, GaugeHandle, Histogram, HistogramHandle, MetricDescription,
    MetricLabel, MetricUnit, MetricsBackend,
};

pub(crate) struct MetricsFacadeBackend {
    recorder: Option<Arc<dyn metrics::Recorder + Send + Sync>>,
}

impl MetricsFacadeBackend {
    pub(crate) fn ambient() -> Self {
        Self { recorder: None }
    }

    #[cfg(test)]
    pub(crate) fn local(recorder: Arc<dyn metrics::Recorder + Send + Sync>) -> Self {
        Self {
            recorder: Some(recorder),
        }
    }

    fn record<T>(&self, record: impl FnOnce() -> T) -> T {
        match self.recorder.as_deref() {
            Some(recorder) => metrics::with_local_recorder(recorder, record),
            None => record(),
        }
    }
}

struct FacadeCounter(metrics::Counter);

impl Counter for FacadeCounter {
    fn increment(&self, value: u64) {
        self.0.increment(value);
    }
}

struct FacadeGauge(metrics::Gauge);

impl Gauge for FacadeGauge {
    fn increment(&self, value: f64) {
        self.0.increment(value);
    }

    fn decrement(&self, value: f64) {
        self.0.decrement(value);
    }

    fn set(&self, value: f64) {
        self.0.set(value);
    }
}

struct FacadeHistogram(metrics::Histogram);

impl Histogram for FacadeHistogram {
    fn record(&self, value: f64) {
        self.0.record(value);
    }
}

fn counter_handle(handle: metrics::Counter) -> CounterHandle {
    Arc::new(FacadeCounter(handle))
}

fn gauge_handle(handle: metrics::Gauge) -> GaugeHandle {
    Arc::new(FacadeGauge(handle))
}

fn histogram_handle(handle: metrics::Histogram) -> HistogramHandle {
    Arc::new(FacadeHistogram(handle))
}

fn unit(unit: MetricUnit) -> metrics::Unit {
    match unit {
        MetricUnit::Count => metrics::Unit::Count,
        MetricUnit::Seconds => metrics::Unit::Seconds,
        MetricUnit::Bytes => metrics::Unit::Bytes,
    }
}

fn labels(labels: &[MetricLabel]) -> Vec<metrics::Label> {
    labels
        .iter()
        .map(|label| {
            let key = match &label.key {
                std::borrow::Cow::Borrowed(value) => metrics::SharedString::from(*value),
                std::borrow::Cow::Owned(value) => metrics::SharedString::from(value.clone()),
            };
            let value = match &label.value {
                std::borrow::Cow::Borrowed(value) => metrics::SharedString::from(*value),
                std::borrow::Cow::Owned(value) => metrics::SharedString::from(value.clone()),
            };
            metrics::Label::new(key, value)
        })
        .collect()
}

impl MetricsBackend for MetricsFacadeBackend {
    fn register_counter(
        &self,
        description: &MetricDescription,
        series_labels: &[MetricLabel],
    ) -> Arc<dyn Counter> {
        counter_handle(self.record(|| {
            let name = description.name().to_string();
            let help = description.description().to_string();
            let labels = labels(series_labels);
            metrics::counter!(description: help, unit: unit(description.unit()), name.clone(), labels.iter())
        }))
    }

    fn register_gauge(
        &self,
        description: &MetricDescription,
        series_labels: &[MetricLabel],
    ) -> Arc<dyn Gauge> {
        gauge_handle(self.record(|| {
            let name = description.name().to_string();
            let help = description.description().to_string();
            let labels = labels(series_labels);
            metrics::gauge!(description: help, unit: unit(description.unit()), name.clone(), labels.iter())
        }))
    }

    fn register_histogram(
        &self,
        description: &MetricDescription,
        series_labels: &[MetricLabel],
    ) -> Arc<dyn Histogram> {
        histogram_handle(self.record(|| {
            let name = description.name().to_string();
            let help = description.description().to_string();
            let labels = labels(series_labels);
            metrics::histogram!(description: help, unit: unit(description.unit()), name.clone(), labels.iter())
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    struct CountingRecorder {
        counter_registrations: AtomicUsize,
    }

    impl metrics::Recorder for CountingRecorder {
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
            _key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            self.counter_registrations.fetch_add(1, Ordering::SeqCst);
            metrics::Counter::noop()
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
            _key: &metrics::Key,
            _metadata: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    fn description() -> MetricDescription {
        MetricDescription::new("adapter_test", MetricUnit::Count, "Adapter test")
    }

    #[test]
    fn default_backend_uses_ambient_metrics_recorder() {
        let recorder = CountingRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            super::super::default_backend()
                .register_counter(&description(), &[])
                .increment(1);
        });

        assert_eq!(recorder.counter_registrations.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn local_backend_uses_explicit_metrics_recorder() {
        let recorder = Arc::new(CountingRecorder::default());
        let backend = MetricsFacadeBackend::local(recorder.clone());
        backend.register_counter(&description(), &[]).increment(1);

        assert_eq!(recorder.counter_registrations.load(Ordering::SeqCst), 1);
    }
}
