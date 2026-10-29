use std::borrow::Cow;
use std::sync::Arc;

/// A monotonically increasing counter handle created by a [`MetricsBackend`].
pub trait Counter: Send + Sync {
    fn increment(&self, value: u64);
}

/// A gauge handle created by a [`MetricsBackend`].
pub trait Gauge: Send + Sync {
    fn increment(&self, value: f64);
    fn decrement(&self, value: f64);
    fn set(&self, value: f64);
}

/// A histogram handle created by a [`MetricsBackend`].
pub trait Histogram: Send + Sync {
    fn record(&self, value: f64);
}

/// Unit of values recorded into a metric.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub enum MetricUnit {
    Count,
    Seconds,
    Bytes,
}

/// Name, unit, and human-readable description of one metric.
///
/// The SDK constructs descriptions and passes them to [`MetricsBackend`] when
/// registering a handle. Labels are supplied separately in their stable order.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct MetricDescription {
    name: Cow<'static, str>,
    unit: MetricUnit,
    description: Cow<'static, str>,
}

impl MetricDescription {
    pub fn new(
        name: impl Into<Cow<'static, str>>,
        unit: MetricUnit,
        description: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self {
            name: name.into(),
            unit,
            description: description.into(),
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn unit(&self) -> MetricUnit {
        self.unit
    }

    pub fn description(&self) -> &str {
        self.description.as_ref()
    }
}

/// One metric label. Registration receives labels as an ordered slice.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct MetricLabel {
    pub(crate) key: Cow<'static, str>,
    pub(crate) value: Cow<'static, str>,
}

impl MetricLabel {
    pub fn new(key: impl Into<Cow<'static, str>>, value: impl Into<Cow<'static, str>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn key(&self) -> &str {
        self.key.as_ref()
    }

    pub fn value(&self) -> &str {
        self.value.as_ref()
    }
}

/// Factory for SDK metric handles.
///
/// Registration happens when a series is first created, not on the recording
/// hot path. Implementations that retain labels must clone them from the
/// provided slice.
pub trait MetricsBackend: Send + Sync {
    fn register_counter(
        &self,
        description: &MetricDescription,
        labels: &[MetricLabel],
    ) -> Arc<dyn Counter>;

    fn register_gauge(
        &self,
        description: &MetricDescription,
        labels: &[MetricLabel],
    ) -> Arc<dyn Gauge>;

    fn register_histogram(
        &self,
        description: &MetricDescription,
        labels: &[MetricLabel],
    ) -> Arc<dyn Histogram>;
}

#[cfg(test)]
impl<T> MetricsBackend for Arc<T>
where
    T: MetricsBackend + ?Sized,
{
    fn register_counter(
        &self,
        description: &MetricDescription,
        labels: &[MetricLabel],
    ) -> Arc<dyn Counter> {
        self.as_ref().register_counter(description, labels)
    }

    fn register_gauge(
        &self,
        description: &MetricDescription,
        labels: &[MetricLabel],
    ) -> Arc<dyn Gauge> {
        self.as_ref().register_gauge(description, labels)
    }

    fn register_histogram(
        &self,
        description: &MetricDescription,
        labels: &[MetricLabel],
    ) -> Arc<dyn Histogram> {
        self.as_ref().register_histogram(description, labels)
    }
}

pub(crate) type CounterHandle = Arc<dyn Counter>;
pub(crate) type GaugeHandle = Arc<dyn Gauge>;
pub(crate) type HistogramHandle = Arc<dyn Histogram>;

#[cfg(any(not(feature = "metrics"), test))]
mod noop {
    use super::*;

    struct NoopHandle;

    impl Counter for NoopHandle {
        fn increment(&self, _value: u64) {}
    }

    impl Gauge for NoopHandle {
        fn increment(&self, _value: f64) {}

        fn decrement(&self, _value: f64) {}

        fn set(&self, _value: f64) {}
    }

    impl Histogram for NoopHandle {
        fn record(&self, _value: f64) {}
    }

    pub(crate) fn noop_counter() -> CounterHandle {
        Arc::new(NoopHandle)
    }

    pub(crate) fn noop_gauge() -> GaugeHandle {
        Arc::new(NoopHandle)
    }

    pub(crate) fn noop_histogram() -> HistogramHandle {
        Arc::new(NoopHandle)
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) struct NoopMetricsBackend;

    #[cfg(not(feature = "metrics"))]
    impl MetricsBackend for NoopMetricsBackend {
        fn register_counter(
            &self,
            _description: &MetricDescription,
            _labels: &[MetricLabel],
        ) -> Arc<dyn Counter> {
            noop_counter()
        }

        fn register_gauge(
            &self,
            _description: &MetricDescription,
            _labels: &[MetricLabel],
        ) -> Arc<dyn Gauge> {
            noop_gauge()
        }

        fn register_histogram(
            &self,
            _description: &MetricDescription,
            _labels: &[MetricLabel],
        ) -> Arc<dyn Histogram> {
            noop_histogram()
        }
    }
}

#[cfg(not(feature = "metrics"))]
pub(crate) use noop::NoopMetricsBackend;
#[cfg(test)]
pub(crate) use noop::{noop_counter, noop_gauge, noop_histogram};
