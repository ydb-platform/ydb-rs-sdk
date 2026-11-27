//! Public API checks for SDK-owned metric backend types.

use std::borrow::Cow;
use std::sync::Arc;

use ydb::{Counter, Gauge, Histogram, MetricDescription, MetricLabel, MetricUnit, MetricsBackend};

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

struct SdkOnlyBackend;

impl MetricsBackend for SdkOnlyBackend {
    fn register_counter(
        &self,
        _description: &MetricDescription,
        _labels: &[MetricLabel],
    ) -> Arc<dyn Counter> {
        Arc::new(NoopHandle)
    }

    fn register_gauge(
        &self,
        _description: &MetricDescription,
        _labels: &[MetricLabel],
    ) -> Arc<dyn Gauge> {
        Arc::new(NoopHandle)
    }

    fn register_histogram(
        &self,
        _description: &MetricDescription,
        _labels: &[MetricLabel],
    ) -> Arc<dyn Histogram> {
        Arc::new(NoopHandle)
    }
}

#[test]
fn description_accepts_static_and_owned_cow_strings() {
    let static_description =
        MetricDescription::new("static_metric", MetricUnit::Count, "Static description");
    let owned_description = MetricDescription::new(
        String::from("owned_metric"),
        MetricUnit::Count,
        Cow::Owned(String::from("Owned description")),
    );

    assert_eq!(static_description.name(), "static_metric");
    assert_eq!(static_description.unit(), MetricUnit::Count);
    assert_eq!(static_description.description(), "Static description");
    assert_eq!(owned_description.name(), "owned_metric");
    assert_eq!(owned_description.unit(), MetricUnit::Count);
    assert_eq!(owned_description.description(), "Owned description");
}

#[test]
fn labels_accept_static_and_owned_strings() {
    let labels = [
        MetricLabel::new("static_key", "static_value"),
        MetricLabel::new(String::from("owned_key"), String::from("owned_value")),
    ];

    assert_eq!(labels[0].key(), "static_key");
    assert_eq!(labels[0].value(), "static_value");
    assert_eq!(labels[1].key(), "owned_key");
    assert_eq!(labels[1].value(), "owned_value");
}

#[test]
fn sdk_only_backend_works_through_trait_objects() {
    let backend: Arc<dyn MetricsBackend> = Arc::new(SdkOnlyBackend);
    let description = MetricDescription::new("sdk_metric", MetricUnit::Count, "SDK-only backend");
    let labels = [MetricLabel::new("key", "value")];

    backend.register_counter(&description, &labels).increment(1);
    let gauge = backend.register_gauge(&description, &labels);
    gauge.increment(1.0);
    gauge.decrement(0.5);
    gauge.set(2.0);
    backend
        .register_histogram(&description, &labels)
        .record(1.0);
}
