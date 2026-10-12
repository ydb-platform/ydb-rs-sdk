use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use prometheus::core::{Atomic, AtomicF64};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct TestCounter(AtomicU64);

impl CounterFn for TestCounter {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::SeqCst);
    }

    fn absolute(&self, value: u64) {
        self.0.store(value, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct TestHistogram(Mutex<Vec<f64>>);

impl HistogramFn for TestHistogram {
    fn record(&self, value: f64) {
        self.0.lock().unwrap().push(value);
    }
}

struct TestGauge(AtomicF64);

impl Default for TestGauge {
    fn default() -> Self {
        TestGauge(AtomicF64::new(0.0))
    }
}

impl GaugeFn for TestGauge {
    fn increment(&self, value: f64) {
        self.0.inc_by(value);
    }

    fn decrement(&self, value: f64) {
        self.0.dec_by(value);
    }

    fn set(&self, value: f64) {
        self.0.set(value);
    }
}

#[derive(Default)]
pub(crate) struct TestRecorder {
    counters: Mutex<HashMap<Key, Arc<TestCounter>>>,
    histograms: Mutex<HashMap<Key, Arc<TestHistogram>>>,
    gauges: Mutex<HashMap<Key, Arc<TestGauge>>>,
}

impl Recorder for TestRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        let counter = self
            .counters
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .clone();
        Counter::from_arc(counter)
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        let gauge = self
            .gauges
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .clone();
        Gauge::from_arc(gauge)
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        let histogram = self
            .histograms
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .clone();
        Histogram::from_arc(histogram)
    }
}

impl TestRecorder {
    pub(crate) fn histogram(&self, name: &str, labels: &Labels) -> Vec<f64> {
        self.histograms
            .lock()
            .unwrap()
            .iter()
            .find(|(key, _)| key_matches(key, name, labels))
            .map(|(_, histogram)| histogram.0.lock().unwrap().clone())
            .unwrap_or_default()
    }

    pub(crate) fn counter(&self, name: &str, labels: &Labels) -> u64 {
        self.counters
            .lock()
            .unwrap()
            .iter()
            .find(|(key, _)| key_matches(key, name, labels))
            .map(|(_, counter)| counter.0.load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    #[allow(dead_code)]
    pub(crate) fn gauge(&self, name: &str, labels: &Labels) -> f64 {
        self.gauges
            .lock()
            .unwrap()
            .iter()
            .find(|(key, _)| key_matches(key, name, labels))
            .map(|(_, gauge)| gauge.0.get())
            .unwrap()
    }
}

pub(crate) type Labels = Vec<(String, String)>;

fn key_matches(key: &Key, name: &str, labels: &Labels) -> bool {
    let mut actual: Labels = key
        .labels()
        .map(|label| (label.key().to_string(), label.value().to_string()))
        .collect();
    let mut expected = labels.clone();
    actual.sort();
    expected.sort();
    key.name() == name && actual == expected
}
