use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use hdrhistogram::Histogram;
use opentelemetry::metrics::Meter;

const HDR_MIN_MICROSECONDS: u64 = 1;
const HDR_MAX_MICROSECONDS: u64 = 60_000_000;
const HDR_SIGNIFICANT_DIGITS: u8 = 5;

const LATENCY_PERCENTILES: [LatencyPercentile; 3] = [
    LatencyPercentile {
        metric_suffix: "p50",
        quantile: 0.50,
    },
    LatencyPercentile {
        metric_suffix: "p95",
        quantile: 0.95,
    },
    LatencyPercentile {
        metric_suffix: "p99",
        quantile: 0.99,
    },
];

struct LatencyPercentile {
    metric_suffix: &'static str,
    quantile: f64,
}

pub(super) struct LatencySeries {
    histograms: HashMap<String, Histogram<u64>>,
    // The first percentile callback snapshots and resets the histograms. The
    // remaining callbacks read that snapshot in any order while new samples
    // accumulate for the next collection.
    pending_snapshot: Option<LatencySnapshot>,
}

struct LatencySnapshot {
    percentiles: HashMap<String, [f64; LATENCY_PERCENTILES.len()]>,
    callbacks_remaining: usize,
}

impl LatencySeries {
    pub(super) fn new() -> Self {
        Self {
            histograms: HashMap::new(),
            pending_snapshot: None,
        }
    }

    pub(super) fn record(&mut self, latency_micros: u64, attrs_key: String) {
        let histogram = self.histograms.entry(attrs_key).or_insert_with(|| {
            Histogram::new_with_bounds(
                HDR_MIN_MICROSECONDS,
                HDR_MAX_MICROSECONDS,
                HDR_SIGNIFICANT_DIGITS,
            )
            .expect("valid hdr bounds")
        });
        histogram.record(latency_micros).ok();
    }

    fn observations_for(&mut self, percentile_index: usize) -> Vec<(String, f64)> {
        let mut snapshot = match self.pending_snapshot.take() {
            Some(snapshot) => snapshot,
            None => self.snapshot_and_reset(),
        };
        let observations = snapshot
            .percentiles
            .iter()
            .map(|(attrs, values)| (attrs.clone(), values[percentile_index]))
            .collect();

        if snapshot.callbacks_remaining > 1 {
            snapshot.callbacks_remaining -= 1;
            self.pending_snapshot = Some(snapshot);
        }

        observations
    }

    fn snapshot_and_reset(&mut self) -> LatencySnapshot {
        let percentiles = self
            .histograms
            .iter_mut()
            .filter_map(|(attrs, histogram)| {
                if histogram.is_empty() {
                    return None;
                }

                let values = LATENCY_PERCENTILES.map(|percentile| {
                    histogram.value_at_quantile(percentile.quantile) as f64 / 1_000_000.0
                });
                histogram.reset();

                Some((attrs.clone(), values))
            })
            .collect();

        LatencySnapshot {
            percentiles,
            callbacks_remaining: LATENCY_PERCENTILES.len(),
        }
    }
}

pub(super) fn register_gauges(
    meter: &Meter,
    series: Arc<Mutex<LatencySeries>>,
    metric_prefix: &'static str,
    description: &'static str,
) {
    for (index, percentile) in LATENCY_PERCENTILES.into_iter().enumerate() {
        let series = series.clone();
        meter
            .f64_observable_gauge(format!(
                "{metric_prefix}.{}.seconds",
                percentile.metric_suffix
            ))
            .with_description(format!(
                "{} {description} in seconds",
                percentile.metric_suffix.to_uppercase()
            ))
            .with_unit("s")
            .with_callback(move |observer| {
                let observations = series.lock().unwrap().observations_for(index);
                for (attrs_key, value) in observations {
                    observer.observe(value, &super::attrs_from_key(&attrs_key));
                }
            })
            .build();
    }
}
