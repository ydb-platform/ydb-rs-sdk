use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hdrhistogram::Histogram;
use opentelemetry::metrics::Meter;

const HDR_SIGNIFICANT_DIGITS: u8 = 3;

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
    // OpenTelemetry 0.27 invokes all registered callbacks sequentially during
    // one collection. The first callback snapshots and resets the histograms;
    // the remaining callbacks read that snapshot while new samples accumulate
    // for the next collection.
    pending_snapshot: Option<LatencySnapshot>,
    recording_error: Option<String>,
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
            recording_error: None,
        }
    }

    pub(super) fn record(&mut self, latency: Duration, attrs_key: String) {
        if self.recording_error.is_some() {
            return;
        }

        if let Err(error) = self.try_record(latency, attrs_key) {
            self.fail(error);
        }
    }

    pub(super) fn recording_error(&self) -> Option<&str> {
        self.recording_error.as_deref()
    }

    pub(super) fn fail(&mut self, error: impl Into<String>) {
        if self.recording_error.is_none() {
            self.recording_error = Some(error.into());
        }
    }

    fn try_record(&mut self, latency: Duration, attrs_key: String) -> Result<(), String> {
        let latency_micros = u64::try_from(latency.as_micros())
            .map_err(|_| "latency does not fit into u64 microseconds".to_string())?;
        let histogram = match self.histograms.entry(attrs_key) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let histogram = Histogram::new(HDR_SIGNIFICANT_DIGITS)
                    .map_err(|error| format!("create latency histogram: {error}"))?;
                entry.insert(histogram)
            }
        };

        histogram
            .record(latency_micros)
            .map_err(|error| format!("record latency: {error}"))
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
                let observations = match series.lock() {
                    Ok(mut series) => series.observations_for(index),
                    Err(poisoned) => {
                        poisoned
                            .into_inner()
                            .fail("latency histogram lock is poisoned");
                        Vec::new()
                    }
                };
                for (attrs_key, value) in observations {
                    observer.observe(value, &super::attrs_from_key(&attrs_key));
                }
            })
            .build();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ATTRS: &str = "ref=current;operation_type=read;operation_status=success";

    #[test]
    fn percentile_callbacks_share_one_snapshot() {
        let mut series = LatencySeries::new();
        series.record(Duration::from_micros(100), ATTRS.to_string());

        assert_eq!(observation(&mut series, 0), 0.000_1);
        series.record(Duration::from_micros(200), ATTRS.to_string());
        assert_eq!(observation(&mut series, 1), 0.000_1);
        assert_eq!(observation(&mut series, 2), 0.000_1);

        assert_eq!(observation(&mut series, 0), 0.000_2);
    }

    #[test]
    fn latency_above_previous_limit_is_recorded() {
        let mut series = LatencySeries::new();
        series.record(Duration::from_secs(120), ATTRS.to_string());

        let latency = observation(&mut series, 2);
        assert!((latency - 120.0).abs() < 0.1, "recorded {latency}");
        assert_eq!(series.recording_error(), None);
    }

    #[test]
    fn unrepresentable_latency_is_reported() {
        let mut series = LatencySeries::new();
        series.record(Duration::MAX, ATTRS.to_string());

        assert_eq!(
            series.recording_error(),
            Some("latency does not fit into u64 microseconds")
        );
    }

    fn observation(series: &mut LatencySeries, percentile_index: usize) -> f64 {
        series
            .observations_for(percentile_index)
            .into_iter()
            .find_map(|(attrs, value)| (attrs == ATTRS).then_some(value))
            .expect("latency observation must exist")
    }
}
