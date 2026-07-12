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
    histograms: [HashMap<String, Histogram<u64>>; LATENCY_PERCENTILES.len()],
}

impl LatencySeries {
    pub(super) fn new() -> Self {
        Self {
            histograms: std::array::from_fn(|_| HashMap::new()),
        }
    }

    pub(super) fn record(&mut self, latency_micros: u64, attrs_key: String) {
        for histograms in &mut self.histograms {
            let histogram = histograms.entry(attrs_key.clone()).or_insert_with(|| {
                Histogram::new_with_bounds(
                    HDR_MIN_MICROSECONDS,
                    HDR_MAX_MICROSECONDS,
                    HDR_SIGNIFICANT_DIGITS,
                )
                .expect("valid hdr bounds")
            });
            histogram.record(latency_micros).ok();
        }
    }

    fn observations_for(&mut self, percentile_index: usize) -> Vec<(String, f64)> {
        let quantile = LATENCY_PERCENTILES[percentile_index].quantile;
        self.histograms[percentile_index]
            .iter_mut()
            .filter_map(|(attrs, histogram)| {
                if histogram.is_empty() {
                    return None;
                }

                let value = histogram.value_at_quantile(quantile) as f64 / 1_000_000.0;
                histogram.reset();

                Some((attrs.clone(), value))
            })
            .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    const ATTRS: &str = "ref=current;operation_type=read;operation_status=success";

    #[test]
    fn percentile_callbacks_reset_only_their_own_histogram() {
        let mut series = LatencySeries::new();
        series.record(100, ATTRS.to_string());

        series.observations_for(0);

        assert!(histogram(&series, 0).is_empty());
        assert_eq!(histogram(&series, 1).len(), 1);
        assert_eq!(histogram(&series, 2).len(), 1);

        series.record(200, ATTRS.to_string());

        assert_eq!(histogram(&series, 0).len(), 1);
        assert_eq!(histogram(&series, 1).len(), 2);
        assert_eq!(histogram(&series, 2).len(), 2);
    }

    fn histogram(series: &LatencySeries, percentile_index: usize) -> &Histogram<u64> {
        series.histograms[percentile_index]
            .get(ATTRS)
            .expect("histogram must exist")
    }
}
