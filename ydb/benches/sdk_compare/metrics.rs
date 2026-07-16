use std::time::Duration;

use anyhow::{Context, Result};
use hdrhistogram::Histogram;
use serde::Serialize;

const LOWEST_LATENCY_US: u64 = 1;
const HIGHEST_LATENCY_US: u64 = 300_000_000;
const SIGNIFICANT_DIGITS: u8 = 3;

pub(crate) struct LatencyRecorder {
    histogram: Histogram<u64>,
}

impl LatencyRecorder {
    pub(crate) fn new() -> Result<Self> {
        let histogram =
            Histogram::new_with_bounds(LOWEST_LATENCY_US, HIGHEST_LATENCY_US, SIGNIFICANT_DIGITS)
                .context("failed to create latency histogram")?;
        Ok(Self { histogram })
    }

    pub(crate) fn record(&mut self, latency: Duration) -> Result<()> {
        let latency_us = latency.as_micros().max(1);
        let latency_us = u64::try_from(latency_us).context("latency does not fit into u64")?;
        self.histogram
            .record(latency_us)
            .with_context(|| format!("latency {latency_us} us is outside histogram bounds"))
    }

    pub(crate) fn merge(&mut self, other: &Self) -> Result<()> {
        self.histogram
            .add(&other.histogram)
            .context("failed to merge latency histograms")
    }

    pub(crate) fn count(&self) -> u64 {
        self.histogram.len()
    }

    pub(crate) fn summary(&self) -> LatencyMetric {
        let count = self.count();
        let latency_us = (count > 0).then(|| LatencySummary {
            min: self.histogram.min(),
            max: self.histogram.max(),
            mean: self.histogram.mean(),
            p50: self.histogram.value_at_quantile(0.5),
            p95: self.histogram.value_at_quantile(0.95),
            p99: self.histogram.value_at_quantile(0.99),
            p99_9: self.histogram.value_at_quantile(0.999),
        });

        LatencyMetric { count, latency_us }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct LatencyMetric {
    pub(crate) count: u64,
    pub(crate) latency_us: Option<LatencySummary>,
}

#[derive(Debug, Serialize)]
pub(crate) struct LatencySummary {
    pub(crate) min: u64,
    pub(crate) max: u64,
    pub(crate) mean: f64,
    pub(crate) p50: u64,
    pub(crate) p95: u64,
    pub(crate) p99: u64,
    pub(crate) p99_9: u64,
}
