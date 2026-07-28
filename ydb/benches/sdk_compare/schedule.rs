use std::time::{Duration, Instant};

use anyhow::{Context, Result};

#[derive(Clone, Copy)]
pub(crate) struct BenchmarkSchedule {
    origin: Instant,
    pub(crate) measurement_start: Instant,
    pub(crate) measurement_end: Instant,
    pub(crate) completion_deadline: Instant,
}

impl BenchmarkSchedule {
    pub(crate) fn new(
        warmup_duration: Duration,
        measurement_duration: Duration,
        drain_timeout: Duration,
    ) -> Result<Self> {
        let origin = Instant::now();
        let measurement_start = origin
            .checked_add(warmup_duration)
            .context("warm-up deadline overflowed")?;
        let measurement_end = measurement_start
            .checked_add(measurement_duration)
            .context("measurement deadline overflowed")?;
        let completion_deadline = measurement_end
            .checked_add(drain_timeout)
            .context("measurement drain deadline overflowed")?;

        Ok(Self {
            origin,
            measurement_start,
            measurement_end,
            completion_deadline,
        })
    }

    pub(crate) fn is_measurement_instant(&self, instant: Instant) -> bool {
        instant >= self.measurement_start && instant < self.measurement_end
    }

    pub(crate) fn ns_at(&self, instant: Instant) -> Result<u64> {
        let elapsed = instant
            .checked_duration_since(self.origin)
            .context("benchmark instant is before schedule origin")?;
        u64::try_from(elapsed.as_nanos())
            .context("benchmark schedule does not fit into u64 nanoseconds")
    }

    pub(crate) fn now_ns(&self) -> Result<u64> {
        self.ns_at(Instant::now())
    }
}
