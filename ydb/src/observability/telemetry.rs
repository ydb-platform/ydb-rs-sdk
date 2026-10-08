use std::ops::Deref;

use crate::observability::metrics::YdbMetrics;

#[derive(Clone)]
pub(crate) struct TelemetryMeta {
    pub(crate) database: String,
    pub(crate) endpoint: String,
}

impl TelemetryMeta {
    pub(crate) fn new(database: String, endpoint: String) -> Self {
        Self { database, endpoint }
    }
}

#[derive(Clone)]
pub(crate) struct Telemetry {
    pub(crate) meta: TelemetryMeta,
    pub(crate) metrics: YdbMetrics,
}

impl Telemetry {
    pub(crate) fn new(meta: &TelemetryMeta) -> Self {
        Telemetry {
            meta: meta.clone(),
            metrics: YdbMetrics::init(meta),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test() -> Self {
        Self::new(&TelemetryMeta::new(
            "test".to_string(),
            "localhost:2135".to_string(),
        ))
    }
}

impl Deref for Telemetry {
    type Target = YdbMetrics;

    fn deref(&self) -> &Self::Target {
        &self.metrics
    }
}
