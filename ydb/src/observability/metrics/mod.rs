use crate::observability::metrics::operation_metrics::OperationMetrics;
use crate::observability::telemetry::TelemetryMeta;

pub mod operation_metrics;
pub mod pool_metrics;

#[derive(Clone)]
pub(crate) struct YdbMetrics {
    pub operation_metrics: OperationMetrics,
}

impl YdbMetrics {
    pub(crate) fn init(meta: &TelemetryMeta) -> Self {
        YdbMetrics {
            operation_metrics: OperationMetrics::init(meta),
        }
    }
}
