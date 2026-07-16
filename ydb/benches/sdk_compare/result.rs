use serde::Serialize;

use crate::config::Scenario;
use crate::metrics::LatencyMetric;

#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkResult {
    scenario: Scenario,
    implementation: Implementation,
    metrics: TopicMetrics,
}

impl BenchmarkResult {
    pub(crate) fn new(scenario: Scenario, metrics: TopicMetrics) -> Self {
        Self {
            scenario,
            implementation: Implementation::rust(),
            metrics,
        }
    }
}

#[derive(Debug, Serialize)]
struct Implementation {
    language: &'static str,
    sdk_version: &'static str,
    build_profile: &'static str,
}

impl Implementation {
    fn rust() -> Self {
        Self {
            language: "rust",
            sdk_version: env!("CARGO_PKG_VERSION"),
            build_profile: if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            },
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct TopicMetrics {
    #[serde(rename = "topic.write_ack")]
    pub(crate) write_ack: LatencyMetric,
    #[serde(rename = "topic.end_to_end")]
    pub(crate) end_to_end: LatencyMetric,
    #[serde(rename = "topic.commit_ack")]
    pub(crate) commit_ack: LatencyMetric,
    pub(crate) write_messages_per_second: f64,
    pub(crate) write_bytes_per_second: f64,
    pub(crate) read_messages_per_second: f64,
    pub(crate) read_bytes_per_second: f64,
}
