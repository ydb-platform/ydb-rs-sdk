use serde::Serialize;

use crate::config::Scenario;
use crate::metrics::LatencyMetric;

#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkResult {
    scenario: Scenario,
    implementation: Implementation,
    metrics: BenchmarkMetrics,
}

impl BenchmarkResult {
    pub(crate) fn topic(scenario: Scenario, metrics: TopicMetrics) -> Self {
        Self {
            scenario,
            implementation: Implementation::rust(),
            metrics: BenchmarkMetrics::Topic(metrics),
        }
    }

    pub(crate) fn query(scenario: Scenario, metrics: QueryMetrics) -> Self {
        Self {
            scenario,
            implementation: Implementation::rust(),
            metrics: BenchmarkMetrics::Query(metrics),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum BenchmarkMetrics {
    Topic(TopicMetrics),
    Query(QueryMetrics),
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

#[derive(Debug, Serialize)]
pub(crate) struct QueryMetrics {
    #[serde(rename = "query.execute")]
    pub(crate) execute: LatencyMetric,
    pub(crate) queries_per_second: f64,
    pub(crate) rows_per_second: f64,
    pub(crate) payload_bytes_per_second: f64,
}
