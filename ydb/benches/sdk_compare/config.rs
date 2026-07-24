use std::fs;
use std::path::Path;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use crate::payload::HEADER_SIZE_BYTES;

pub(crate) const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Scenario {
    pub(crate) schema_version: u32,
    pub(crate) name: String,
    pub(crate) execution: Execution,
    pub(crate) workload: Workload,
}

impl Scenario {
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let json = fs::read_to_string(path)
            .with_context(|| format!("failed to read scenario file {}", path.display()))?;

        Self::from_json(&json)
            .with_context(|| format!("failed to load scenario file {}", path.display()))
    }

    pub(crate) fn from_json(json: &str) -> Result<Self> {
        let scenario: Self = serde_json::from_str(json).context("failed to parse scenario JSON")?;
        scenario.validate()?;
        Ok(scenario)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == SCHEMA_VERSION,
            "unsupported schema_version {}; expected {}",
            self.schema_version,
            SCHEMA_VERSION
        );
        ensure!(
            !self.name.trim().is_empty(),
            "scenario name must not be empty"
        );
        self.execution.validate()?;
        self.workload.validate()?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Execution {
    pub(crate) worker_threads: usize,
    pub(crate) warmup_seconds: u64,
    pub(crate) measurement_seconds: u64,
    pub(crate) drain_timeout_seconds: u64,
}

impl Execution {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.worker_threads > 0,
            "worker_threads must be greater than zero"
        );
        ensure!(
            self.measurement_seconds > 0,
            "measurement_seconds must be greater than zero"
        );
        ensure!(
            self.drain_timeout_seconds > 0,
            "drain_timeout_seconds must be greater than zero"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Workload {
    Topic(TopicWorkload),
}

impl Workload {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Topic(topic) => topic.validate(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TopicWorkload {
    pub(crate) topic_name: String,
    pub(crate) consumer_name: String,
    pub(crate) partition_count: u32,
    pub(crate) writer_count: usize,
    pub(crate) reader_count: usize,
    pub(crate) message_size_bytes: usize,
    pub(crate) max_in_flight_per_writer: usize,
    pub(crate) partition_write_speed_bytes_per_second: i64,
}

impl TopicWorkload {
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.topic_name.trim().is_empty(),
            "topic_name must not be empty"
        );
        ensure!(
            !self.consumer_name.trim().is_empty(),
            "consumer_name must not be empty"
        );
        ensure!(
            self.partition_count > 0,
            "partition_count must be greater than zero"
        );
        ensure!(
            self.writer_count > 0,
            "writer_count must be greater than zero"
        );
        ensure!(
            self.reader_count > 0,
            "reader_count must be greater than zero"
        );
        ensure!(
            self.message_size_bytes >= HEADER_SIZE_BYTES,
            "message_size_bytes must be at least {HEADER_SIZE_BYTES}"
        );
        ensure!(
            self.max_in_flight_per_writer > 0,
            "max_in_flight_per_writer must be greater than zero"
        );
        ensure!(
            self.partition_write_speed_bytes_per_second > 0,
            "partition_write_speed_bytes_per_second must be greater than zero"
        );
        Ok(())
    }
}
