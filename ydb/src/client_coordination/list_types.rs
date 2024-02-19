use derive_builder::Builder;

use crate::grpc_wrapper::raw_coordination_service::config::{
    RawConsistencyMode, RawCoordinationNodeConfig, RawRateLimiterCountersMode,
};
use crate::grpc_wrapper::raw_coordination_service::describe_node::RawDescribeNodeResult;
use crate::{errors, SchemeEntry};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsistencyMode {
    Strict,
    Relaxed,
}

impl From<Option<ConsistencyMode>> for RawConsistencyMode {
    fn from(value: Option<ConsistencyMode>) -> Self {
        match value {
            None => RawConsistencyMode::Unset,
            Some(ConsistencyMode::Strict) => RawConsistencyMode::Strict,
            Some(ConsistencyMode::Relaxed) => RawConsistencyMode::Relaxed,
        }
    }
}

impl From<RawConsistencyMode> for Option<ConsistencyMode> {
    fn from(value: RawConsistencyMode) -> Self {
        match value {
            RawConsistencyMode::Unset => None,
            RawConsistencyMode::Strict => Some(ConsistencyMode::Strict),
            RawConsistencyMode::Relaxed => Some(ConsistencyMode::Relaxed),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RateLimiterCountersMode {
    Aggregated,
    Detailed,
}

impl From<Option<RateLimiterCountersMode>> for RawRateLimiterCountersMode {
    fn from(value: Option<RateLimiterCountersMode>) -> Self {
        match value {
            None => RawRateLimiterCountersMode::Unset,
            Some(RateLimiterCountersMode::Aggregated) => RawRateLimiterCountersMode::Aggregated,
            Some(RateLimiterCountersMode::Detailed) => RawRateLimiterCountersMode::Detailed,
        }
    }
}

impl From<RawRateLimiterCountersMode> for Option<RateLimiterCountersMode> {
    fn from(value: RawRateLimiterCountersMode) -> Self {
        match value {
            RawRateLimiterCountersMode::Unset => None,
            RawRateLimiterCountersMode::Aggregated => Some(RateLimiterCountersMode::Aggregated),
            RawRateLimiterCountersMode::Detailed => Some(RateLimiterCountersMode::Detailed),
        }
    }
}

#[derive(Builder)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct NodeConfig {
    // Use NodeConfigBuilder
    #[builder(default)]
    pub self_check_period_millis: u32,
    #[builder(default)]
    pub session_grace_period_millis: u32,
    #[builder(default)]
    pub read_consistency_mode: Option<ConsistencyMode>,
    #[builder(default)]
    pub attach_consistency_mode: Option<ConsistencyMode>,
    #[builder(default)]
    pub rate_limiter_counters_mode: Option<RateLimiterCountersMode>,
}

impl From<RawCoordinationNodeConfig> for NodeConfig {
    fn from(value: RawCoordinationNodeConfig) -> Self {
        Self {
            self_check_period_millis: value.self_check_period_millis,
            session_grace_period_millis: value.session_grace_period_millis,
            read_consistency_mode: value.read_consistency_mode.into(),
            attach_consistency_mode: value.attach_consistency_mode.into(),
            rate_limiter_counters_mode: value.rate_limiter_counters_mode.into(),
        }
    }
}

pub struct NodeDescription {
    pub entry: SchemeEntry,
    pub config: NodeConfig,
}

impl From<RawDescribeNodeResult> for NodeDescription {
    fn from(value: RawDescribeNodeResult) -> Self {
        Self {
            entry: value.self_,
            config: NodeConfig::from(value.config),
        }
    }
}
