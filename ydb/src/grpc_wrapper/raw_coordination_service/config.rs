use ydb_grpc::ydb_proto::coordination::{Config, ConsistencyMode, RateLimiterCountersMode};

use crate::{
    client_coordination::list_types::NodeConfig,
    grpc_wrapper::raw_errors::{RawError, RawResult},
};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) enum RawConsistencyMode {
    Unset,
    Strict,
    Relaxed,
}

impl From<RawConsistencyMode> for i32 {
    fn from(value: RawConsistencyMode) -> Self {
        let value = match value {
            RawConsistencyMode::Unset => ConsistencyMode::Unset,
            RawConsistencyMode::Strict => ConsistencyMode::Strict,
            RawConsistencyMode::Relaxed => ConsistencyMode::Relaxed,
        };
        value as i32
    }
}

impl TryFrom<i32> for RawConsistencyMode {
    type Error = RawError;

    fn try_from(value: i32) -> RawResult<Self> {
        let value = ConsistencyMode::try_from(value).map_err(|_| RawError::ProtobufDecodeError(
            format!("invalid consistency mode: {value}"),
        ))?;
        match value {
            ConsistencyMode::Unset => Ok(RawConsistencyMode::Unset),
            ConsistencyMode::Strict => Ok(RawConsistencyMode::Strict),
            ConsistencyMode::Relaxed => Ok(RawConsistencyMode::Relaxed),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) enum RawRateLimiterCountersMode {
    Unset,
    Aggregated,
    Detailed,
}

impl From<RawRateLimiterCountersMode> for i32 {
    fn from(value: RawRateLimiterCountersMode) -> Self {
        let value = match value {
            RawRateLimiterCountersMode::Unset => RateLimiterCountersMode::Unset,
            RawRateLimiterCountersMode::Aggregated => RateLimiterCountersMode::Aggregated,
            RawRateLimiterCountersMode::Detailed => RateLimiterCountersMode::Detailed,
        };
        value as i32
    }
}

impl TryFrom<i32> for RawRateLimiterCountersMode {
    type Error = RawError;

    fn try_from(value: i32) -> RawResult<Self> {
        let value = RateLimiterCountersMode::try_from(value).map_err(|_| {
            RawError::ProtobufDecodeError(format!("invalid rate limiter counters mode: {value}"))
        })?;
        match value {
            RateLimiterCountersMode::Unset => Ok(RawRateLimiterCountersMode::Unset),
            RateLimiterCountersMode::Aggregated => Ok(RawRateLimiterCountersMode::Aggregated),
            RateLimiterCountersMode::Detailed => Ok(RawRateLimiterCountersMode::Detailed),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct RawCoordinationNodeConfig {
    pub path: String,
    pub self_check_period_millis: u32,
    pub session_grace_period_millis: u32,
    pub read_consistency_mode: RawConsistencyMode,
    pub attach_consistency_mode: RawConsistencyMode,
    pub rate_limiter_counters_mode: RawRateLimiterCountersMode,
}

impl From<NodeConfig> for RawCoordinationNodeConfig {
    fn from(config: NodeConfig) -> Self {
        Self {
            path: "".to_string(),
            self_check_period_millis: config.self_check_period_millis,
            session_grace_period_millis: config.session_grace_period_millis,
            read_consistency_mode: RawConsistencyMode::from(config.read_consistency_mode),
            attach_consistency_mode: RawConsistencyMode::from(config.attach_consistency_mode),
            rate_limiter_counters_mode: RawRateLimiterCountersMode::from(
                config.rate_limiter_counters_mode,
            ),
        }
    }
}

impl From<RawCoordinationNodeConfig> for Config {
    fn from(value: RawCoordinationNodeConfig) -> Self {
        Self {
            path: value.path,
            self_check_period_millis: value.self_check_period_millis,
            session_grace_period_millis: value.session_grace_period_millis,
            read_consistency_mode: value.read_consistency_mode.into(),
            attach_consistency_mode: value.attach_consistency_mode.into(),
            rate_limiter_counters_mode: value.rate_limiter_counters_mode.into(),
        }
    }
}

impl TryFrom<Config> for RawCoordinationNodeConfig {
    type Error = RawError;

    fn try_from(value: Config) -> RawResult<Self> {
        Ok(Self {
            path: value.path,
            self_check_period_millis: value.self_check_period_millis,
            session_grace_period_millis: value.session_grace_period_millis,
            read_consistency_mode: value.read_consistency_mode.try_into()?,
            attach_consistency_mode: value.attach_consistency_mode.try_into()?,
            rate_limiter_counters_mode: value.rate_limiter_counters_mode.try_into()?,
        })
    }
}
