use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use ydb_grpc::ydb_proto::topic::MeteringMode;

#[derive(serde::Serialize, Clone, Default, Debug)]
pub(crate) enum RawMeteringMode {
    #[default]
    Unspecified,
    ReservedCapacity,
    RequestUnits,
}

impl TryFrom<i32> for RawMeteringMode {
    type Error = RawError;

    fn try_from(value: i32) -> RawResult<Self> {
        let value = MeteringMode::try_from(value).map_err(|e| {
            RawError::ProtobufDecodeError(format!("invalid metering mode: {value} ({e})"))
        })?;
        match value {
            MeteringMode::Unspecified => Ok(RawMeteringMode::Unspecified),
            MeteringMode::ReservedCapacity => Ok(RawMeteringMode::ReservedCapacity),
            MeteringMode::RequestUnits => Ok(RawMeteringMode::RequestUnits),
        }
    }
}

impl From<RawMeteringMode> for MeteringMode {
    fn from(v: RawMeteringMode) -> Self {
        match v {
            RawMeteringMode::Unspecified => MeteringMode::Unspecified,
            RawMeteringMode::ReservedCapacity => MeteringMode::ReservedCapacity,
            RawMeteringMode::RequestUnits => MeteringMode::RequestUnits,
        }
    }
}
