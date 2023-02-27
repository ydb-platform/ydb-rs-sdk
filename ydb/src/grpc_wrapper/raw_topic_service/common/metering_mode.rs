#[derive(serde::Serialize)]
pub(crate) enum RawMeteringMode {
    Unspecified,
    ReservedCapacity,
    RequestUnits,
}

impl From<RawMeteringMode> for ydb_grpc::ydb_proto::topic::MeteringMode {
    fn from(v: RawMeteringMode) -> Self {
        use ydb_grpc::ydb_proto::topic::MeteringMode as meteringMode;
        match v {
            RawMeteringMode::Unspecified => meteringMode::Unspecified,
            RawMeteringMode::ReservedCapacity => meteringMode::ReservedCapacity,
            RawMeteringMode::RequestUnits => meteringMode::RequestUnits,
        }
    }
}
