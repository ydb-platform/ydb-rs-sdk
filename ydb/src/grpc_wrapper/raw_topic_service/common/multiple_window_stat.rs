use ydb_grpc::ydb_proto::topic::MultipleWindowsStat;

#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct RawMultipleWindowsStat {
    pub per_minute: i64,
    pub per_hour: i64,
    pub per_day: i64,
}

impl From<MultipleWindowsStat> for RawMultipleWindowsStat {
    fn from(value: MultipleWindowsStat) -> Self {
        Self {
            per_minute: value.per_minute,
            per_hour: value.per_hour,
            per_day: value.per_day,
        }
    }
}

impl From<RawMultipleWindowsStat> for MultipleWindowsStat {
    fn from(value: RawMultipleWindowsStat) -> Self {
        Self {
            per_minute: value.per_minute,
            per_hour: value.per_hour,
            per_day: value.per_day,
        }
    }
}
