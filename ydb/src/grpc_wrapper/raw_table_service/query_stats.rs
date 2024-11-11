use crate::QueryStatsMode;

#[derive(serde::Serialize)]
pub(crate) enum RawQueryStatsMode {
    None,
    Basic,
    Full,
    Profile,
}

impl From<RawQueryStatsMode> for ydb_grpc::ydb_proto::table::query_stats_collection::Mode {
    fn from(v: RawQueryStatsMode) -> Self {
        use ydb_grpc::ydb_proto::table::query_stats_collection::Mode as grpcMode;
        match v {
            RawQueryStatsMode::None => grpcMode::StatsCollectionNone,
            RawQueryStatsMode::Basic => grpcMode::StatsCollectionBasic,
            RawQueryStatsMode::Full => grpcMode::StatsCollectionFull,
            RawQueryStatsMode::Profile => grpcMode::StatsCollectionProfile,
        }
    }
}

impl From<QueryStatsMode> for RawQueryStatsMode {
    fn from(value: QueryStatsMode) -> Self {
        match value {
            QueryStatsMode::Basic => RawQueryStatsMode::Basic,
            QueryStatsMode::None => RawQueryStatsMode::None,
            QueryStatsMode::Full => RawQueryStatsMode::Full,
            QueryStatsMode::Profile => RawQueryStatsMode::Profile,
        }
    }
}
