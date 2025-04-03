#[derive(serde::Serialize)]
pub(crate) enum RawQueryStatMode {
    None,
    Basic,
    Full,
    Profile,
}

impl From<RawQueryStatMode> for ydb_grpc::ydb_proto::table::query_stats_collection::Mode {
    fn from(v: RawQueryStatMode) -> Self {
        use ydb_grpc::ydb_proto::table::query_stats_collection::Mode as grpcMode;
        match v {
            RawQueryStatMode::None => grpcMode::StatsCollectionNone,
            RawQueryStatMode::Basic => grpcMode::StatsCollectionBasic,
            RawQueryStatMode::Full => grpcMode::StatsCollectionFull,
            RawQueryStatMode::Profile => grpcMode::StatsCollectionProfile,
        }
    }
}
