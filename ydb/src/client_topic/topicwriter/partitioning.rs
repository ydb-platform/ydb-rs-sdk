use ydb_grpc::ydb_proto::topic::stream_write_message::init_request::Partitioning as GrpcInitPartitioning;

#[derive(Clone, Debug, Default)]
pub enum PartitioningStrategy {
    #[default]
    ByProducerId,
    PartitionId(i64),
}

impl PartitioningStrategy {
    pub(crate) fn to_grpc_init_partitioning(&self, producer_id: String) -> GrpcInitPartitioning {
        match self {
            Self::ByProducerId => GrpcInitPartitioning::MessageGroupId(producer_id),
            Self::PartitionId(id) => GrpcInitPartitioning::PartitionId(*id),
        }
    }
}
