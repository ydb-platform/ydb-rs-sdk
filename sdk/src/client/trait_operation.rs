use ydb_protobuf::generated::ydb::discovery::WhoAmIResponse;
use ydb_protobuf::generated::ydb::operations::Operation as YdbOperation;

pub trait Operation {
    fn operation(self: &Self) -> Option<YdbOperation>;
}

impl Operation for WhoAmIResponse {
    fn operation(self: &Self) -> Option<YdbOperation> {
        return self.operation.clone();
    }
}
