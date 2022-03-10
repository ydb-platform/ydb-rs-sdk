use std::fmt::Debug;
use ydb_protobuf::ydb_proto::discovery::{ListEndpointsResponse, WhoAmIResponse};
use ydb_protobuf::ydb_proto::operations::Operation as YdbOperation;
use ydb_protobuf::ydb_proto::table::{
    CommitTransactionResponse, CreateSessionResponse, DeleteSessionResponse,
    ExecuteDataQueryResponse, ExecuteSchemeQueryResponse, KeepAliveResponse,
    RollbackTransactionResponse,
};

pub(crate) trait Operation: Debug {
    fn operation(self: &Self) -> Option<YdbOperation>;
}

macro_rules! operation_impl_for {
    ($t:ty) => {
        impl Operation for $t {
            fn operation(&self) -> Option<YdbOperation> {
                return self.operation.clone();
            }
        }
    };
}

operation_impl_for!(CommitTransactionResponse);
operation_impl_for!(CreateSessionResponse);
operation_impl_for!(DeleteSessionResponse);
operation_impl_for!(ExecuteDataQueryResponse);
operation_impl_for!(ExecuteSchemeQueryResponse);
operation_impl_for!(KeepAliveResponse);
operation_impl_for!(ListEndpointsResponse);
operation_impl_for!(RollbackTransactionResponse);
operation_impl_for!(WhoAmIResponse);
