use std::fmt::Debug;
use ydb_grpc::ydb_proto::auth::LoginResponse;
use ydb_grpc::ydb_proto::coordination::{
    AlterNodeResponse, CreateNodeResponse, DescribeNodeResponse, DropNodeResponse,
};
use ydb_grpc::ydb_proto::discovery::{ListEndpointsResponse, WhoAmIResponse};
use ydb_grpc::ydb_proto::operations::Operation as YdbOperation;
use ydb_grpc::ydb_proto::scheme::{
    ListDirectoryResponse, MakeDirectoryResponse, RemoveDirectoryResponse,
};
use ydb_grpc::ydb_proto::table::{
    BulkUpsertResponse, CommitTransactionResponse, CopyTableResponse, CopyTablesResponse,
    CreateSessionResponse, DeleteSessionResponse, ExecuteDataQueryResponse,
    ExecuteSchemeQueryResponse, KeepAliveResponse, RollbackTransactionResponse,
};
use ydb_grpc::ydb_proto::topic::{
    AlterTopicResponse, CreateTopicResponse, DescribeConsumerResponse, DescribeTopicResponse,
    DropTopicResponse, UpdateOffsetsInTransactionResponse,
};

pub(crate) trait Operation: Debug {
    fn operation(&self) -> Option<YdbOperation>;
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
operation_impl_for!(MakeDirectoryResponse);
operation_impl_for!(ListDirectoryResponse);
operation_impl_for!(RemoveDirectoryResponse);
operation_impl_for!(AlterTopicResponse);
operation_impl_for!(CreateTopicResponse);
operation_impl_for!(DescribeTopicResponse);
operation_impl_for!(DropTopicResponse);
operation_impl_for!(CreateNodeResponse);
operation_impl_for!(DescribeNodeResponse);
operation_impl_for!(AlterNodeResponse);
operation_impl_for!(DropNodeResponse);
operation_impl_for!(CopyTableResponse);
operation_impl_for!(CopyTablesResponse);
operation_impl_for!(LoginResponse);
operation_impl_for!(DescribeConsumerResponse);
operation_impl_for!(UpdateOffsetsInTransactionResponse);
operation_impl_for!(BulkUpsertResponse);
