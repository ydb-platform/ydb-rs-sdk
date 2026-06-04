use std::fmt::Debug;
use ydb_grpc::ydb_proto::auth::LoginResponse;
use ydb_grpc::ydb_proto::coordination::{
    AlterNodeResponse, CreateNodeResponse, DescribeNodeResponse, DropNodeResponse,
};
use ydb_grpc::ydb_proto::discovery::{ListEndpointsResponse, WhoAmIResponse};
use ydb_grpc::ydb_proto::issue::IssueMessage;
use ydb_grpc::ydb_proto::operations::Operation as YdbOperation;
use ydb_grpc::ydb_proto::scheme::{
    ListDirectoryResponse, MakeDirectoryResponse, RemoveDirectoryResponse,
};
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::table::{
    BulkUpsertResponse, CommitTransactionResponse, CopyTableResponse, CopyTablesResponse,
    CreateSessionResponse, DeleteSessionResponse, DescribeTableResponse, ExecuteDataQueryResponse,
    ExecuteSchemeQueryResponse, ExplainDataQueryResponse, KeepAliveResponse, ReadRowsResponse,
    RollbackTransactionResponse,
};
use ydb_grpc::ydb_proto::topic::{
    AlterTopicResponse, CreateTopicResponse, DescribeConsumerResponse, DescribeTopicResponse,
    DropTopicResponse, UpdateOffsetsInTransactionResponse,
};
use ydb_grpc::ydb_proto::ResultSet;

use crate::grpc_wrapper::raw_errors::{RawError, RawResult};

pub(crate) trait YdbGrpcStatus<T>: Debug {
    fn status(&self) -> RawResult<StatusCode>;
    fn issues(&self) -> RawResult<&[IssueMessage]>;
    fn into_result(self) -> RawResult<T>;
}

impl YdbGrpcStatus<ResultSet> for ReadRowsResponse {
    fn status(&self) -> RawResult<StatusCode> {
        Ok(self.status())
    }

    fn issues(&self) -> RawResult<&[IssueMessage]> {
        Ok(&self.issues)
    }

    fn into_result(self) -> RawResult<ResultSet> {
        self.result_set
            .ok_or_else(|| RawError::Custom("no result data".into()))
    }
}

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

        impl<T: Default + prost::Message> YdbGrpcStatus<T> for $t {
            fn status(&self) -> RawResult<StatusCode> {
                let operation = self
                    .operation
                    .as_ref()
                    .ok_or_else(|| RawError::Custom("no operation to fetch status".to_string()))?;

                Ok(operation.status())
            }

            fn issues(&self) -> RawResult<&[IssueMessage]> {
                let operation = self
                    .operation
                    .as_ref()
                    .ok_or_else(|| RawError::Custom("no operation to fetch status".to_string()))?;

                Ok(&operation.issues)
            }
            fn into_result(self) -> RawResult<T> {
                let operation = self
                    .operation
                    .ok_or_else(|| RawError::Custom("no operation object in result".to_string()))?;

                let result = operation
                    .result
                    .ok_or_else(|| RawError::Custom("no result data in operation".into()))?;

                let decoded = T::decode(result.value.as_slice())?;
                Ok(decoded)
            }
        }
    };
}

operation_impl_for!(CommitTransactionResponse);
operation_impl_for!(CreateSessionResponse);
operation_impl_for!(DeleteSessionResponse);
operation_impl_for!(ExecuteDataQueryResponse);
operation_impl_for!(ExecuteSchemeQueryResponse);
operation_impl_for!(ExplainDataQueryResponse);
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
operation_impl_for!(DescribeTableResponse);
operation_impl_for!(LoginResponse);
operation_impl_for!(DescribeConsumerResponse);
operation_impl_for!(UpdateOffsetsInTransactionResponse);
operation_impl_for!(BulkUpsertResponse);
