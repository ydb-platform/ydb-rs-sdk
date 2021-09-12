#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UploadRowsRequest {
    #[prost(string, tag = "1")]
    pub table: ::prost::alloc::string::String,
    /// Must be List of Structs
    #[prost(message, optional, tag = "2")]
    pub rows: ::core::option::Option<super::TypedValue>,
    #[prost(message, optional, tag = "3")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UploadRowsResponse {
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// TODO: ?
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UploadRowsResult {}
////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteStreamQueryRequest {
    #[prost(string, tag = "1")]
    pub yql_text: ::prost::alloc::string::String,
    #[prost(map = "string, message", tag = "2")]
    pub parameters: ::std::collections::HashMap<::prost::alloc::string::String, super::TypedValue>,
    #[prost(enumeration = "execute_stream_query_request::ProfileMode", tag = "3")]
    pub profile_mode: i32,
    #[prost(bool, tag = "4")]
    pub explain: bool,
}
/// Nested message and enum types in `ExecuteStreamQueryRequest`.
pub mod execute_stream_query_request {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ProfileMode {
        Unspecified = 0,
        None = 1,
        Basic = 2,
        Full = 3,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteStreamQueryResponse {
    #[prost(enumeration = "super::status_ids::StatusCode", tag = "1")]
    pub status: i32,
    #[prost(message, repeated, tag = "2")]
    pub issues: ::prost::alloc::vec::Vec<super::issue::IssueMessage>,
    #[prost(message, optional, tag = "3")]
    pub result: ::core::option::Option<ExecuteStreamQueryResult>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamQueryProgress {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteStreamQueryResult {
    #[prost(oneof = "execute_stream_query_result::Result", tags = "1, 2, 3, 4")]
    pub result: ::core::option::Option<execute_stream_query_result::Result>,
}
/// Nested message and enum types in `ExecuteStreamQueryResult`.
pub mod execute_stream_query_result {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag = "1")]
        ResultSet(super::super::ResultSet),
        #[prost(string, tag = "2")]
        Profile(::prost::alloc::string::String),
        #[prost(message, tag = "3")]
        Progress(super::StreamQueryProgress),
        #[prost(string, tag = "4")]
        QueryPlan(::prost::alloc::string::String),
    }
}
////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetDiskSpaceUsageRequest {
    #[prost(message, optional, tag = "1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(string, tag = "2")]
    pub database: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetDiskSpaceUsageResponse {
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetDiskSpaceUsageResult {
    #[prost(string, tag = "1")]
    pub cloud_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub folder_id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub database_id: ::prost::alloc::string::String,
    /// in bytes
    #[prost(uint64, tag = "4")]
    pub total_size: u64,
    #[prost(uint64, tag = "5")]
    pub data_size: u64,
    #[prost(uint64, tag = "6")]
    pub index_size: u64,
}
