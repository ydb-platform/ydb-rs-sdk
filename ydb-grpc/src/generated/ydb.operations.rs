#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OperationParams {
    #[prost(enumeration="operation_params::OperationMode", tag="1")]
    pub operation_mode: i32,
    /// Indicates that client is no longer interested in the result of operation after the specified duration
    /// starting from the time operation arrives at the server.
    /// Server will try to stop the execution of operation and if no result is currently available the operation
    /// will receive TIMEOUT status code, which will be sent back to client if it was waiting for the operation result.
    /// Timeout of operation does not tell anything about its result, it might be completed successfully
    /// or cancelled on server.
    #[prost(message, optional, tag="2")]
    pub operation_timeout: ::core::option::Option<::pbjson_types::Duration>,
    /// Server will try to cancel the operation after the specified duration starting from the time
    /// the operation arrives at server.
    /// In case of successful cancellation operation will receive CANCELLED status code, which will be
    /// sent back to client if it was waiting for the operation result.
    /// In case when cancellation isn't possible, no action will be performed.
    #[prost(message, optional, tag="3")]
    pub cancel_after: ::core::option::Option<::pbjson_types::Duration>,
    /// User-defined labels of operation.
    #[prost(map="string, string", tag="4")]
    pub labels: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    /// If enabled, server will report cost information, if supported by the operation.
    /// This flag is mostly useful for SYNC operations, to get the cost information in the response.
    #[prost(enumeration="super::feature_flag::Status", tag="5")]
    pub report_cost_info: i32,
}
/// Nested message and enum types in `OperationParams`.
pub mod operation_params {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum OperationMode {
        Unspecified = 0,
        /// Server will only reply once operation is finished (ready=true), and operation object won't be
        /// accessible after the reply. This is a basic request-response mode.
        Sync = 1,
        Async = 2,
    }
    impl OperationMode {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                OperationMode::Unspecified => "OPERATION_MODE_UNSPECIFIED",
                OperationMode::Sync => "SYNC",
                OperationMode::Async => "ASYNC",
            }
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOperationRequest {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOperationResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CancelOperationRequest {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CancelOperationResponse {
    #[prost(enumeration="super::status_ids::StatusCode", tag="1")]
    pub status: i32,
    #[prost(message, repeated, tag="2")]
    pub issues: ::prost::alloc::vec::Vec<super::issue::IssueMessage>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ForgetOperationRequest {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ForgetOperationResponse {
    #[prost(enumeration="super::status_ids::StatusCode", tag="1")]
    pub status: i32,
    #[prost(message, repeated, tag="2")]
    pub issues: ::prost::alloc::vec::Vec<super::issue::IssueMessage>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListOperationsRequest {
    #[prost(string, tag="1")]
    pub kind: ::prost::alloc::string::String,
    #[prost(uint64, tag="2")]
    pub page_size: u64,
    #[prost(string, tag="3")]
    pub page_token: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListOperationsResponse {
    #[prost(enumeration="super::status_ids::StatusCode", tag="1")]
    pub status: i32,
    #[prost(message, repeated, tag="2")]
    pub issues: ::prost::alloc::vec::Vec<super::issue::IssueMessage>,
    #[prost(message, repeated, tag="3")]
    pub operations: ::prost::alloc::vec::Vec<Operation>,
    #[prost(string, tag="4")]
    pub next_page_token: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Operation {
    /// Identifier of the operation, empty value means no active operation object is present (it was forgotten or
    /// not created in the first place, as in SYNC operation mode).
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    /// true - this operation has beed finished (doesn't matter successful or not),
    /// so Status field has status code, and Result field can contains result data.
    /// false - this operation still running. You can repeat request using operation Id.
    #[prost(bool, tag="2")]
    pub ready: bool,
    #[prost(enumeration="super::status_ids::StatusCode", tag="3")]
    pub status: i32,
    #[prost(message, repeated, tag="4")]
    pub issues: ::prost::alloc::vec::Vec<super::issue::IssueMessage>,
    /// Result data
    #[prost(message, optional, tag="5")]
    pub result: ::core::option::Option<::pbjson_types::Any>,
    #[prost(message, optional, tag="6")]
    pub metadata: ::core::option::Option<::pbjson_types::Any>,
    /// Contains information about the cost of the operation.
    /// For completed operations, it shows the final cost of the operation.
    /// For operations in progress, it might indicate the current cost of the operation (if supported).
    #[prost(message, optional, tag="7")]
    pub cost_info: ::core::option::Option<super::CostInfo>,
}