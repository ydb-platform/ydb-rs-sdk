#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteYqlRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(string, tag="2")]
    pub script: ::prost::alloc::string::String,
    #[prost(map="string, message", tag="3")]
    pub parameters: ::std::collections::HashMap<::prost::alloc::string::String, super::TypedValue>,
    #[prost(enumeration="super::table::query_stats_collection::Mode", tag="4")]
    pub collect_stats: i32,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteYqlResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteYqlResult {
    #[prost(message, repeated, tag="1")]
    pub result_sets: ::prost::alloc::vec::Vec<super::ResultSet>,
    #[prost(message, optional, tag="2")]
    pub query_stats: ::core::option::Option<super::table_stats::QueryStats>,
}
/// Response for StreamExecuteYql is a stream of ExecuteYqlPartialResponse messages.
/// These responses can contain ExecuteYqlPartialResult messages with
/// results (or result parts) for data or scan queries in the script.
/// YqlScript can have multiple results (result sets).
/// Each result set has an index (starting at 0).
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteYqlPartialResponse {
    #[prost(enumeration="super::status_ids::StatusCode", tag="1")]
    pub status: i32,
    #[prost(message, repeated, tag="2")]
    pub issues: ::prost::alloc::vec::Vec<super::issue::IssueMessage>,
    #[prost(message, optional, tag="3")]
    pub result: ::core::option::Option<ExecuteYqlPartialResult>,
}
/// Contains result set (or a result set part) for one data or scan query in the script.
/// One result set can be split into several responses with same result_index.
/// Only the final response can contain query stats.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteYqlPartialResult {
    /// Index of current result
    #[prost(uint32, tag="1")]
    pub result_set_index: u32,
    /// Result set (or a result set part) for one data or scan query
    #[prost(message, optional, tag="2")]
    pub result_set: ::core::option::Option<super::ResultSet>,
    #[prost(message, optional, tag="3")]
    pub query_stats: ::core::option::Option<super::table_stats::QueryStats>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExplainYqlRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(string, tag="2")]
    pub script: ::prost::alloc::string::String,
    #[prost(enumeration="explain_yql_request::Mode", tag="3")]
    pub mode: i32,
}
/// Nested message and enum types in `ExplainYqlRequest`.
pub mod explain_yql_request {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Mode {
        Unspecified = 0,
        /// PARSE = 1;
        Validate = 2,
        Plan = 3,
    }
    impl Mode {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Mode::Unspecified => "MODE_UNSPECIFIED",
                Mode::Validate => "VALIDATE",
                Mode::Plan => "PLAN",
            }
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExplainYqlResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExplainYqlResult {
    #[prost(map="string, message", tag="1")]
    pub parameters_types: ::std::collections::HashMap<::prost::alloc::string::String, super::Type>,
    #[prost(string, tag="2")]
    pub plan: ::prost::alloc::string::String,
}