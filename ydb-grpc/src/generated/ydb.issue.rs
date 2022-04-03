/// IssueMessage is a transport format for ydb/library/yql/public/issue library
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IssueMessage {
    #[prost(message, optional, tag="1")]
    pub position: ::core::option::Option<issue_message::Position>,
    #[prost(string, tag="2")]
    pub message: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub end_position: ::core::option::Option<issue_message::Position>,
    #[prost(uint32, tag="4")]
    pub issue_code: u32,
    /// Severity values from ydb/library/yql/public/issue/protos/issue_severity.proto
    /// FATAL = 0;
    /// ERROR = 1;
    /// WARNING = 2;
    /// INFO = 3;
    #[prost(uint32, tag="5")]
    pub severity: u32,
    #[prost(message, repeated, tag="6")]
    pub issues: ::prost::alloc::vec::Vec<IssueMessage>,
}
/// Nested message and enum types in `IssueMessage`.
pub mod issue_message {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Position {
        #[prost(uint32, tag="1")]
        pub row: u32,
        #[prost(uint32, tag="2")]
        pub column: u32,
        #[prost(string, tag="3")]
        pub file: ::prost::alloc::string::String,
    }
}