/// Describes select, update (insert, upsert, replace) and delete operations
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OperationStats {
    #[prost(uint64, tag = "1")]
    pub rows: u64,
    #[prost(uint64, tag = "2")]
    pub bytes: u64,
}
/// Describes all operations on a table
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableAccessStats {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub reads: ::core::option::Option<OperationStats>,
    #[prost(message, optional, tag = "4")]
    pub updates: ::core::option::Option<OperationStats>,
    #[prost(message, optional, tag = "5")]
    pub deletes: ::core::option::Option<OperationStats>,
    #[prost(uint64, tag = "6")]
    pub partitions_count: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryPhaseStats {
    #[prost(uint64, tag = "1")]
    pub duration_us: u64,
    #[prost(message, repeated, tag = "2")]
    pub table_access: ::prost::alloc::vec::Vec<TableAccessStats>,
    #[prost(uint64, tag = "3")]
    pub cpu_time_us: u64,
    #[prost(uint64, tag = "4")]
    pub affected_shards: u64,
    #[prost(bool, tag = "5")]
    pub literal_phase: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CompilationStats {
    #[prost(bool, tag = "1")]
    pub from_cache: bool,
    #[prost(uint64, tag = "2")]
    pub duration_us: u64,
    #[prost(uint64, tag = "3")]
    pub cpu_time_us: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryStats {
    /// A query might have one or more execution phases
    #[prost(message, repeated, tag = "1")]
    pub query_phases: ::prost::alloc::vec::Vec<QueryPhaseStats>,
    #[prost(message, optional, tag = "2")]
    pub compilation: ::core::option::Option<CompilationStats>,
    #[prost(uint64, tag = "3")]
    pub process_cpu_time_us: u64,
    #[prost(string, tag = "4")]
    pub query_plan: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub query_ast: ::prost::alloc::string::String,
}
