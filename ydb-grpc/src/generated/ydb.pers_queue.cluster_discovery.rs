#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteSessionParams {
    /// Path to the topic to write to.
    #[prost(string, tag="1")]
    pub topic: ::prost::alloc::string::String,
    /// Message group identifier.
    #[prost(bytes="vec", tag="2")]
    pub source_id: ::prost::alloc::vec::Vec<u8>,
    /// Partition group to write to. 0 by default.
    #[prost(uint32, tag="3")]
    pub partition_group: u32,
    /// Force the specified cluster via its name. Leave it empty by default.
    #[prost(string, tag="4")]
    pub preferred_cluster_name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClusterInfo {
    /// A host discovery endpoint to use at the next step.
    #[prost(string, tag="1")]
    pub endpoint: ::prost::alloc::string::String,
    /// An official cluster name.
    #[prost(string, tag="2")]
    pub name: ::prost::alloc::string::String,
    /// Is the cluster available right now?
    #[prost(bool, tag="3")]
    pub available: bool,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadSessionParams {
    /// Path to the topic to read from.
    #[prost(string, tag="1")]
    pub topic: ::prost::alloc::string::String,
    /// Read mode is set according to the read rule.
    #[prost(oneof="read_session_params::ReadRule", tags="2, 3")]
    pub read_rule: ::core::option::Option<read_session_params::ReadRule>,
}
/// Nested message and enum types in `ReadSessionParams`.
pub mod read_session_params {
    /// Read mode is set according to the read rule.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ReadRule {
        #[prost(string, tag="2")]
        MirrorToCluster(::prost::alloc::string::String),
        #[prost(message, tag="3")]
        AllOriginal(::pbjson_types::Empty),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteSessionClusters {
    /// Ordered clusters with statuses.
    #[prost(message, repeated, tag="1")]
    pub clusters: ::prost::alloc::vec::Vec<ClusterInfo>,
    /// The reason why a particular cluster was prioritized.
    #[prost(enumeration="write_session_clusters::SelectionReason", tag="2")]
    pub primary_cluster_selection_reason: i32,
}
/// Nested message and enum types in `WriteSessionClusters`.
pub mod write_session_clusters {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum SelectionReason {
        Unspecified = 0,
        ClientPreference = 1,
        ClientLocation = 2,
        ConsistentDistribution = 3,
    }
    impl SelectionReason {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                SelectionReason::Unspecified => "SELECTION_REASON_UNSPECIFIED",
                SelectionReason::ClientPreference => "CLIENT_PREFERENCE",
                SelectionReason::ClientLocation => "CLIENT_LOCATION",
                SelectionReason::ConsistentDistribution => "CONSISTENT_DISTRIBUTION",
            }
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadSessionClusters {
    /// Ordered clusters with statuses.
    #[prost(message, repeated, tag="1")]
    pub clusters: ::prost::alloc::vec::Vec<ClusterInfo>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DiscoverClustersRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::super::operations::OperationParams>,
    /// Clusters will be discovered separately for each element of the list.
    #[prost(message, repeated, tag="2")]
    pub write_sessions: ::prost::alloc::vec::Vec<WriteSessionParams>,
    #[prost(message, repeated, tag="3")]
    pub read_sessions: ::prost::alloc::vec::Vec<ReadSessionParams>,
    /// Latest clusters status version known to the client application. Use 0 by default.
    #[prost(int64, tag="4")]
    pub minimal_version: i64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DiscoverClustersResponse {
    /// Operation contains the result of the request. Check the ydb_operation.proto.
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DiscoverClustersResult {
    /// Discovered per-session clusters.
    #[prost(message, repeated, tag="1")]
    pub write_sessions_clusters: ::prost::alloc::vec::Vec<WriteSessionClusters>,
    #[prost(message, repeated, tag="2")]
    pub read_sessions_clusters: ::prost::alloc::vec::Vec<ReadSessionClusters>,
    /// Latest clusters status version known to the cluster discovery service.
    #[prost(int64, tag="3")]
    pub version: i64,
}