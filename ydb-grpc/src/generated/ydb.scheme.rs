/// Create directory.
/// All intermediate directories must be created
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MakeDirectoryRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MakeDirectoryResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// Remove directory
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveDirectoryRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveDirectoryResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// List directory
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDirectoryRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDirectoryResponse {
    /// Holds ListDirectoryResult in case of successful call
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Permissions {
    /// SID (Security ID) of user or group
    #[prost(string, tag="1")]
    pub subject: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="2")]
    pub permission_names: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Entry {
    /// Name of scheme entry (dir2 of /dir1/dir2)
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// SID (Security ID) of user or group
    #[prost(string, tag="2")]
    pub owner: ::prost::alloc::string::String,
    #[prost(enumeration="entry::Type", tag="5")]
    pub r#type: i32,
    #[prost(message, repeated, tag="6")]
    pub effective_permissions: ::prost::alloc::vec::Vec<Permissions>,
    #[prost(message, repeated, tag="7")]
    pub permissions: ::prost::alloc::vec::Vec<Permissions>,
    /// Size of entry in bytes. Currently filled for:
    /// - TABLE;
    /// - DATABASE.
    /// Empty (zero) in other cases.
    #[prost(uint64, tag="8")]
    pub size_bytes: u64,
}
/// Nested message and enum types in `Entry`.
pub mod entry {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Type {
        Unspecified = 0,
        Directory = 1,
        Table = 2,
        PersQueueGroup = 3,
        Database = 4,
        RtmrVolume = 5,
        BlockStoreVolume = 6,
        CoordinationNode = 7,
        Sequence = 15,
        Replication = 16,
    }
    impl Type {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Type::Unspecified => "TYPE_UNSPECIFIED",
                Type::Directory => "DIRECTORY",
                Type::Table => "TABLE",
                Type::PersQueueGroup => "PERS_QUEUE_GROUP",
                Type::Database => "DATABASE",
                Type::RtmrVolume => "RTMR_VOLUME",
                Type::BlockStoreVolume => "BLOCK_STORE_VOLUME",
                Type::CoordinationNode => "COORDINATION_NODE",
                Type::Sequence => "SEQUENCE",
                Type::Replication => "REPLICATION",
            }
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDirectoryResult {
    #[prost(message, optional, tag="1")]
    pub self_: ::core::option::Option<Entry>,
    #[prost(message, repeated, tag="2")]
    pub children: ::prost::alloc::vec::Vec<Entry>,
}
/// Returns information about object with given path
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribePathRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribePathResponse {
    /// Holds DescribePathResult in case of DescribePathResult
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribePathResult {
    #[prost(message, optional, tag="1")]
    pub self_: ::core::option::Option<Entry>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PermissionsAction {
    #[prost(oneof="permissions_action::Action", tags="1, 2, 3, 4")]
    pub action: ::core::option::Option<permissions_action::Action>,
}
/// Nested message and enum types in `PermissionsAction`.
pub mod permissions_action {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Action {
        /// Grant permissions
        #[prost(message, tag="1")]
        Grant(super::Permissions),
        /// Revoke permissions
        #[prost(message, tag="2")]
        Revoke(super::Permissions),
        /// Rewrite permissions for given subject (last set win in case of multiple set for one subject)
        #[prost(message, tag="3")]
        Set(super::Permissions),
        /// New owner for object
        #[prost(string, tag="4")]
        ChangeOwner(::prost::alloc::string::String),
    }
}
/// Modify permissions of given object
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ModifyPermissionsRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="3")]
    pub actions: ::prost::alloc::vec::Vec<PermissionsAction>,
    /// Clear all permissions on the object for all subjects
    #[prost(bool, tag="4")]
    pub clear_permissions: bool,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ModifyPermissionsResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}