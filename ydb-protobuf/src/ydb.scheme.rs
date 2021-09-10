/// Create directory.
/// All intermediate directories must be created
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MakeDirectoryRequest {
    #[prost(message, optional, tag = "1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(string, tag = "2")]
    pub path: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MakeDirectoryResponse {
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// Remove directory
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveDirectoryRequest {
    #[prost(message, optional, tag = "1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(string, tag = "2")]
    pub path: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveDirectoryResponse {
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// List directory
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDirectoryRequest {
    #[prost(message, optional, tag = "1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(string, tag = "2")]
    pub path: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDirectoryResponse {
    /// Holds ListDirectoryResult in case of successful call
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Permissions {
    /// SID (Security ID) of user or group
    #[prost(string, tag = "1")]
    pub subject: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "2")]
    pub permission_names: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Entry {
    /// Name of scheme entry (dir2 of /dir1/dir2)
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    /// SID (Security ID) of user or group
    #[prost(string, tag = "2")]
    pub owner: ::prost::alloc::string::String,
    #[prost(enumeration = "entry::Type", tag = "5")]
    pub r#type: i32,
    #[prost(message, repeated, tag = "6")]
    pub effective_permissions: ::prost::alloc::vec::Vec<Permissions>,
    #[prost(message, repeated, tag = "7")]
    pub permissions: ::prost::alloc::vec::Vec<Permissions>,
}
/// Nested message and enum types in `Entry`.
pub mod entry {
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
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDirectoryResult {
    #[prost(message, optional, tag = "1")]
    pub self_: ::core::option::Option<Entry>,
    #[prost(message, repeated, tag = "2")]
    pub children: ::prost::alloc::vec::Vec<Entry>,
}
/// Returns information about object with given path
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribePathRequest {
    #[prost(message, optional, tag = "1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(string, tag = "2")]
    pub path: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribePathResponse {
    /// Holds DescribePathResult in case of DescribePathResult
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribePathResult {
    #[prost(message, optional, tag = "1")]
    pub self_: ::core::option::Option<Entry>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PermissionsAction {
    #[prost(oneof = "permissions_action::Action", tags = "1, 2, 3, 4")]
    pub action: ::core::option::Option<permissions_action::Action>,
}
/// Nested message and enum types in `PermissionsAction`.
pub mod permissions_action {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Action {
        /// Grant permissions
        #[prost(message, tag = "1")]
        Grant(super::Permissions),
        /// Revoke permissions
        #[prost(message, tag = "2")]
        Revoke(super::Permissions),
        /// Rewrite permissions for given subject (last set win in case of multiple set for one subject)
        #[prost(message, tag = "3")]
        Set(super::Permissions),
        /// New owner for object
        #[prost(string, tag = "4")]
        ChangeOwner(::prost::alloc::string::String),
    }
}
/// Modify permissions of given object
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ModifyPermissionsRequest {
    #[prost(message, optional, tag = "1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(string, tag = "2")]
    pub path: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub actions: ::prost::alloc::vec::Vec<PermissionsAction>,
    /// Clear all permissions on the object for all subjects
    #[prost(bool, tag = "4")]
    pub clear_permissions: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ModifyPermissionsResponse {
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
