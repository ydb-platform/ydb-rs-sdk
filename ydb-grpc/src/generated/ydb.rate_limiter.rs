//
// Rate Limiter control API.
//

//
// Resource properties.
//

/// Settings for hierarchical deficit round robin (HDRR) algorithm.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HierarchicalDrrSettings {
    /// Resource consumption speed limit.
    /// Value is required for root resource.
    /// 0 is equivalent to not set.
    /// Must be nonnegative.
    #[prost(double, tag="1")]
    pub max_units_per_second: f64,
    /// Maximum burst size of resource consumption across the whole cluster
    /// divided by max_units_per_second.
    /// Default value is 1.
    /// This means that maximum burst size might be equal to max_units_per_second.
    /// 0 is equivalent to not set.
    /// Must be nonnegative.
    #[prost(double, tag="2")]
    pub max_burst_size_coefficient: f64,
    /// Prefetch in local bucket up to prefetch_coefficient*max_units_per_second units (full size).
    /// Default value is inherited from parent or 0.2 for root.
    /// Disables prefetching if any negative value is set
    /// (It is useful to avoid bursts in case of large number of local buckets).
    #[prost(double, tag="3")]
    pub prefetch_coefficient: f64,
    /// Prefetching starts if there is less than prefetch_watermark fraction of full local bucket left.
    /// Default value is inherited from parent or 0.75 for root.
    /// Must be nonnegative and less than or equal to 1.
    #[prost(double, tag="4")]
    pub prefetch_watermark: f64,
}
/// Rate limiter resource description.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Resource {
    /// Resource path. Elements are separated by slash.
    /// The first symbol is not slash.
    /// The first element is root resource name.
    /// Resource path is the path of resource inside coordination node.
    #[prost(string, tag="1")]
    pub resource_path: ::prost::alloc::string::String,
    #[prost(oneof="resource::Type", tags="2")]
    pub r#type: ::core::option::Option<resource::Type>,
}
/// Nested message and enum types in `Resource`.
pub mod resource {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Type {
        /// Settings for Hierarchical DRR algorithm.
        #[prost(message, tag="2")]
        HierarchicalDrr(super::HierarchicalDrrSettings),
    }
}
//
// CreateResource method.
//

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateResourceRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Path of a coordination node.
    #[prost(string, tag="2")]
    pub coordination_node_path: ::prost::alloc::string::String,
    /// Resource properties.
    #[prost(message, optional, tag="3")]
    pub resource: ::core::option::Option<Resource>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateResourceResponse {
    /// Holds CreateResourceResult in case of successful call.
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateResourceResult {
}
//
// AlterResource method.
//

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterResourceRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Path of a coordination node.
    #[prost(string, tag="2")]
    pub coordination_node_path: ::prost::alloc::string::String,
    /// New resource properties.
    #[prost(message, optional, tag="3")]
    pub resource: ::core::option::Option<Resource>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterResourceResponse {
    /// Holds AlterResourceResult in case of successful call.
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterResourceResult {
}
//
// DropResource method.
//

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropResourceRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Path of a coordination node.
    #[prost(string, tag="2")]
    pub coordination_node_path: ::prost::alloc::string::String,
    /// Path of resource inside a coordination node.
    #[prost(string, tag="3")]
    pub resource_path: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropResourceResponse {
    /// Holds DropResourceResult in case of successful call.
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropResourceResult {
}
//
// ListResources method.
//

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListResourcesRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Path of a coordination node.
    #[prost(string, tag="2")]
    pub coordination_node_path: ::prost::alloc::string::String,
    /// Path of resource inside a coordination node.
    /// May be empty.
    /// In that case all root resources will be listed.
    #[prost(string, tag="3")]
    pub resource_path: ::prost::alloc::string::String,
    /// List resources recursively.
    #[prost(bool, tag="4")]
    pub recursive: bool,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListResourcesResponse {
    /// Holds ListResourcesResult in case of successful call.
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListResourcesResult {
    #[prost(string, repeated, tag="1")]
    pub resource_paths: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
//
// DescribeResource method.
//

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeResourceRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Path of a coordination node.
    #[prost(string, tag="2")]
    pub coordination_node_path: ::prost::alloc::string::String,
    /// Path of resource inside a coordination node.
    #[prost(string, tag="3")]
    pub resource_path: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeResourceResponse {
    /// Holds DescribeResourceResult in case of successful call.
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeResourceResult {
    #[prost(message, optional, tag="1")]
    pub resource: ::core::option::Option<Resource>,
}
//
// AcquireResource method.
//

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcquireResourceRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Path of a coordination node.
    #[prost(string, tag="2")]
    pub coordination_node_path: ::prost::alloc::string::String,
    /// Path of resource inside a coordination node.
    #[prost(string, tag="3")]
    pub resource_path: ::prost::alloc::string::String,
    #[prost(oneof="acquire_resource_request::Units", tags="4, 5")]
    pub units: ::core::option::Option<acquire_resource_request::Units>,
}
/// Nested message and enum types in `AcquireResourceRequest`.
pub mod acquire_resource_request {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Units {
        /// Request resource's units for usage.
        #[prost(uint64, tag="4")]
        Required(u64),
        /// Actually used resource's units by client.
        #[prost(uint64, tag="5")]
        Used(u64),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcquireResourceResponse {
    /// Holds AcquireResourceResult in case of successful call.
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcquireResourceResult {
}
