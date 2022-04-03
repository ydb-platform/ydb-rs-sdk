#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListEndpointsRequest {
    #[prost(string, tag="1")]
    pub database: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="2")]
    pub service: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EndpointInfo {
    #[prost(string, tag="1")]
    pub address: ::prost::alloc::string::String,
    #[prost(uint32, tag="2")]
    pub port: u32,
    #[prost(float, tag="3")]
    pub load_factor: f32,
    #[prost(bool, tag="4")]
    pub ssl: bool,
    #[prost(string, repeated, tag="5")]
    pub service: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag="6")]
    pub location: ::prost::alloc::string::String,
    #[prost(uint32, tag="7")]
    pub node_id: u32,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListEndpointsResult {
    #[prost(message, repeated, tag="1")]
    pub endpoints: ::prost::alloc::vec::Vec<EndpointInfo>,
    #[prost(string, tag="2")]
    pub self_location: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListEndpointsResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WhoAmIRequest {
    /// Include user groups in response
    #[prost(bool, tag="1")]
    pub include_groups: bool,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WhoAmIResult {
    /// User SID (Security ID)
    #[prost(string, tag="1")]
    pub user: ::prost::alloc::string::String,
    /// List of group SIDs (Security IDs) for the user
    #[prost(string, repeated, tag="2")]
    pub groups: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WhoAmIResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
