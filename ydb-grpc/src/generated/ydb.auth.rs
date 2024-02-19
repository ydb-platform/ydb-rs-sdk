#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LoginRequest {
    #[prost(message, optional, tag = "1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(string, tag = "2")]
    pub user: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub password: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LoginResponse {
    /// After successfull completion must contain LoginResult.
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LoginResult {
    #[prost(string, tag = "1")]
    pub token: ::prost::alloc::string::String,
}