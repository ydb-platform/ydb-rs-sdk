#[doc = r" Generated client implementations."]
pub mod scheme_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct SchemeServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl SchemeServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> SchemeServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> SchemeServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            SchemeServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        #[doc = " Make Directory."]
        pub async fn make_directory(
            &mut self,
            request: impl tonic::IntoRequest<super::super::MakeDirectoryRequest>,
        ) -> Result<tonic::Response<super::super::MakeDirectoryResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/Ydb.Scheme.V1.SchemeService/MakeDirectory");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Remove Directory."]
        pub async fn remove_directory(
            &mut self,
            request: impl tonic::IntoRequest<super::super::RemoveDirectoryRequest>,
        ) -> Result<tonic::Response<super::super::RemoveDirectoryResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.Scheme.V1.SchemeService/RemoveDirectory",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Returns information about given directory and objects inside it."]
        pub async fn list_directory(
            &mut self,
            request: impl tonic::IntoRequest<super::super::ListDirectoryRequest>,
        ) -> Result<tonic::Response<super::super::ListDirectoryResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/Ydb.Scheme.V1.SchemeService/ListDirectory");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Returns information about object with given path."]
        pub async fn describe_path(
            &mut self,
            request: impl tonic::IntoRequest<super::super::DescribePathRequest>,
        ) -> Result<tonic::Response<super::super::DescribePathResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/Ydb.Scheme.V1.SchemeService/DescribePath");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Modify permissions."]
        pub async fn modify_permissions(
            &mut self,
            request: impl tonic::IntoRequest<super::super::ModifyPermissionsRequest>,
        ) -> Result<tonic::Response<super::super::ModifyPermissionsResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.Scheme.V1.SchemeService/ModifyPermissions",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
