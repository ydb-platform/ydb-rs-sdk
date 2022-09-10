/// Generated client implementations.
pub mod rate_limiter_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct RateLimiterServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl RateLimiterServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> RateLimiterServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> RateLimiterServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            RateLimiterServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Create a new resource in existing coordination node.
        pub async fn create_resource(
            &mut self,
            request: impl tonic::IntoRequest<super::super::CreateResourceRequest>,
        ) -> Result<
                tonic::Response<super::super::CreateResourceResponse>,
                tonic::Status,
            > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.RateLimiter.V1.RateLimiterService/CreateResource",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Update a resource in coordination node.
        pub async fn alter_resource(
            &mut self,
            request: impl tonic::IntoRequest<super::super::AlterResourceRequest>,
        ) -> Result<
                tonic::Response<super::super::AlterResourceResponse>,
                tonic::Status,
            > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.RateLimiter.V1.RateLimiterService/AlterResource",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Delete a resource from coordination node.
        pub async fn drop_resource(
            &mut self,
            request: impl tonic::IntoRequest<super::super::DropResourceRequest>,
        ) -> Result<tonic::Response<super::super::DropResourceResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.RateLimiter.V1.RateLimiterService/DropResource",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// List resources in given coordination node.
        pub async fn list_resources(
            &mut self,
            request: impl tonic::IntoRequest<super::super::ListResourcesRequest>,
        ) -> Result<
                tonic::Response<super::super::ListResourcesResponse>,
                tonic::Status,
            > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.RateLimiter.V1.RateLimiterService/ListResources",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Describe properties of resource in coordination node.
        pub async fn describe_resource(
            &mut self,
            request: impl tonic::IntoRequest<super::super::DescribeResourceRequest>,
        ) -> Result<
                tonic::Response<super::super::DescribeResourceResponse>,
                tonic::Status,
            > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.RateLimiter.V1.RateLimiterService/DescribeResource",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Take units for usage of a resource in coordination node.
        pub async fn acquire_resource(
            &mut self,
            request: impl tonic::IntoRequest<super::super::AcquireResourceRequest>,
        ) -> Result<
                tonic::Response<super::super::AcquireResourceResponse>,
                tonic::Status,
            > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.RateLimiter.V1.RateLimiterService/AcquireResource",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}