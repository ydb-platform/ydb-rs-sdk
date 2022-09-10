/// Generated client implementations.
pub mod cms_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct CmsServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl CmsServiceClient<tonic::transport::Channel> {
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
    impl<T> CmsServiceClient<T>
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
        ) -> CmsServiceClient<InterceptedService<T, F>>
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
            CmsServiceClient::new(InterceptedService::new(inner, interceptor))
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
        /// Create a new database.
        pub async fn create_database(
            &mut self,
            request: impl tonic::IntoRequest<super::super::CreateDatabaseRequest>,
        ) -> Result<
                tonic::Response<super::super::CreateDatabaseResponse>,
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
                "/Ydb.Cms.V1.CmsService/CreateDatabase",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Get current database's status.
        pub async fn get_database_status(
            &mut self,
            request: impl tonic::IntoRequest<super::super::GetDatabaseStatusRequest>,
        ) -> Result<
                tonic::Response<super::super::GetDatabaseStatusResponse>,
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
                "/Ydb.Cms.V1.CmsService/GetDatabaseStatus",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Alter database resources.
        pub async fn alter_database(
            &mut self,
            request: impl tonic::IntoRequest<super::super::AlterDatabaseRequest>,
        ) -> Result<
                tonic::Response<super::super::AlterDatabaseResponse>,
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
                "/Ydb.Cms.V1.CmsService/AlterDatabase",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// List all databases.
        pub async fn list_databases(
            &mut self,
            request: impl tonic::IntoRequest<super::super::ListDatabasesRequest>,
        ) -> Result<
                tonic::Response<super::super::ListDatabasesResponse>,
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
                "/Ydb.Cms.V1.CmsService/ListDatabases",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Remove database.
        pub async fn remove_database(
            &mut self,
            request: impl tonic::IntoRequest<super::super::RemoveDatabaseRequest>,
        ) -> Result<
                tonic::Response<super::super::RemoveDatabaseResponse>,
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
                "/Ydb.Cms.V1.CmsService/RemoveDatabase",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Describe supported database options.
        pub async fn describe_database_options(
            &mut self,
            request: impl tonic::IntoRequest<
                super::super::DescribeDatabaseOptionsRequest,
            >,
        ) -> Result<
                tonic::Response<super::super::DescribeDatabaseOptionsResponse>,
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
                "/Ydb.Cms.V1.CmsService/DescribeDatabaseOptions",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}