/// Generated client implementations.
pub mod operation_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct OperationServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl OperationServiceClient<tonic::transport::Channel> {
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
    impl<T> OperationServiceClient<T>
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
        ) -> OperationServiceClient<InterceptedService<T, F>>
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
            OperationServiceClient::new(InterceptedService::new(inner, interceptor))
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
        /// Check status for a given operation.
        pub async fn get_operation(
            &mut self,
            request: impl tonic::IntoRequest<
                super::super::super::operations::GetOperationRequest,
            >,
        ) -> Result<
                tonic::Response<super::super::super::operations::GetOperationResponse>,
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
                "/Ydb.Operation.V1.OperationService/GetOperation",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Starts cancellation of a long-running operation,
        /// Clients can use GetOperation to check whether the cancellation succeeded
        /// or whether the operation completed despite cancellation.
        pub async fn cancel_operation(
            &mut self,
            request: impl tonic::IntoRequest<
                super::super::super::operations::CancelOperationRequest,
            >,
        ) -> Result<
                tonic::Response<
                    super::super::super::operations::CancelOperationResponse,
                >,
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
                "/Ydb.Operation.V1.OperationService/CancelOperation",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Forgets long-running operation. It does not cancel the operation and returns
        /// an error if operation was not completed.
        pub async fn forget_operation(
            &mut self,
            request: impl tonic::IntoRequest<
                super::super::super::operations::ForgetOperationRequest,
            >,
        ) -> Result<
                tonic::Response<
                    super::super::super::operations::ForgetOperationResponse,
                >,
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
                "/Ydb.Operation.V1.OperationService/ForgetOperation",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Lists operations that match the specified filter in the request.
        pub async fn list_operations(
            &mut self,
            request: impl tonic::IntoRequest<
                super::super::super::operations::ListOperationsRequest,
            >,
        ) -> Result<
                tonic::Response<super::super::super::operations::ListOperationsResponse>,
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
                "/Ydb.Operation.V1.OperationService/ListOperations",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}