#[doc = r" Generated client implementations."]
pub mod operation_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct OperationServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl OperationServiceClient<tonic::transport::Channel> {
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
    impl<T> OperationServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + Sync + 'static,
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
        ) -> OperationServiceClient<InterceptedService<T, F>>
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
            OperationServiceClient::new(InterceptedService::new(inner, interceptor))
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
        #[doc = " Check status for a given operation."]
        pub async fn get_operation(
            &mut self,
            request: impl tonic::IntoRequest<super::super::super::operations::GetOperationRequest>,
        ) -> Result<
            tonic::Response<super::super::super::operations::GetOperationResponse>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
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
        #[doc = " Starts cancellation of a long-running operation,"]
        #[doc = " Clients can use GetOperation to check whether the cancellation succeeded"]
        #[doc = " or whether the operation completed despite cancellation."]
        pub async fn cancel_operation(
            &mut self,
            request: impl tonic::IntoRequest<super::super::super::operations::CancelOperationRequest>,
        ) -> Result<
            tonic::Response<super::super::super::operations::CancelOperationResponse>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
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
        #[doc = " Forgets long-running operation. It does not cancel the operation and returns"]
        #[doc = " an error if operation was not completed."]
        pub async fn forget_operation(
            &mut self,
            request: impl tonic::IntoRequest<super::super::super::operations::ForgetOperationRequest>,
        ) -> Result<
            tonic::Response<super::super::super::operations::ForgetOperationResponse>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
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
        #[doc = " Lists operations that match the specified filter in the request."]
        pub async fn list_operations(
            &mut self,
            request: impl tonic::IntoRequest<super::super::super::operations::ListOperationsRequest>,
        ) -> Result<
            tonic::Response<super::super::super::operations::ListOperationsResponse>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
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
