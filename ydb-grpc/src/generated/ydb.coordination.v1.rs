/// Generated client implementations.
pub mod coordination_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct CoordinationServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl CoordinationServiceClient<tonic::transport::Channel> {
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
    impl<T> CoordinationServiceClient<T>
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
        ) -> CoordinationServiceClient<InterceptedService<T, F>>
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
            CoordinationServiceClient::new(InterceptedService::new(inner, interceptor))
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
        ///*
        /// Bidirectional stream used to establish a session with a coordination node
        /// Relevant APIs for managing semaphores, distributed locking, creating or
        /// restoring a previously established session are described using nested
        /// messages in SessionRequest and SessionResponse. Session is established
        /// with a specific coordination node (previously created using CreateNode
        /// below) and semaphores are local to that coordination node.
        pub async fn session(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::super::SessionRequest,
            >,
        ) -> Result<
                tonic::Response<tonic::codec::Streaming<super::super::SessionResponse>>,
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
                "/Ydb.Coordination.V1.CoordinationService/Session",
            );
            self.inner.streaming(request.into_streaming_request(), path, codec).await
        }
        /// Creates a new coordination node
        pub async fn create_node(
            &mut self,
            request: impl tonic::IntoRequest<super::super::CreateNodeRequest>,
        ) -> Result<tonic::Response<super::super::CreateNodeResponse>, tonic::Status> {
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
                "/Ydb.Coordination.V1.CoordinationService/CreateNode",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Modifies settings of a coordination node
        pub async fn alter_node(
            &mut self,
            request: impl tonic::IntoRequest<super::super::AlterNodeRequest>,
        ) -> Result<tonic::Response<super::super::AlterNodeResponse>, tonic::Status> {
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
                "/Ydb.Coordination.V1.CoordinationService/AlterNode",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Drops a coordination node
        pub async fn drop_node(
            &mut self,
            request: impl tonic::IntoRequest<super::super::DropNodeRequest>,
        ) -> Result<tonic::Response<super::super::DropNodeResponse>, tonic::Status> {
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
                "/Ydb.Coordination.V1.CoordinationService/DropNode",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Describes a coordination node
        pub async fn describe_node(
            &mut self,
            request: impl tonic::IntoRequest<super::super::DescribeNodeRequest>,
        ) -> Result<tonic::Response<super::super::DescribeNodeResponse>, tonic::Status> {
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
                "/Ydb.Coordination.V1.CoordinationService/DescribeNode",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}