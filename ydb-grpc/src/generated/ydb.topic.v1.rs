/// Generated client implementations.
pub mod topic_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct TopicServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl TopicServiceClient<tonic::transport::Channel> {
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
    impl<T> TopicServiceClient<T>
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
        ) -> TopicServiceClient<InterceptedService<T, F>>
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
            TopicServiceClient::new(InterceptedService::new(inner, interceptor))
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
        /// Create Write Session
        /// Pipeline example:
        /// client                  server
        ///         InitRequest(Topic, MessageGroupID, ...)
        ///        ---------------->
        ///         InitResponse(Partition, MaxSeqNo, ...)
        ///        <----------------
        ///         WriteRequest(data1, seqNo1)
        ///        ---------------->
        ///         WriteRequest(data2, seqNo2)
        ///        ---------------->
        ///         WriteResponse(seqNo1, offset1, ...)
        ///        <----------------
        ///         WriteRequest(data3, seqNo3)
        ///        ---------------->
        ///         WriteResponse(seqNo2, offset2, ...)
        ///        <----------------
        ///         [something went wrong] (status != SUCCESS, issues not empty)
        ///        <----------------
        pub async fn stream_write(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::super::stream_write_message::FromClient,
            >,
        ) -> Result<
                tonic::Response<
                    tonic::codec::Streaming<
                        super::super::stream_write_message::FromServer,
                    >,
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
                "/Ydb.Topic.V1.TopicService/StreamWrite",
            );
            self.inner.streaming(request.into_streaming_request(), path, codec).await
        }
        /// Create Read Session
        /// Pipeline:
        /// client                  server
        ///         InitRequest(Topics, ClientId, ...)
        ///        ---------------->
        ///         InitResponse(SessionId)
        ///        <----------------
        ///         ReadRequest
        ///        ---------------->
        ///         ReadRequest
        ///        ---------------->
        ///         StartPartitionSessionRequest(Topic1, Partition1, PartitionSessionID1, ...)
        ///        <----------------
        ///         StartPartitionSessionRequest(Topic2, Partition2, PartitionSessionID2, ...)
        ///        <----------------
        ///         StartPartitionSessionResponse(PartitionSessionID1, ...)
        ///             client must respond with this message to actually start recieving data messages from this partition
        ///        ---------------->
        ///         StopPartitionSessionRequest(PartitionSessionID1, ...)
        ///        <----------------
        ///         StopPartitionSessionResponse(PartitionSessionID1, ...)
        ///             only after this response server will give this parittion to other session.
        ///        ---------------->
        ///         StartPartitionSessionResponse(PartitionSession2, ...)
        ///        ---------------->
        ///         ReadResponse(data, ...)
        ///        <----------------
        ///         CommitRequest(PartitionCommit1, ...)
        ///        ---------------->
        ///         CommitResponse(PartitionCommitAck1, ...)
        ///        <----------------
        ///         [something went wrong] (status != SUCCESS, issues not empty)
        ///        <----------------
        pub async fn stream_read(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::super::stream_read_message::FromClient,
            >,
        ) -> Result<
                tonic::Response<
                    tonic::codec::Streaming<
                        super::super::stream_read_message::FromServer,
                    >,
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
                "/Ydb.Topic.V1.TopicService/StreamRead",
            );
            self.inner.streaming(request.into_streaming_request(), path, codec).await
        }
        /// Single commit offset request.
        pub async fn commit_offset(
            &mut self,
            request: impl tonic::IntoRequest<super::super::CommitOffsetRequest>,
        ) -> Result<tonic::Response<super::super::CommitOffsetResponse>, tonic::Status> {
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
                "/Ydb.Topic.V1.TopicService/CommitOffset",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Add information about offset ranges to the transaction.
        pub async fn update_offsets_in_transaction(
            &mut self,
            request: impl tonic::IntoRequest<
                super::super::UpdateOffsetsInTransactionRequest,
            >,
        ) -> Result<
                tonic::Response<super::super::UpdateOffsetsInTransactionResponse>,
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
                "/Ydb.Topic.V1.TopicService/UpdateOffsetsInTransaction",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Create topic command.
        pub async fn create_topic(
            &mut self,
            request: impl tonic::IntoRequest<super::super::CreateTopicRequest>,
        ) -> Result<tonic::Response<super::super::CreateTopicResponse>, tonic::Status> {
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
                "/Ydb.Topic.V1.TopicService/CreateTopic",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Describe topic command.
        pub async fn describe_topic(
            &mut self,
            request: impl tonic::IntoRequest<super::super::DescribeTopicRequest>,
        ) -> Result<
                tonic::Response<super::super::DescribeTopicResponse>,
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
                "/Ydb.Topic.V1.TopicService/DescribeTopic",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Describe topic's consumer command.
        pub async fn describe_consumer(
            &mut self,
            request: impl tonic::IntoRequest<super::super::DescribeConsumerRequest>,
        ) -> Result<
                tonic::Response<super::super::DescribeConsumerResponse>,
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
                "/Ydb.Topic.V1.TopicService/DescribeConsumer",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Alter topic command.
        pub async fn alter_topic(
            &mut self,
            request: impl tonic::IntoRequest<super::super::AlterTopicRequest>,
        ) -> Result<tonic::Response<super::super::AlterTopicResponse>, tonic::Status> {
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
                "/Ydb.Topic.V1.TopicService/AlterTopic",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Drop topic command.
        pub async fn drop_topic(
            &mut self,
            request: impl tonic::IntoRequest<super::super::DropTopicRequest>,
        ) -> Result<tonic::Response<super::super::DropTopicResponse>, tonic::Status> {
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
                "/Ydb.Topic.V1.TopicService/DropTopic",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}