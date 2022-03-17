#[doc = r" Generated client implementations."]
pub mod table_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct TableServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl TableServiceClient<tonic::transport::Channel> {
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
    impl<T> TableServiceClient<T>
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
        ) -> TableServiceClient<InterceptedService<T, F>>
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
            TableServiceClient::new(InterceptedService::new(inner, interceptor))
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
        #[doc = " Create new session. Implicit session creation is forbidden,"]
        #[doc = " so user must create new session before execute any query,"]
        #[doc = " otherwise BAD_SESSION status will be returned."]
        #[doc = " Simultaneous execution of requests are forbiden."]
        #[doc = " Sessions are volatile, can be invalidated by server, for example in case"]
        #[doc = " of fatal errors. All requests with this session will fail with BAD_SESSION status."]
        #[doc = " So, client must be able to handle BAD_SESSION status."]
        pub async fn create_session(
            &mut self,
            request: impl tonic::IntoRequest<super::super::CreateSessionRequest>,
        ) -> Result<tonic::Response<super::super::CreateSessionResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/Ydb.Table.V1.TableService/CreateSession");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Ends a session, releasing server resources associated with it."]
        pub async fn delete_session(
            &mut self,
            request: impl tonic::IntoRequest<super::super::DeleteSessionRequest>,
        ) -> Result<tonic::Response<super::super::DeleteSessionResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/Ydb.Table.V1.TableService/DeleteSession");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Idle sessions can be kept alive by calling KeepAlive periodically."]
        pub async fn keep_alive(
            &mut self,
            request: impl tonic::IntoRequest<super::super::KeepAliveRequest>,
        ) -> Result<tonic::Response<super::super::KeepAliveResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/Ydb.Table.V1.TableService/KeepAlive");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Creates new table."]
        pub async fn create_table(
            &mut self,
            request: impl tonic::IntoRequest<super::super::CreateTableRequest>,
        ) -> Result<tonic::Response<super::super::CreateTableResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/Ydb.Table.V1.TableService/CreateTable");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Drop table."]
        pub async fn drop_table(
            &mut self,
            request: impl tonic::IntoRequest<super::super::DropTableRequest>,
        ) -> Result<tonic::Response<super::super::DropTableResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/Ydb.Table.V1.TableService/DropTable");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Modifies schema of given table."]
        pub async fn alter_table(
            &mut self,
            request: impl tonic::IntoRequest<super::super::AlterTableRequest>,
        ) -> Result<tonic::Response<super::super::AlterTableResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/Ydb.Table.V1.TableService/AlterTable");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Creates copy of given table."]
        pub async fn copy_table(
            &mut self,
            request: impl tonic::IntoRequest<super::super::CopyTableRequest>,
        ) -> Result<tonic::Response<super::super::CopyTableResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/Ydb.Table.V1.TableService/CopyTable");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Creates consistent copy of given tables."]
        pub async fn copy_tables(
            &mut self,
            request: impl tonic::IntoRequest<super::super::CopyTablesRequest>,
        ) -> Result<tonic::Response<super::super::CopyTablesResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/Ydb.Table.V1.TableService/CopyTables");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Creates consistent move of given tables."]
        pub async fn rename_tables(
            &mut self,
            request: impl tonic::IntoRequest<super::super::RenameTablesRequest>,
        ) -> Result<tonic::Response<super::super::RenameTablesResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/Ydb.Table.V1.TableService/RenameTables");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Returns information about given table (metadata)."]
        pub async fn describe_table(
            &mut self,
            request: impl tonic::IntoRequest<super::super::DescribeTableRequest>,
        ) -> Result<tonic::Response<super::super::DescribeTableResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/Ydb.Table.V1.TableService/DescribeTable");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Explains data query."]
        #[doc = " SessionId of previously created session must be provided."]
        pub async fn explain_data_query(
            &mut self,
            request: impl tonic::IntoRequest<super::super::ExplainDataQueryRequest>,
        ) -> Result<tonic::Response<super::super::ExplainDataQueryResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/Ydb.Table.V1.TableService/ExplainDataQuery");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Prepares data query, returns query id."]
        #[doc = " SessionId of previously created session must be provided."]
        pub async fn prepare_data_query(
            &mut self,
            request: impl tonic::IntoRequest<super::super::PrepareDataQueryRequest>,
        ) -> Result<tonic::Response<super::super::PrepareDataQueryResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/Ydb.Table.V1.TableService/PrepareDataQuery");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Executes data query."]
        #[doc = " SessionId of previously created session must be provided."]
        pub async fn execute_data_query(
            &mut self,
            request: impl tonic::IntoRequest<super::super::ExecuteDataQueryRequest>,
        ) -> Result<tonic::Response<super::super::ExecuteDataQueryResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/Ydb.Table.V1.TableService/ExecuteDataQuery");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Executes scheme query."]
        #[doc = " SessionId of previously created session must be provided."]
        pub async fn execute_scheme_query(
            &mut self,
            request: impl tonic::IntoRequest<super::super::ExecuteSchemeQueryRequest>,
        ) -> Result<tonic::Response<super::super::ExecuteSchemeQueryResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.Table.V1.TableService/ExecuteSchemeQuery",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Begins new transaction."]
        pub async fn begin_transaction(
            &mut self,
            request: impl tonic::IntoRequest<super::super::BeginTransactionRequest>,
        ) -> Result<tonic::Response<super::super::BeginTransactionResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/Ydb.Table.V1.TableService/BeginTransaction");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Commits specified active transaction."]
        pub async fn commit_transaction(
            &mut self,
            request: impl tonic::IntoRequest<super::super::CommitTransactionRequest>,
        ) -> Result<tonic::Response<super::super::CommitTransactionResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.Table.V1.TableService/CommitTransaction",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Performs a rollback of the specified active transaction."]
        pub async fn rollback_transaction(
            &mut self,
            request: impl tonic::IntoRequest<super::super::RollbackTransactionRequest>,
        ) -> Result<tonic::Response<super::super::RollbackTransactionResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.Table.V1.TableService/RollbackTransaction",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Describe supported table options."]
        pub async fn describe_table_options(
            &mut self,
            request: impl tonic::IntoRequest<super::super::DescribeTableOptionsRequest>,
        ) -> Result<tonic::Response<super::super::DescribeTableOptionsResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.Table.V1.TableService/DescribeTableOptions",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Streaming read table"]
        pub async fn stream_read_table(
            &mut self,
            request: impl tonic::IntoRequest<super::super::ReadTableRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::super::ReadTableResponse>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/Ydb.Table.V1.TableService/StreamReadTable");
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
        #[doc = " Upserts a batch of rows non-transactionally."]
        #[doc = " Returns success only when all rows were successfully upserted. In case of an error some rows might"]
        #[doc = " be upserted and some might not."]
        pub async fn bulk_upsert(
            &mut self,
            request: impl tonic::IntoRequest<super::super::BulkUpsertRequest>,
        ) -> Result<tonic::Response<super::super::BulkUpsertResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/Ydb.Table.V1.TableService/BulkUpsert");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Executes scan query with streaming result."]
        pub async fn stream_execute_scan_query(
            &mut self,
            request: impl tonic::IntoRequest<super::super::ExecuteScanQueryRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::super::ExecuteScanQueryPartialResponse>>,
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
                "/Ydb.Table.V1.TableService/StreamExecuteScanQuery",
            );
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
    }
}
