use crate::client::TimeoutSettings;

use crate::errors::*;
use crate::session::TableSession;
use crate::session_pool::{SessionPool, TableSessionPool};
use crate::types::Value;

use crate::grpc_connection_manager::GrpcConnectionManager;

use crate::grpc_wrapper::grpc_limits::WithGrpcMaxMessageSize;
use crate::grpc_wrapper::raw_table_service::bulk_upsert::RawBulkUpsertRequest;
use crate::grpc_wrapper::raw_table_service::client::RawTableClient;
use crate::grpc_wrapper::raw_table_service::copy_table::{
    RawCopyTableRequest, RawCopyTablesRequest,
};
use crate::grpc_wrapper::raw_table_service::describe_table::{
    table_description_from_raw, RawDescribeTableRequest,
};
use crate::grpc_wrapper::raw_table_service::describe_table_options::{
    RawDescribeTableOptionsRequest, RawDescribeTableOptionsResult,
};
use crate::grpc_wrapper::raw_table_service::drop_table::RawDropTableRequest;
use crate::grpc_wrapper::raw_table_service::read_rows::RawReadRowsRequest;
use crate::grpc_wrapper::raw_table_service::rename_tables::{
    RawRenameTableItem, RawRenameTablesRequest,
};
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use crate::retry::{NoRetrier, Retry, RetryParams, TimeoutRetrier};
use crate::session::CreateTableClient;
use crate::table_requests::{
    AlterTableRequest, CreateTableRequest, DropTableRequest, ReadRowsRequest,
    TableOptionsDescription,
};
use crate::table_service_types::{CopyTableItem, RenameTableItem, TableDescription};
use crate::types_converters::try_vec_to_list_of_structs;
use itertools::Itertools;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use ydb_grpc::ydb_proto::table::v1::table_service_client::TableServiceClient;

pub(crate) type TableServiceClientType = TableServiceClient<InterceptedChannel>;

impl WithGrpcMaxMessageSize for TableServiceClientType {
    fn with_grpc_max_message_size(self, bytes: usize) -> Self {
        self.max_decoding_message_size(bytes)
            .max_encoding_message_size(bytes)
    }
}

/// Retry options for internal [`TableClient`] operations.
pub struct RetryOptions {
    /// Operations under the option is idempotent. Repeat completed operation - safe.
    idempotent_operation: bool,

    /// Algorithm for retry decision
    retrier: Option<Arc<Box<dyn Retry>>>,
}

impl RetryOptions {
    /// Default option for no retries
    pub fn new() -> Self {
        Self {
            idempotent_operation: false,
            retrier: None,
        }
    }

    /// Operations under the options is safe for complete few times instead of one.
    #[allow(dead_code)]
    pub(crate) fn with_idempotent(mut self, idempotent: bool) -> Self {
        self.idempotent_operation = idempotent;
        self
    }

    /// Set retry timeout
    #[allow(dead_code)]
    pub(crate) fn with_timeout(mut self, timeout: Duration) -> Self {
        self.retrier = Some(Arc::new(Box::new(TimeoutRetrier { timeout })));
        self
    }
}

impl Default for RetryOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Client for YDB Table service: DDL via RPC (`CreateTable`, …), sessionless data plane (`ReadRows`, `BulkUpsert`), describe.
///
/// Ad-hoc DDL YQL (`CREATE TABLE` / `DROP TABLE` as text) belongs to [`crate::QueryClient::exec`] with [`crate::TxMode::Implicit`].
/// YQL execution, transactions, explain, and streaming reads also belong to [`crate::QueryClient`].
#[derive(Clone)]
pub struct TableClient {
    session_pool: TableSessionPool,
    retrier: Arc<Box<dyn Retry>>,
    idempotent_operation: bool,
    timeouts: TimeoutSettings,
}

impl TableClient {
    pub(crate) fn new(
        connection_manager: GrpcConnectionManager,
        timeouts: TimeoutSettings,
        session_pool: SessionPool,
    ) -> Self {
        Self {
            session_pool: TableSessionPool::from_shared(session_pool, connection_manager, timeouts),
            retrier: Arc::new(Box::<TimeoutRetrier>::default()),
            idempotent_operation: false,
            timeouts,
        }
    }

    pub fn clone_with_timeouts(&self, timeouts: TimeoutSettings) -> Self {
        Self {
            timeouts,
            ..self.clone()
        }
    }

    #[allow(dead_code)]
    pub fn clone_with_retry_timeout(&self, timeout: Duration) -> Self {
        Self {
            retrier: Arc::new(Box::new(TimeoutRetrier { timeout })),
            ..self.clone()
        }
    }

    #[allow(dead_code)]
    pub fn clone_with_no_retry(&self) -> Self {
        Self {
            retrier: Arc::new(Box::new(NoRetrier {})),
            ..self.clone()
        }
    }

    pub(crate) async fn create_session(&self) -> YdbResult<TableSession> {
        Ok(self
            .session_pool
            .session()
            .await?
            .with_timeouts(self.timeouts))
    }

    async fn sessionless_table_client(&self) -> YdbResult<RawTableClient> {
        self.session_pool
            .connection_manager()
            .create_table_client(self.timeouts)
            .await
    }

    async fn bulk_upsert_once(&self, table_path: String, rows: Value) -> YdbResult<()> {
        let raw_rows: crate::grpc_wrapper::raw_table_service::value::RawTypedValue =
            rows.try_into().map_err(YdbError::from)?;
        let mut client = self.sessionless_table_client().await?;
        client
            .bulk_upsert(RawBulkUpsertRequest {
                table: table_path,
                rows: raw_rows.into(),
                operation_params: self.timeouts.operation_params(),
            })
            .await
            .map_err(YdbError::from)?;
        Ok(())
    }

    async fn read_rows_once(&self, request: RawReadRowsRequest) -> YdbResult<crate::ResultSet> {
        let mut client = self.sessionless_table_client().await?;
        let raw_response = client.read_rows(request).await.map_err(YdbError::from)?;
        raw_response.result_set.try_into()
    }

    async fn retry_idempotent<CallbackFuture, CallbackResult>(
        &self,
        callback: impl Fn() -> CallbackFuture,
    ) -> YdbResult<CallbackResult>
    where
        CallbackFuture: Future<Output = YdbResult<CallbackResult>>,
    {
        Self {
            idempotent_operation: true,
            ..self.clone()
        }
        .retry_operation(callback)
        .await
    }

    async fn retry_operation<CallbackFuture, CallbackResult>(
        &self,
        callback: impl Fn() -> CallbackFuture,
    ) -> YdbResult<CallbackResult>
    where
        CallbackFuture: Future<Output = YdbResult<CallbackResult>>,
    {
        let mut attempt: usize = 0;
        let start = Instant::now();
        loop {
            attempt += 1;
            let last_err = match callback().await {
                Ok(res) => return Ok(res),
                Err(err) => match (err.need_retry(), self.idempotent_operation) {
                    (NeedRetry::True, _) => err,
                    (NeedRetry::IdempotentOnly, true) => err,
                    _ => return Err(err),
                },
            };

            let now = std::time::Instant::now();
            let retry_decision = self.retrier.wait_duration(RetryParams {
                attempt,
                time_from_start: now.duration_since(start),
            });
            if !retry_decision.allow_retry {
                return Err(last_err);
            }
            tokio::time::sleep(retry_decision.wait_timeout).await;
        }
    }

    /// Read rows by primary key without opening a session (go-sdk: `table.Client.ReadRows`).
    ///
    /// `request.keys` must be a list of [`Value::Struct`] primary-key values.
    /// Returns an empty result set when `keys` is empty.
    pub async fn read_rows_request(&self, request: ReadRowsRequest) -> YdbResult<crate::ResultSet> {
        if request.keys.is_empty() {
            return Ok(crate::ResultSet::default());
        }

        let raw = request.clone().into_raw(String::new())?;
        self.retry_idempotent(|| async { self.read_rows_once(raw.clone()).await })
            .await
    }

    /// From table with given path `table_path` request rows by primary keys `keys`, which must be
    /// vector of [`Value::Struct`]. If any key does not meet requirement, error will be returned.
    ///
    /// If `columns` is `None`, all columns of requested rows will be returned. Otherwise, only
    /// `columns` will be returned.
    pub async fn read_rows(
        &self,
        table_path: impl Into<String>,
        keys: Vec<Value>,
        columns: Option<Vec<String>>,
    ) -> YdbResult<crate::ResultSet> {
        let mut request = ReadRowsRequest::new(table_path).with_keys(keys);
        if let Some(columns) = columns {
            request.columns = columns;
        }
        self.read_rows_request(request).await
    }

    /// Bulk upsert rows without opening a session (go-sdk: `table.Client.BulkUpsert`).
    pub async fn bulk_upsert(
        &self,
        table_path: impl Into<String>,
        rows: Vec<Value>,
    ) -> YdbResult<()> {
        let Some(value) = try_vec_to_list_of_structs(rows)? else {
            return Ok(());
        };
        let table_path = table_path.into();
        self.retry_idempotent(|| async {
            self.bulk_upsert_once(table_path.clone(), value.clone())
                .await
        })
        .await
    }

    pub async fn copy_table(&self, source_path: String, destination_path: String) -> YdbResult<()> {
        self.retry_operation(|| async {
            let mut session = self.create_session().await?;
            let session_id = session.id.clone();
            let operation_params = session.operation_params();
            session
                .in_flight_rpc(async |table| {
                    table
                        .copy_table(RawCopyTableRequest {
                            session_id,
                            source_path: source_path.clone(),
                            destination_path: destination_path.clone(),
                            operation_params,
                        })
                        .await
                })
                .await
        })
        .await
    }

    pub async fn copy_tables(&self, tables: Vec<CopyTableItem>) -> YdbResult<()> {
        self.retry_operation(|| async {
            let mut session = self.create_session().await?;
            let session_id = session.id.clone();
            let operation_params = session.operation_params();
            session
                .in_flight_rpc(async |table| {
                    table
                        .copy_tables(RawCopyTablesRequest {
                            operation_params,
                            session_id,
                            tables: tables.clone().into_iter().map_into().collect(),
                        })
                        .await
                })
                .await
        })
        .await
    }

    pub async fn rename_table(
        &self,
        source_path: String,
        destination_path: String,
        replace_destination: bool,
    ) -> YdbResult<()> {
        self.retry_operation(|| async {
            let mut session = self.create_session().await?;
            let session_id = session.id.clone();
            let operation_params = session.operation_params();
            session
                .in_flight_rpc(async |table| {
                    table
                        .rename_tables(RawRenameTablesRequest {
                            session_id,
                            operation_params,
                            tables: vec![RawRenameTableItem {
                                source_path: source_path.clone(),
                                destination_path: destination_path.clone(),
                                replace_destination,
                            }],
                        })
                        .await
                })
                .await
        })
        .await
    }

    pub async fn rename_tables(&self, tables: Vec<RenameTableItem>) -> YdbResult<()> {
        self.retry_operation(|| async {
            let mut session = self.create_session().await?;
            let session_id = session.id.clone();
            let operation_params = session.operation_params();
            session
                .in_flight_rpc(async |table| {
                    table
                        .rename_tables(RawRenameTablesRequest {
                            operation_params,
                            session_id,
                            tables: tables.clone().into_iter().map_into().collect(),
                        })
                        .await
                })
                .await
        })
        .await
    }

    pub async fn describe_table(&self, path: String) -> YdbResult<TableDescription> {
        self.retry_operation(|| async {
            let mut session = self.create_session().await?;
            let session_id = session.id.clone();
            let operation_params = session.operation_params();
            let raw = session
                .in_flight_rpc(async |table| {
                    table
                        .describe_table(RawDescribeTableRequest {
                            session_id,
                            path: path.clone(),
                            operation_params,
                        })
                        .await
                })
                .await?;
            table_description_from_raw(raw).map_err(|e| YdbError::custom(e.error))
        })
        .await
    }

    /// Create a table via `CreateTable` RPC.
    pub async fn create_table(&self, request: CreateTableRequest) -> YdbResult<()> {
        self.retry_operation(|| async {
            let mut session = self.create_session().await?;
            let raw = request
                .clone()
                .into_raw(session.id.clone(), session.operation_params())?;
            session
                .in_flight_rpc(async |table| table.create_table(raw).await)
                .await
        })
        .await
    }

    /// Drop a table via `DropTable` RPC.
    pub async fn drop_table(&self, request: DropTableRequest) -> YdbResult<()> {
        self.retry_operation(|| async {
            let mut session = self.create_session().await?;
            let req = RawDropTableRequest {
                session_id: session.id.clone(),
                path: request.path.clone(),
                operation_params: session.operation_params(),
            };
            session
                .in_flight_rpc(async |table| table.drop_table(req).await)
                .await
        })
        .await
    }

    /// Alter a table via `AlterTable` RPC (columns, attributes, etc.).
    pub async fn alter_table(&self, request: AlterTableRequest) -> YdbResult<()> {
        self.retry_operation(|| async {
            let mut session = self.create_session().await?;
            let raw = request
                .clone()
                .into_raw(session.id.clone(), session.operation_params())?;
            session
                .in_flight_rpc(async |table| table.alter_table(raw).await)
                .await
        })
        .await
    }

    /// Describe cluster-wide table option presets.
    pub async fn describe_table_options(&self) -> YdbResult<TableOptionsDescription> {
        self.retry_operation(|| async {
            let mut session = self.create_session().await?;
            let req = RawDescribeTableOptionsRequest {
                operation_params: session.operation_params(),
            };
            let raw: RawDescribeTableOptionsResult = session
                .in_flight_rpc(async |table| table.describe_table_options(req).await)
                .await?;
            Ok(raw.into())
        })
        .await
    }
}
