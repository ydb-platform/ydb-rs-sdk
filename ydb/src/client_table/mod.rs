mod builders;
pub(crate) mod call_options;

use crate::RefWithLifetime;
use crate::async_closure::AsyncFnMut;
use crate::retry_settings::{RetrySettings, RetryState};
use crate::session::TableSession;
use crate::session_pool::{SessionPool, TableSessionPool};
use crate::types::Value;
use crate::{closure, errors::*};

use crate::grpc_connection_manager::GrpcConnectionManager;
use tracing::instrument;

use crate::grpc_wrapper::grpc_limits::WithGrpcMaxMessageSize;
use crate::grpc_wrapper::raw_table_service::bulk_upsert::RawBulkUpsertRequest;
use crate::grpc_wrapper::raw_table_service::client::RawTableClient;
use crate::grpc_wrapper::raw_table_service::copy_table::{
    RawCopyTableRequest, RawCopyTablesRequest,
};
use crate::grpc_wrapper::raw_table_service::describe_table::{
    RawDescribeTableRequest, table_description_from_raw,
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
use crate::session::CreateTableClient;
use crate::table_requests::{
    AlterTableRequest, CreateTableRequest, DropTableRequest, ReadRowsRequest,
    TableOptionsDescription,
};
use crate::table_service_types::{CopyTableItem, RenameTableItem, TableDescription};
use crate::types_converters::try_vec_to_list_of_structs;
use itertools::Itertools;
use ydb_grpc::ydb_proto::table::v1::table_service_client::TableServiceClient;

pub use builders::{
    AlterTableBuilder, BulkUpsertBuilder, CopyTableBuilder, CopyTablesBuilder, CreateTableBuilder,
    DescribeTableBuilder, DescribeTableOptionsBuilder, DropTableBuilder, ReadRowsBuilder,
    RenameTableBuilder, RenameTablesBuilder,
};

use call_options::{TableCallOptions, resolve_timeouts};

pub(crate) type TableServiceClientType = TableServiceClient<InterceptedChannel>;

impl WithGrpcMaxMessageSize for TableServiceClientType {
    fn with_grpc_max_message_size(self, bytes: usize) -> Self {
        self.max_decoding_message_size(bytes)
            .max_encoding_message_size(bytes)
    }
}

/// Client for YDB Table service: DDL via RPC (`CreateTable`, …), sessionless data plane (`ReadRows`, `BulkUpsert`), describe.
///
/// Ad-hoc DDL YQL (`CREATE TABLE` / `DROP TABLE` as text) belongs to [`crate::QueryClient::exec`] with [`crate::TxMode::Implicit`].
/// YQL execution, transactions, explain, and streaming reads also belong to [`crate::QueryClient`].
///
/// Per-call timeouts and idempotency are set on operation builders, e.g.
/// `table_client.read_rows(path, keys, None).timeout(Duration::from_secs(1)).idempotent(true).await`.
#[derive(Clone)]
pub struct TableClient {
    session_pool: TableSessionPool,
}

impl TableClient {
    pub(crate) fn new(
        connection_manager: GrpcConnectionManager,
        session_pool: SessionPool,
        retry_settings: RetrySettings,
    ) -> Self {
        Self {
            session_pool: TableSessionPool::from_shared(
                session_pool,
                connection_manager,
                retry_settings,
            ),
        }
    }

    pub(crate) async fn create_session_with_opts(
        &self,
        opts: &TableCallOptions,
    ) -> YdbResult<TableSession> {
        let timeouts = resolve_timeouts(opts);
        Ok(self.session_pool.session().await?.with_timeouts(timeouts))
    }

    async fn sessionless_table_client(&self, opts: &TableCallOptions) -> YdbResult<RawTableClient> {
        self.session_pool
            .connection_manager()
            .create_table_client(resolve_timeouts(opts))
            .await
    }

    async fn bulk_upsert_once(
        &self,
        table_path: String,
        rows: Value,
        opts: &TableCallOptions,
    ) -> YdbResult<()> {
        let raw_rows: crate::grpc_wrapper::raw_table_service::value::RawTypedValue =
            rows.try_into().map_err(YdbError::from)?;
        let mut client = self.sessionless_table_client(opts).await?;
        client
            .bulk_upsert(RawBulkUpsertRequest {
                table: table_path,
                rows: raw_rows.into(),
                operation_params: resolve_timeouts(opts).operation_params(),
            })
            .await
            .map_err(YdbError::from)?;
        Ok(())
    }

    async fn read_rows_once(
        &self,
        request: RawReadRowsRequest,
        opts: &TableCallOptions,
    ) -> YdbResult<crate::ResultSet> {
        let mut client = self.sessionless_table_client(opts).await?;
        let raw_response = client.read_rows(request).await.map_err(YdbError::from)?;
        raw_response.result_set.try_into()
    }

    /// Read rows by primary key without opening a session (go-sdk: `table.Client.ReadRows`).
    ///
    /// `keys` must be a list of [`Value::Struct`] primary-key values.
    /// Returns an empty result set when `keys` is empty.
    pub fn read_rows(
        &self,
        table_path: impl Into<String>,
        keys: Vec<Value>,
        columns: Option<Vec<String>>,
    ) -> ReadRowsBuilder<'_> {
        ReadRowsBuilder {
            client: self,
            table_path: table_path.into(),
            keys,
            columns,
            opts: TableCallOptions::default(),
        }
    }

    async fn retry_table_operation<F, T>(
        &self,
        opts: &TableCallOptions,
        default_idempotency: Idempotency,
        attempt_fn: F,
    ) -> YdbResult<T>
    where
        F: AsyncFnMut<RefWithLifetime<RetryState>, Output = YdbResult<T>>,
    {
        self.session_pool
            .retry_settings()
            .clone()
            .with_deadline(opts.timeout)
            .retry_on_retriable_errors(
                opts.idempotent
                    .map(Idempotency::from)
                    .unwrap_or(default_idempotency),
                attempt_fn,
            )
            .await
    }

    #[instrument(name = "ydb.TableClient.ReadRows", skip_all, fields(db.system.name = "ydb", ydb.table.path = %table_path), err)]
    pub(crate) async fn read_rows_call(
        &self,
        table_path: String,
        keys: Vec<Value>,
        columns: Option<Vec<String>>,
        opts: TableCallOptions,
    ) -> YdbResult<crate::ResultSet> {
        if keys.is_empty() {
            return Ok(crate::ResultSet::default());
        }

        let mut request = ReadRowsRequest::new(table_path).with_keys(keys);
        if let Some(columns) = columns {
            request.columns = columns;
        }
        let raw = request.into_raw(String::new())?;
        self.retry_table_operation(
            &opts,
            Idempotency::Idempotent,
            closure!([&client = self, &opts, &raw], async |_| {
                client.read_rows_once(raw.clone(), opts).await
            }),
        )
        .await
    }

    /// Bulk upsert rows without opening a session (go-sdk: `table.Client.BulkUpsert`).
    pub fn bulk_upsert(
        &self,
        table_path: impl Into<String>,
        rows: Vec<Value>,
    ) -> BulkUpsertBuilder<'_> {
        BulkUpsertBuilder {
            client: self,
            table_path: table_path.into(),
            rows,
            opts: TableCallOptions::default(),
        }
    }

    #[instrument(name = "ydb.TableClient.BulkUpsert", skip_all, fields(db.system.name = "ydb", ydb.table.path = %table_path), err)]
    pub(crate) async fn bulk_upsert_call(
        &self,
        table_path: String,
        rows: Vec<Value>,
        opts: TableCallOptions,
    ) -> YdbResult<()> {
        let Some(value) = try_vec_to_list_of_structs(rows)? else {
            return Ok(());
        };
        self.retry_table_operation(
            &opts,
            Idempotency::Idempotent,
            closure!([&client = self, &table_path, &value, &opts], async |_| {
                client
                    .bulk_upsert_once(table_path.clone(), value.clone(), opts)
                    .await
            }),
        )
        .await
    }

    pub fn copy_table(
        &self,
        source_path: String,
        destination_path: String,
    ) -> CopyTableBuilder<'_> {
        CopyTableBuilder {
            client: self,
            source_path,
            destination_path,
            opts: TableCallOptions::default(),
        }
    }

    #[instrument(name = "ydb.TableClient.CopyTable", skip_all, fields(db.system.name = "ydb", ydb.table.path = %source_path), err)]
    pub(crate) async fn copy_table_call(
        &self,
        source_path: String,
        destination_path: String,
        opts: TableCallOptions,
    ) -> YdbResult<()> {
        self.retry_table_operation(
            &opts,
            Idempotency::NonIdempotent,
            closure!(
                [&client = self, &source_path, &destination_path, &opts],
                async |_| {
                    let mut session = client.create_session_with_opts(opts).await?;
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
                }
            ),
        )
        .await
    }

    pub fn copy_tables(&self, tables: Vec<CopyTableItem>) -> CopyTablesBuilder<'_> {
        CopyTablesBuilder {
            client: self,
            tables,
            opts: TableCallOptions::default(),
        }
    }

    #[instrument(name = "ydb.TableClient.CopyTables", skip_all, fields(db.system.name = "ydb"), err)]
    pub(crate) async fn copy_tables_call(
        &self,
        tables: Vec<CopyTableItem>,
        opts: TableCallOptions,
    ) -> YdbResult<()> {
        self.retry_table_operation(
            &opts,
            Idempotency::NonIdempotent,
            closure!([&client = self, &tables, &opts], async |_| {
                let mut session = client.create_session_with_opts(opts).await?;
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
            }),
        )
        .await
    }

    pub fn rename_table(
        &self,
        source_path: String,
        destination_path: String,
        replace_destination: bool,
    ) -> RenameTableBuilder<'_> {
        RenameTableBuilder {
            client: self,
            source_path,
            destination_path,
            replace_destination,
            opts: TableCallOptions::default(),
        }
    }

    #[instrument(name = "ydb.TableClient.RenameTable", skip_all, fields(db.system.name = "ydb", ydb.table.path = %source_path), err)]
    pub(crate) async fn rename_table_call(
        &self,
        source_path: String,
        destination_path: String,
        replace_destination: bool,
        opts: TableCallOptions,
    ) -> YdbResult<()> {
        self.retry_table_operation(
            &opts,
            Idempotency::NonIdempotent,
            closure!(
                [&client = self, &opts, &source_path, &destination_path],
                async |_| {
                    let mut session = client.create_session_with_opts(opts).await?;
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
                }
            ),
        )
        .await
    }

    pub fn rename_tables(&self, tables: Vec<RenameTableItem>) -> RenameTablesBuilder<'_> {
        RenameTablesBuilder {
            client: self,
            tables,
            opts: TableCallOptions::default(),
        }
    }

    #[instrument(name = "ydb.TableClient.RenameTables", skip_all, fields(db.system.name = "ydb"), err)]
    pub(crate) async fn rename_tables_call(
        &self,
        tables: Vec<RenameTableItem>,
        opts: TableCallOptions,
    ) -> YdbResult<()> {
        self.retry_table_operation(
            &opts,
            Idempotency::NonIdempotent,
            closure!([&client = self, &tables, &opts], async |_| {
                let mut session = client.create_session_with_opts(opts).await?;
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
            }),
        )
        .await
    }

    pub fn describe_table(&self, path: String) -> DescribeTableBuilder<'_> {
        DescribeTableBuilder {
            client: self,
            path,
            opts: TableCallOptions::default(),
        }
    }

    #[instrument(name = "ydb.TableClient.DescribeTable", skip_all, fields(db.system.name = "ydb", ydb.table.path = %path), err)]
    pub(crate) async fn describe_table_call(
        &self,
        path: String,
        opts: TableCallOptions,
    ) -> YdbResult<TableDescription> {
        self.retry_table_operation(
            &opts,
            Idempotency::NonIdempotent,
            closure!([&client = self, &path, &opts], async |_| {
                let mut session = client.create_session_with_opts(opts).await?;
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
            }),
        )
        .await
    }

    /// Create a table via `CreateTable` RPC.
    pub fn create_table(&self, request: CreateTableRequest) -> CreateTableBuilder<'_> {
        CreateTableBuilder {
            client: self,
            request,
            opts: TableCallOptions::default(),
        }
    }

    #[instrument(name = "ydb.TableClient.CreateTable", skip_all, fields(db.system.name = "ydb"), err)]
    pub(crate) async fn create_table_call(
        &self,
        request: CreateTableRequest,
        opts: TableCallOptions,
    ) -> YdbResult<()> {
        self.retry_table_operation(
            &opts,
            Idempotency::NonIdempotent,
            closure!([&client = self, &request, &opts], async |_| {
                let mut session = client.create_session_with_opts(opts).await?;
                let raw = request
                    .clone()
                    .into_raw(session.id.clone(), session.operation_params())?;
                session
                    .in_flight_rpc(async |table| table.create_table(raw).await)
                    .await
            }),
        )
        .await
    }

    /// Drop a table via `DropTable` RPC.
    pub fn drop_table(&self, request: DropTableRequest) -> DropTableBuilder<'_> {
        DropTableBuilder {
            client: self,
            request,
            opts: TableCallOptions::default(),
        }
    }

    #[instrument(name = "ydb.TableClient.DropTable", skip_all, fields(db.system.name = "ydb", ydb.table.path = %request.path), err)]
    pub(crate) async fn drop_table_call(
        &self,
        request: DropTableRequest,
        opts: TableCallOptions,
    ) -> YdbResult<()> {
        self.retry_table_operation(
            &opts,
            Idempotency::NonIdempotent,
            closure!([&client = self, &request, &opts], async |_| {
                let mut session = client.create_session_with_opts(opts).await?;
                let req = RawDropTableRequest {
                    session_id: session.id.clone(),
                    path: request.path.clone(),
                    operation_params: session.operation_params(),
                };
                session
                    .in_flight_rpc(async |table| table.drop_table(req).await)
                    .await
            }),
        )
        .await
    }

    /// Alter a table via `AlterTable` RPC (columns, attributes, etc.).
    pub fn alter_table(&self, request: AlterTableRequest) -> AlterTableBuilder<'_> {
        AlterTableBuilder {
            client: self,
            request,
            opts: TableCallOptions::default(),
        }
    }

    #[instrument(name = "ydb.TableClient.AlterTable", skip_all, fields(db.system.name = "ydb"), err)]
    pub(crate) async fn alter_table_call(
        &self,
        request: AlterTableRequest,
        opts: TableCallOptions,
    ) -> YdbResult<()> {
        self.retry_table_operation(
            &opts,
            Idempotency::NonIdempotent,
            closure!([&client = self, &request, &opts], async |_| {
                let mut session = client.create_session_with_opts(opts).await?;
                let raw = request
                    .clone()
                    .into_raw(session.id.clone(), session.operation_params())?;
                session
                    .in_flight_rpc(async |table| table.alter_table(raw).await)
                    .await
            }),
        )
        .await
    }

    /// Describe cluster-wide table option presets.
    pub fn describe_table_options(&self) -> DescribeTableOptionsBuilder<'_> {
        DescribeTableOptionsBuilder {
            client: self,
            opts: TableCallOptions::default(),
        }
    }

    #[instrument(name = "ydb.TableClient.DescribeTableOptions", skip_all, fields(db.system.name = "ydb"), err)]
    pub(crate) async fn describe_table_options_call(
        &self,
        opts: TableCallOptions,
    ) -> YdbResult<TableOptionsDescription> {
        self.retry_table_operation(
            &opts,
            Idempotency::NonIdempotent,
            closure!([&client = self, &opts], async |_| {
                let mut session = client.create_session_with_opts(opts).await?;
                let req = RawDescribeTableOptionsRequest {
                    operation_params: session.operation_params(),
                };
                let raw: RawDescribeTableOptionsResult = session
                    .in_flight_rpc(async |table| table.describe_table_options(req).await)
                    .await?;
                Ok(raw.into())
            }),
        )
        .await
    }
}
