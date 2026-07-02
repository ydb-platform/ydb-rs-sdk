use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;

use crate::errors::YdbResult;
use crate::result::ResultSet;
use crate::table_requests::{
    AlterTableRequest, CreateTableRequest, DropTableRequest, TableOptionsDescription,
};
use crate::table_service_types::{CopyTableItem, RenameTableItem, TableDescription};
use crate::types::Value;

use super::call_options::TableCallOptions;
use super::TableClient;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

macro_rules! impl_table_call_builder {
    ($name:ident) => {
        impl<'a> $name<'a> {
            /// Per-call wall-clock limit (YDB `OperationParams` and retry budget for idempotent ops).
            pub fn timeout(mut self, timeout: Duration) -> Self {
                self.opts.timeout = Some(timeout);
                self
            }
        }
    };
}

pub struct ReadRowsBuilder<'a> {
    pub(crate) client: &'a TableClient,
    pub(crate) table_path: String,
    pub(crate) keys: Vec<Value>,
    pub(crate) columns: Option<Vec<String>>,
    pub(crate) opts: TableCallOptions,
}

impl_table_call_builder!(ReadRowsBuilder);

impl<'a> IntoFuture for ReadRowsBuilder<'a> {
    type Output = YdbResult<ResultSet>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.read_rows_call(
            self.table_path,
            self.keys,
            self.columns,
            self.opts,
        ))
    }
}

pub struct BulkUpsertBuilder<'a> {
    pub(crate) client: &'a TableClient,
    pub(crate) table_path: String,
    pub(crate) rows: Vec<Value>,
    pub(crate) opts: TableCallOptions,
}

impl_table_call_builder!(BulkUpsertBuilder);

impl<'a> IntoFuture for BulkUpsertBuilder<'a> {
    type Output = YdbResult<()>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.bulk_upsert_call(self.table_path, self.rows, self.opts))
    }
}

pub struct CopyTableBuilder<'a> {
    pub(crate) client: &'a TableClient,
    pub(crate) source_path: String,
    pub(crate) destination_path: String,
    pub(crate) opts: TableCallOptions,
}

impl<'a> IntoFuture for CopyTableBuilder<'a> {
    type Output = YdbResult<()>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.copy_table_call(
            self.source_path,
            self.destination_path,
            self.opts,
        ))
    }
}

pub struct CopyTablesBuilder<'a> {
    pub(crate) client: &'a TableClient,
    pub(crate) tables: Vec<CopyTableItem>,
    pub(crate) opts: TableCallOptions,
}
impl<'a> IntoFuture for CopyTablesBuilder<'a> {
    type Output = YdbResult<()>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.copy_tables_call(self.tables, self.opts))
    }
}

pub struct RenameTableBuilder<'a> {
    pub(crate) client: &'a TableClient,
    pub(crate) source_path: String,
    pub(crate) destination_path: String,
    pub(crate) replace_destination: bool,
    pub(crate) opts: TableCallOptions,
}
impl<'a> IntoFuture for RenameTableBuilder<'a> {
    type Output = YdbResult<()>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.rename_table_call(
            self.source_path,
            self.destination_path,
            self.replace_destination,
            self.opts,
        ))
    }
}

pub struct RenameTablesBuilder<'a> {
    pub(crate) client: &'a TableClient,
    pub(crate) tables: Vec<RenameTableItem>,
    pub(crate) opts: TableCallOptions,
}
impl<'a> IntoFuture for RenameTablesBuilder<'a> {
    type Output = YdbResult<()>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.rename_tables_call(self.tables, self.opts))
    }
}

pub struct DescribeTableBuilder<'a> {
    pub(crate) client: &'a TableClient,
    pub(crate) path: String,
    pub(crate) opts: TableCallOptions,
}
impl<'a> IntoFuture for DescribeTableBuilder<'a> {
    type Output = YdbResult<TableDescription>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.describe_table_call(self.path, self.opts))
    }
}

pub struct CreateTableBuilder<'a> {
    pub(crate) client: &'a TableClient,
    pub(crate) request: CreateTableRequest,
    pub(crate) opts: TableCallOptions,
}
impl<'a> IntoFuture for CreateTableBuilder<'a> {
    type Output = YdbResult<()>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.create_table_call(self.request, self.opts))
    }
}

pub struct DropTableBuilder<'a> {
    pub(crate) client: &'a TableClient,
    pub(crate) request: DropTableRequest,
    pub(crate) opts: TableCallOptions,
}
impl<'a> IntoFuture for DropTableBuilder<'a> {
    type Output = YdbResult<()>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.drop_table_call(self.request, self.opts))
    }
}

pub struct AlterTableBuilder<'a> {
    pub(crate) client: &'a TableClient,
    pub(crate) request: AlterTableRequest,
    pub(crate) opts: TableCallOptions,
}
impl<'a> IntoFuture for AlterTableBuilder<'a> {
    type Output = YdbResult<()>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.alter_table_call(self.request, self.opts))
    }
}

pub struct DescribeTableOptionsBuilder<'a> {
    pub(crate) client: &'a TableClient,
    pub(crate) opts: TableCallOptions,
}
impl<'a> IntoFuture for DescribeTableOptionsBuilder<'a> {
    type Output = YdbResult<TableOptionsDescription>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.describe_table_options_call(self.opts))
    }
}

impl_table_call_builder!(CopyTableBuilder);
impl_table_call_builder!(CopyTablesBuilder);
impl_table_call_builder!(RenameTableBuilder);
impl_table_call_builder!(RenameTablesBuilder);
impl_table_call_builder!(DescribeTableBuilder);
impl_table_call_builder!(CreateTableBuilder);
impl_table_call_builder!(DropTableBuilder);
impl_table_call_builder!(AlterTableBuilder);
impl_table_call_builder!(DescribeTableOptionsBuilder);