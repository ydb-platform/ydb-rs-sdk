//! Request builders for Table service DDL and read-table operations.
//!
//! API shape follows [ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk) `table/options`.

use std::collections::HashMap;

use crate::errors::{YdbError, YdbResult};
use crate::grpc_wrapper::raw_table_service::create_table::RawCreateTableColumn;
use crate::grpc_wrapper::raw_table_service::stream_read_table::{
    RawReadTableKeyBound, RawReadTableKeyRange,
};
use crate::types::Value;

/// Column specification for [`CreateTableRequest`] and [`AlterTableRequest`].
#[derive(Clone, Debug)]
pub struct TableColumn {
    pub name: String,
    pub type_example: Value,
    pub not_null: bool,
    pub family: String,
}

impl TableColumn {
    pub fn new(name: impl Into<String>, type_example: Value) -> Self {
        Self {
            name: name.into(),
            type_example,
            not_null: true,
            family: String::new(),
        }
    }

    pub fn with_not_null(mut self, not_null: bool) -> Self {
        self.not_null = not_null;
        self
    }

    pub fn with_family(mut self, family: impl Into<String>) -> Self {
        self.family = family.into();
        self
    }

    pub(crate) fn into_raw(self) -> YdbResult<RawCreateTableColumn> {
        let typed: crate::grpc_wrapper::raw_table_service::value::RawTypedValue =
            self.type_example.try_into().map_err(YdbError::from)?;
        Ok(RawCreateTableColumn {
            name: self.name,
            column_type: typed.r#type,
            not_null: self.not_null,
            family: self.family,
        })
    }
}

/// CreateTable RPC request (go-sdk: `Session.CreateTable`).
#[derive(Clone, Debug, Default)]
pub struct CreateTableRequest {
    pub path: String,
    pub columns: Vec<TableColumn>,
    pub primary_key: Vec<String>,
    pub attributes: HashMap<String, String>,
}

impl CreateTableRequest {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            ..Default::default()
        }
    }

    pub(crate) fn into_raw(
        self,
        session_id: String,
        operation_params: crate::grpc_wrapper::raw_ydb_operation::RawOperationParams,
    ) -> YdbResult<crate::grpc_wrapper::raw_table_service::create_table::RawCreateTableRequest>
    {
        let columns = self
            .columns
            .into_iter()
            .map(|column| column.into_raw())
            .collect::<YdbResult<Vec<_>>>()?;
        Ok(
            crate::grpc_wrapper::raw_table_service::create_table::RawCreateTableRequest {
                session_id,
                path: self.path,
                columns,
                primary_key: self.primary_key,
                attributes: self.attributes,
                operation_params,
            },
        )
    }

    pub fn with_column(mut self, column: TableColumn) -> Self {
        self.columns.push(column);
        self
    }

    pub fn with_primary_key(
        mut self,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.primary_key = columns.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }
}

/// DropTable RPC request.
#[derive(Clone, Debug)]
pub struct DropTableRequest {
    pub path: String,
}

impl DropTableRequest {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }
}

/// ReadRows RPC request (go-sdk: `table.Client.ReadRows` + `options.ReadRowsOption`).
#[derive(Clone, Debug, Default)]
pub struct ReadRowsRequest {
    pub path: String,
    pub keys: Vec<Value>,
    pub columns: Vec<String>,
}

impl ReadRowsRequest {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            ..Default::default()
        }
    }

    pub fn with_keys(mut self, keys: Vec<Value>) -> Self {
        self.keys = keys;
        self
    }

    pub fn with_column(mut self, name: impl Into<String>) -> Self {
        self.columns.push(name.into());
        self
    }

    pub(crate) fn into_raw(
        self,
        session_id: String,
    ) -> YdbResult<crate::grpc_wrapper::raw_table_service::read_rows::RawReadRowsRequest> {
        let keys = crate::types_converters::try_vec_to_list_of_structs(self.keys)?
            .ok_or_else(|| YdbError::Custom("read rows keys must be a list of structs".into()))?;
        Ok(
            crate::grpc_wrapper::raw_table_service::read_rows::RawReadRowsRequest {
                session_id,
                path: self.path,
                keys: keys.try_into().map_err(YdbError::from)?,
                columns: self.columns,
            },
        )
    }
}

/// AlterTable RPC request.
#[derive(Clone, Debug, Default)]
pub struct AlterTableRequest {
    pub path: String,
    pub add_columns: Vec<TableColumn>,
    pub drop_columns: Vec<String>,
    pub alter_columns: Vec<TableColumn>,
    pub alter_attributes: HashMap<String, String>,
}

impl AlterTableRequest {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            ..Default::default()
        }
    }

    pub(crate) fn into_raw(
        self,
        session_id: String,
        operation_params: crate::grpc_wrapper::raw_ydb_operation::RawOperationParams,
    ) -> YdbResult<crate::grpc_wrapper::raw_table_service::alter_table::RawAlterTableRequest> {
        let add_columns = self
            .add_columns
            .into_iter()
            .map(|column| column.into_raw())
            .collect::<YdbResult<Vec<_>>>()?;
        let alter_columns = self
            .alter_columns
            .into_iter()
            .map(|column| column.into_raw())
            .collect::<YdbResult<Vec<_>>>()?;
        Ok(
            crate::grpc_wrapper::raw_table_service::alter_table::RawAlterTableRequest {
                session_id,
                path: self.path,
                add_columns,
                drop_columns: self.drop_columns,
                alter_columns,
                alter_attributes: self.alter_attributes,
                operation_params,
            },
        )
    }

    pub fn add_column(mut self, column: TableColumn) -> Self {
        self.add_columns.push(column);
        self
    }

    pub fn drop_column(mut self, name: impl Into<String>) -> Self {
        self.drop_columns.push(name.into());
        self
    }

    pub fn alter_column(mut self, column: TableColumn) -> Self {
        self.alter_columns.push(column);
        self
    }

    /// Set or update a table attribute. Use an empty value to drop an attribute (server-side rule).
    pub fn alter_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.alter_attributes.insert(key.into(), value.into());
        self
    }
}

/// Primary-key bound for [`ReadTableOptions`].
#[derive(Clone, Debug)]
pub enum ReadTableKeyBound {
    Greater(Value),
    GreaterOrEqual(Value),
    Less(Value),
    LessOrEqual(Value),
}

impl ReadTableKeyBound {
    fn into_raw(self) -> YdbResult<RawReadTableKeyBound> {
        Ok(match self {
            ReadTableKeyBound::Greater(v) => {
                RawReadTableKeyBound::Greater(v.try_into().map_err(YdbError::from)?)
            }
            ReadTableKeyBound::GreaterOrEqual(v) => {
                RawReadTableKeyBound::GreaterOrEqual(v.try_into().map_err(YdbError::from)?)
            }
            ReadTableKeyBound::Less(v) => {
                RawReadTableKeyBound::Less(v.try_into().map_err(YdbError::from)?)
            }
            ReadTableKeyBound::LessOrEqual(v) => {
                RawReadTableKeyBound::LessOrEqual(v.try_into().map_err(YdbError::from)?)
            }
        })
    }
}

/// Primary-key range for [`ReadTableOptions`].
#[derive(Clone, Debug, Default)]
pub struct ReadTableKeyRange {
    pub from: Option<ReadTableKeyBound>,
    pub to: Option<ReadTableKeyBound>,
}

impl ReadTableKeyRange {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_from(mut self, bound: ReadTableKeyBound) -> Self {
        self.from = Some(bound);
        self
    }

    pub fn with_to(mut self, bound: ReadTableKeyBound) -> Self {
        self.to = Some(bound);
        self
    }

    pub(crate) fn into_raw(self) -> YdbResult<RawReadTableKeyRange> {
        let mut raw = RawReadTableKeyRange::default();
        if let Some(from) = self.from {
            let bound = from.into_raw()?;
            raw.from = match bound {
                RawReadTableKeyBound::Greater(v) => RawReadTableKeyBound::Greater(v),
                RawReadTableKeyBound::GreaterOrEqual(v) => RawReadTableKeyBound::GreaterOrEqual(v),
                other => {
                    return Err(YdbError::Custom(format!(
                        "invalid lower bound for read table key range: {other:?}"
                    )));
                }
            };
        }
        if let Some(to) = self.to {
            let bound = to.into_raw()?;
            raw.to = match bound {
                RawReadTableKeyBound::Less(v) => RawReadTableKeyBound::Less(v),
                RawReadTableKeyBound::LessOrEqual(v) => RawReadTableKeyBound::LessOrEqual(v),
                other => {
                    return Err(YdbError::Custom(format!(
                        "invalid upper bound for read table key range: {other:?}"
                    )));
                }
            };
        }
        Ok(raw)
    }
}

/// Options for [`Session::stream_read_table`] (go-sdk: `ReadTableOption`).
#[derive(Clone, Debug, Default)]
pub struct ReadTableOptions {
    pub columns: Vec<String>,
    pub ordered: bool,
    pub row_limit: u64,
    pub key_range: Option<ReadTableKeyRange>,
}

impl ReadTableOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_column(mut self, name: impl Into<String>) -> Self {
        self.columns.push(name.into());
        self
    }

    pub fn with_ordered(mut self, ordered: bool) -> Self {
        self.ordered = ordered;
        self
    }

    pub fn with_row_limit(mut self, limit: u64) -> Self {
        self.row_limit = limit;
        self
    }

    pub fn with_key_range(mut self, range: ReadTableKeyRange) -> Self {
        self.key_range = Some(range);
        self
    }
}

/// Named policy preset from [`TableClient::retry_describe_table_options`].
#[derive(Clone, Debug)]
pub struct NamedPolicyDescription {
    pub name: String,
    pub labels: HashMap<String, String>,
}

/// Cluster-wide table option presets (go-sdk: `options.TableOptionsDescription`).
#[derive(Clone, Debug, Default)]
pub struct TableOptionsDescription {
    pub table_profile_presets: Vec<NamedPolicyDescription>,
    pub storage_policy_presets: Vec<NamedPolicyDescription>,
    pub compaction_policy_presets: Vec<NamedPolicyDescription>,
    pub partitioning_policy_presets: Vec<NamedPolicyDescription>,
    pub execution_policy_presets: Vec<NamedPolicyDescription>,
    pub replication_policy_presets: Vec<NamedPolicyDescription>,
    pub caching_policy_presets: Vec<NamedPolicyDescription>,
}

impl From<crate::grpc_wrapper::raw_table_service::describe_table_options::RawNamedPolicyDescription>
    for NamedPolicyDescription
{
    fn from(
        value: crate::grpc_wrapper::raw_table_service::describe_table_options::RawNamedPolicyDescription,
    ) -> Self {
        Self {
            name: value.name,
            labels: value.labels,
        }
    }
}

impl From<crate::grpc_wrapper::raw_table_service::describe_table_options::RawDescribeTableOptionsResult>
    for TableOptionsDescription
{
    fn from(
        value: crate::grpc_wrapper::raw_table_service::describe_table_options::RawDescribeTableOptionsResult,
    ) -> Self {
        Self {
            table_profile_presets: value.table_profile_presets.into_iter().map_into().collect(),
            storage_policy_presets: value.storage_policy_presets.into_iter().map_into().collect(),
            compaction_policy_presets: value
                .compaction_policy_presets
                .into_iter()
                .map_into()
                .collect(),
            partitioning_policy_presets: value
                .partitioning_policy_presets
                .into_iter()
                .map_into()
                .collect(),
            execution_policy_presets: value
                .execution_policy_presets
                .into_iter()
                .map_into()
                .collect(),
            replication_policy_presets: value
                .replication_policy_presets
                .into_iter()
                .map_into()
                .collect(),
            caching_policy_presets: value.caching_policy_presets.into_iter().map_into().collect(),
        }
    }
}

use itertools::Itertools;
