//! Request builders for Table service DDL and read-table operations.
//!
//! API shape follows [ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk) `table/options`.

use std::collections::HashMap;

use crate::errors::{YdbError, YdbResult};
use crate::grpc_wrapper::raw_table_service::create_table::{RawCreateTableColumn, RawPartitions};
use crate::grpc_wrapper::raw_table_service::partitioning_settings::RawPartitioningSettings;
use crate::grpc_wrapper::raw_table_service::value::r#type::{RawType, TupleType};
use crate::grpc_wrapper::raw_table_service::value::{RawTypedValue, RawValue};
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

/// Auto-partitioning policy and partition-count bounds for a table.
///
/// Every field is optional: an unset field leaves the server default in place
/// on `CreateTable`, and leaves the current value untouched on `AlterTable`.
/// This mirrors the tri-state feature flags used on the wire.
///
/// ```
/// # use ydb::TablePartitioningSettings;
/// let settings = TablePartitioningSettings::new()
///     .with_partitioning_by_size(true)
///     .with_partition_size_mb(256)
///     .with_min_partitions_count(2)
///     .with_max_partitions_count(64);
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TablePartitioningSettings {
    /// Columns the table is partitioned by.
    pub partition_by: Vec<String>,
    /// Split and merge partitions automatically as they cross the size bounds.
    pub partitioning_by_size: Option<bool>,
    /// Preferred partition size in megabytes for auto partitioning by size.
    pub partition_size_mb: Option<u64>,
    /// Split partitions automatically based on their load.
    pub partitioning_by_load: Option<bool>,
    /// Auto-merge stops once the table is down to this many partitions.
    pub min_partitions_count: Option<u64>,
    /// Auto-split stops once the table reaches this many partitions.
    pub max_partitions_count: Option<u64>,
}

impl TablePartitioningSettings {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_partitioning_by(
        mut self,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.partition_by = columns.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_partitioning_by_size(mut self, enabled: bool) -> Self {
        self.partitioning_by_size = Some(enabled);
        self
    }

    pub fn with_partition_size_mb(mut self, partition_size_mb: u64) -> Self {
        self.partition_size_mb = Some(partition_size_mb);
        self
    }

    pub fn with_partitioning_by_load(mut self, enabled: bool) -> Self {
        self.partitioning_by_load = Some(enabled);
        self
    }

    pub fn with_min_partitions_count(mut self, min_partitions_count: u64) -> Self {
        self.min_partitions_count = Some(min_partitions_count);
        self
    }

    pub fn with_max_partitions_count(mut self, max_partitions_count: u64) -> Self {
        self.max_partitions_count = Some(max_partitions_count);
        self
    }

    pub(crate) fn into_raw(self) -> RawPartitioningSettings {
        RawPartitioningSettings {
            partition_by: self.partition_by,
            partitioning_by_size: self.partitioning_by_size,
            partition_size_mb: self.partition_size_mb,
            partitioning_by_load: self.partitioning_by_load,
            min_partitions_count: self.min_partitions_count,
            max_partitions_count: self.max_partitions_count,
        }
    }
}

impl From<RawPartitioningSettings> for TablePartitioningSettings {
    fn from(value: RawPartitioningSettings) -> Self {
        Self {
            partition_by: value.partition_by,
            partitioning_by_size: value.partitioning_by_size,
            partition_size_mb: value.partition_size_mb,
            partitioning_by_load: value.partitioning_by_load,
            min_partitions_count: value.min_partitions_count,
            max_partitions_count: value.max_partitions_count,
        }
    }
}

/// Initial partition layout for a new table.
///
/// Only one layout can be requested; the two variants are mutually exclusive
/// on the wire.
#[derive(Clone, Debug, PartialEq)]
pub enum TablePartitions {
    /// Split the key range into `count` equal parts.
    ///
    /// The leading primary key columns must be `Uint32` or `Uint64`.
    Uniform(u64),
    /// Use the given key prefixes as partition borders, in ascending order.
    ///
    /// Each split point is a prefix of the primary key: one value per leading
    /// key column. The table gets one more partition than there are split
    /// points.
    AtKeys(Vec<Vec<Value>>),
}

/// Encode one split point as the `Tuple<Optional<T>, ...>` YDB expects.
///
/// A bare scalar is rejected by the server ("Partition ranges are not sorted"),
/// because a split point describes a prefix of the primary key rather than a
/// single value. The public [`Value`] has no tuple variant yet (see #309), so
/// the tuple is built directly in the raw layer.
fn split_point_to_typed_value(prefix: Vec<Value>) -> YdbResult<ydb_grpc::ydb_proto::TypedValue> {
    if prefix.is_empty() {
        return Err(YdbError::Custom(
            "split point must contain at least one primary key value".to_string(),
        ));
    }

    let mut elements = Vec::with_capacity(prefix.len());
    let mut items = Vec::with_capacity(prefix.len());
    for value in prefix {
        let raw = RawTypedValue::try_from(value)?;
        elements.push(RawType::Optional(Box::new(raw.r#type)));
        items.push(raw.value);
    }

    Ok(ydb_grpc::ydb_proto::TypedValue::from(RawTypedValue {
        r#type: RawType::Tuple(TupleType { elements }),
        value: RawValue::Items(items),
    }))
}

impl TablePartitions {
    pub(crate) fn into_raw(self) -> YdbResult<RawPartitions> {
        Ok(match self {
            TablePartitions::Uniform(count) => RawPartitions::Uniform(count),
            TablePartitions::AtKeys(split_points) => RawPartitions::AtKeys(
                split_points
                    .into_iter()
                    .map(split_point_to_typed_value)
                    .collect::<YdbResult<Vec<_>>>()?,
            ),
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
    pub partitioning_settings: Option<TablePartitioningSettings>,
    pub partitions: Option<TablePartitions>,
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
                partitioning_settings: self
                    .partitioning_settings
                    .map(TablePartitioningSettings::into_raw),
                partitions: self.partitions.map(TablePartitions::into_raw).transpose()?,
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

    /// Set the auto-partitioning policy for the new table.
    pub fn with_partitioning_settings(mut self, settings: TablePartitioningSettings) -> Self {
        self.partitioning_settings = Some(settings);
        self
    }

    /// Create the table with `count` uniformly split partitions.
    ///
    /// Replaces any previously set partition layout.
    pub fn with_uniform_partitions(mut self, count: u64) -> Self {
        self.partitions = Some(TablePartitions::Uniform(count));
        self
    }

    /// Create the table split at the given primary key values.
    ///
    /// Replaces any previously set partition layout.
    /// Each split point is a prefix of the primary key: one value per leading
    /// key column, given in ascending order.
    ///
    /// ```
    /// # use ydb::{CreateTableRequest, TableColumn, Value};
    /// CreateTableRequest::new("/local/example")
    ///     .with_column(TableColumn::new("id", Value::Uint64(0)))
    ///     .with_primary_key(["id"])
    ///     .with_partition_at_keys([vec![Value::Uint64(100)], vec![Value::Uint64(200)]]);
    /// ```
    pub fn with_partition_at_keys(
        mut self,
        split_points: impl IntoIterator<Item = impl IntoIterator<Item = Value>>,
    ) -> Self {
        self.partitions = Some(TablePartitions::AtKeys(
            split_points
                .into_iter()
                .map(|prefix| prefix.into_iter().collect())
                .collect(),
        ));
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
    pub alter_partitioning_settings: Option<TablePartitioningSettings>,
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
                alter_partitioning_settings: self
                    .alter_partitioning_settings
                    .map(TablePartitioningSettings::into_raw),
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

    /// Set or update a table attribute (go-sdk: `options.WithAlterAttribute`).
    ///
    /// To remove an attribute, use [`Self::drop_attribute`] or pass an empty `value`
    /// (server drops keys with blank values in `alter_attributes`).
    pub fn alter_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.alter_attributes.insert(key.into(), value.into());
        self
    }

    /// Add a table attribute (go-sdk: `options.WithAddAttribute`).
    pub fn add_attribute(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.alter_attribute(key, value)
    }

    /// Replace the auto-partitioning policy
    /// (go-sdk: `options.WithAlterPartitionSettingsObject`).
    ///
    /// Fields left unset keep their current server-side value.
    pub fn alter_partitioning_settings(mut self, settings: TablePartitioningSettings) -> Self {
        self.alter_partitioning_settings = Some(settings);
        self
    }

    /// Drop a table attribute (go-sdk: `options.WithDropAttribute`).
    pub fn drop_attribute(mut self, key: impl Into<String>) -> Self {
        self.alter_attributes.insert(key.into(), String::new());
        self
    }
}

/// Named policy preset from [`TableClient::describe_table_options`].
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

#[cfg(test)]
mod tests {
    use super::AlterTableRequest;

    #[test]
    fn drop_attribute_sets_empty_value_for_server() {
        let req = AlterTableRequest::new("t").drop_attribute("baz");
        assert_eq!(req.alter_attributes.get("baz"), Some(&String::new()));
    }

    #[test]
    fn add_attribute_same_as_alter_attribute() {
        let add = AlterTableRequest::new("t").add_attribute("foo", "bar");
        let alter = AlterTableRequest::new("t").alter_attribute("foo", "bar");
        assert_eq!(add.alter_attributes, alter.alter_attributes);
    }
}
