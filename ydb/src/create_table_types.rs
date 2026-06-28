use crate::errors;
use crate::table_service_types::{IndexType, StoreType};
use crate::types::Value;
use crate::YdbResult;
use derive_builder::Builder;
use std::collections::HashMap;

/// Feature flag used in table creation options (enabled / disabled / unspecified).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub enum FeatureFlag {
    #[default]
    Unspecified,
    Enabled,
    Disabled,
}

/// Storage pool media selector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoragePool {
    pub media: String,
}

impl StoragePool {
    pub fn new(media: impl Into<String>) -> Self {
        Self {
            media: media.into(),
        }
    }
}

/// Compression mode for a column family in [`ColumnFamily`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub enum ColumnFamilyCompression {
    #[default]
    Unspecified,
    None,
    Lz4,
}

/// Compression mode for a column family policy in [`ColumnFamilyPolicy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub enum ColumnFamilyPolicyCompression {
    #[default]
    Unspecified,
    Uncompressed,
    Compressed,
}

/// Column family settings for table creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnFamily {
    pub name: String,
    pub data: Option<StoragePool>,
    pub compression: ColumnFamilyCompression,
    pub keep_in_memory: FeatureFlag,
}

/// Column family policy inside a storage policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnFamilyPolicy {
    pub name: String,
    pub data: Option<StoragePool>,
    pub external: Option<StoragePool>,
    pub keep_in_memory: FeatureFlag,
    pub compression: ColumnFamilyPolicyCompression,
}

/// Storage settings for a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageSettings {
    pub tablet_commit_log0: Option<StoragePool>,
    pub tablet_commit_log1: Option<StoragePool>,
    pub external: Option<StoragePool>,
    pub store_external_blobs: FeatureFlag,
}

/// Storage policy preset and overrides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoragePolicy {
    pub preset_name: String,
    pub syslog: Option<StoragePool>,
    pub log: Option<StoragePool>,
    pub data: Option<StoragePool>,
    pub external: Option<StoragePool>,
    pub keep_in_memory: FeatureFlag,
    pub column_families: Vec<ColumnFamilyPolicy>,
}

/// Compaction policy preset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPolicy {
    pub preset_name: String,
}

/// Auto-partitioning policy for table profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub enum AutoPartitioningPolicy {
    #[default]
    Unspecified,
    Disabled,
    AutoSplit,
    AutoSplitMerge,
}

/// Partitioning policy preset for table profile.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitioningPolicy {
    pub preset_name: String,
    pub auto_partitioning: AutoPartitioningPolicy,
    pub uniform_partitions: Option<u64>,
    pub partition_at_keys: Vec<Value>,
}

/// Execution policy preset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionPolicy {
    pub preset_name: String,
}

/// Replication policy preset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationPolicy {
    pub preset_name: String,
    pub replicas_count: u32,
    pub create_per_availability_zone: FeatureFlag,
    pub allow_promotion: FeatureFlag,
}

/// Caching policy preset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachingPolicy {
    pub preset_name: String,
}

/// Table profile combining storage, compaction, and other presets.
#[derive(Debug, Clone, PartialEq)]
pub struct TableProfile {
    pub preset_name: String,
    pub storage_policy: Option<StoragePolicy>,
    pub compaction_policy: Option<CompactionPolicy>,
    pub partitioning_policy: Option<PartitioningPolicy>,
    pub execution_policy: Option<ExecutionPolicy>,
    pub replication_policy: Option<ReplicationPolicy>,
    pub caching_policy: Option<CachingPolicy>,
}

/// TTL mode based on a date-type column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DateTypeColumnTtl {
    pub column_name: String,
    pub expire_after_seconds: u32,
}

/// Unit for interpreting unix-epoch TTL column values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub enum UnixEpochUnit {
    #[default]
    Unspecified,
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

/// TTL mode based on a unix-epoch numeric column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueSinceUnixEpochTtl {
    pub column_name: String,
    pub column_unit: UnixEpochUnit,
    pub expire_after_seconds: u32,
}

/// TTL mode for table creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TtlMode {
    DateTypeColumn(DateTypeColumnTtl),
    ValueSinceUnixEpoch(ValueSinceUnixEpochTtl),
}

/// Table rows time-to-live settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TtlSettings {
    pub run_interval_seconds: u32,
    pub mode: TtlMode,
}

/// Partitioning settings for auto split/merge on table creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePartitioningSettings {
    pub partition_by: Vec<String>,
    pub partitioning_by_size: FeatureFlag,
    pub partition_size_mb: u64,
    pub partitioning_by_load: FeatureFlag,
    pub min_partitions_count: u64,
    pub max_partitions_count: u64,
}

/// Read replicas configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadReplicasSettings {
    PerAzReadReplicasCount(u64),
    AnyAzReadReplicasCount(u64),
}

/// Initial table partitioning strategy.
#[derive(Debug, Clone, PartialEq)]
pub enum TablePartitions {
    Uniform(u64),
    AtKeys(Vec<Value>),
}

/// Sequence default value options for a column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceOptions {
    pub name: Option<String>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub start_value: Option<i64>,
    pub cache: Option<u64>,
    pub increment: Option<i64>,
    pub cycle: Option<bool>,
}

/// Column default value.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnDefault {
    Literal(Value),
    Sequence(SequenceOptions),
}

/// Column definition for table creation.
#[derive(Debug, Clone, PartialEq)]
pub struct TableColumn {
    pub name: String,
    /// Example value whose type defines the column type.
    pub type_example: Value,
    pub not_null: bool,
    pub family: String,
    pub default_value: Option<ColumnDefault>,
}

impl TableColumn {
    /// Creates a NOT NULL column. `type_example` defines the YDB type (e.g. `Value::Int64(0)`).
    pub fn required(name: impl Into<String>, type_example: Value) -> Self {
        Self {
            name: name.into(),
            type_example,
            not_null: true,
            family: String::new(),
            default_value: None,
        }
    }

    /// Creates a nullable column by wrapping the type in `Optional<…>`.
    pub fn nullable(name: impl Into<String>, type_example: Value) -> YdbResult<Self> {
        let optional_type = Value::optional_from(type_example, None)?;
        Ok(Self {
            name: name.into(),
            type_example: optional_type,
            not_null: false,
            family: String::new(),
            default_value: None,
        })
    }

    pub fn with_family(mut self, family: impl Into<String>) -> Self {
        self.family = family.into();
        self
    }

    pub fn with_default(mut self, default_value: ColumnDefault) -> Self {
        self.default_value = Some(default_value);
        self
    }
}

/// Secondary index definition for table creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableIndex {
    pub name: String,
    pub index_columns: Vec<String>,
    pub data_columns: Vec<String>,
    pub index_type: IndexType,
}

impl CreateTableIndex {
    pub fn global(name: impl Into<String>, index_columns: Vec<String>) -> Self {
        Self::new(name, index_columns, IndexType::Global)
    }

    pub fn global_async(name: impl Into<String>, index_columns: Vec<String>) -> Self {
        Self::new(name, index_columns, IndexType::GlobalAsync)
    }

    pub fn global_unique(name: impl Into<String>, index_columns: Vec<String>) -> Self {
        Self::new(name, index_columns, IndexType::GlobalUnique)
    }

    pub fn new(name: impl Into<String>, index_columns: Vec<String>, index_type: IndexType) -> Self {
        Self {
            name: name.into(),
            index_columns,
            data_columns: Vec::new(),
            index_type,
        }
    }

    pub fn with_data_columns(mut self, data_columns: Vec<String>) -> Self {
        self.data_columns = data_columns;
        self
    }
}

/// Options for [`crate::TableClient::create_table`].
#[derive(Builder, Debug, Clone, PartialEq)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct CreateTableOptions {
    #[builder(setter(into))]
    pub columns: Vec<TableColumn>,
    #[builder(setter(into))]
    pub primary_key: Vec<String>,
    #[builder(default)]
    pub indexes: Vec<CreateTableIndex>,
    #[builder(setter(strip_option), default)]
    pub profile: Option<TableProfile>,
    #[builder(setter(strip_option), default)]
    pub ttl_settings: Option<TtlSettings>,
    #[builder(setter(strip_option), default)]
    pub storage_settings: Option<StorageSettings>,
    #[builder(default)]
    pub column_families: Vec<ColumnFamily>,
    #[builder(default)]
    pub attributes: HashMap<String, String>,
    #[builder(setter(strip_option), default)]
    pub compaction_policy: Option<String>,
    #[builder(setter(strip_option), default)]
    pub partitioning_settings: Option<TablePartitioningSettings>,
    #[builder(setter(strip_option), default)]
    pub partitions: Option<TablePartitions>,
    #[builder(default)]
    pub key_bloom_filter: FeatureFlag,
    #[builder(setter(strip_option), default)]
    pub read_replicas_settings: Option<ReadReplicasSettings>,
    #[builder(setter(strip_option), default)]
    pub tiering: Option<String>,
    #[builder(default)]
    pub temporary: bool,
    #[builder(default = "StoreType::Unspecified")]
    pub store_type: StoreType,
}

impl CreateTableOptions {
    pub fn validate(&self) -> YdbResult<()> {
        if self.columns.is_empty() {
            return Err(errors::YdbError::Custom(
                "create_table: columns must not be empty".into(),
            ));
        }
        if self.primary_key.is_empty() {
            return Err(errors::YdbError::Custom(
                "create_table: primary_key must not be empty".into(),
            ));
        }
        let column_names: std::collections::HashSet<&str> =
            self.columns.iter().map(|c| c.name.as_str()).collect();
        for pk_col in &self.primary_key {
            if !column_names.contains(pk_col.as_str()) {
                return Err(errors::YdbError::Custom(format!(
                    "create_table: primary_key column '{pk_col}' is not defined in columns"
                )));
            }
        }
        Ok(())
    }
}

impl CreateTableOptionsBuilder {
    /// Sets uniform initial partitioning (`uniform_partitions` in the proto).
    pub fn uniform_partitions(&mut self, count: u64) -> &mut Self {
        self.partitions = Some(Some(TablePartitions::Uniform(count)));
        self
    }
}

#[cfg(test)]
#[path = "create_table_types_test.rs"]
mod create_table_types_test;
