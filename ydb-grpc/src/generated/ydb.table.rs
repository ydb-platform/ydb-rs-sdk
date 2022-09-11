/// Create new session
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSessionRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
}
/// Create new session
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSessionResponse {
    /// Holds CreateSessionResult in case of CreateSessionResult
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSessionResult {
    /// Session identifier
    #[prost(string, tag="1")]
    pub session_id: ::prost::alloc::string::String,
}
/// Delete session with given id string
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSessionRequest {
    /// Session identifier
    #[prost(string, tag="1")]
    pub session_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSessionResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GlobalIndex {
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GlobalAsyncIndex {
}
/// Represent secondary index
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableIndex {
    /// Name of index
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// list of columns
    #[prost(string, repeated, tag="2")]
    pub index_columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// list of columns content to be copied in to index table
    #[prost(string, repeated, tag="5")]
    pub data_columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Type of index
    #[prost(oneof="table_index::Type", tags="3, 4")]
    pub r#type: ::core::option::Option<table_index::Type>,
}
/// Nested message and enum types in `TableIndex`.
pub mod table_index {
    /// Type of index
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Type {
        #[prost(message, tag="3")]
        GlobalIndex(super::GlobalIndex),
        #[prost(message, tag="4")]
        GlobalAsyncIndex(super::GlobalAsyncIndex),
    }
}
/// Represent secondary index with index state
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableIndexDescription {
    /// Name of index
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// list of columns
    #[prost(string, repeated, tag="2")]
    pub index_columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(enumeration="table_index_description::Status", tag="4")]
    pub status: i32,
    /// list of columns content to be copied in to index table
    #[prost(string, repeated, tag="6")]
    pub data_columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Size of index data in bytes
    #[prost(uint64, tag="7")]
    pub size_bytes: u64,
    /// Type of index
    #[prost(oneof="table_index_description::Type", tags="3, 5")]
    pub r#type: ::core::option::Option<table_index_description::Type>,
}
/// Nested message and enum types in `TableIndexDescription`.
pub mod table_index_description {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Unspecified = 0,
        /// Index is ready to use
        Ready = 1,
        /// index is being built
        Building = 2,
    }
    impl Status {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Status::Unspecified => "STATUS_UNSPECIFIED",
                Status::Ready => "STATUS_READY",
                Status::Building => "STATUS_BUILDING",
            }
        }
    }
    /// Type of index
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Type {
        #[prost(message, tag="3")]
        GlobalIndex(super::GlobalIndex),
        #[prost(message, tag="5")]
        GlobalAsyncIndex(super::GlobalAsyncIndex),
    }
}
/// State of index building operation
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexBuildState {
}
/// Nested message and enum types in `IndexBuildState`.
pub mod index_build_state {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum State {
        Unspecified = 0,
        Preparing = 1,
        TransferingData = 2,
        Applying = 3,
        Done = 4,
        Cancellation = 5,
        Cancelled = 6,
        Rejection = 7,
        Rejected = 8,
    }
    impl State {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                State::Unspecified => "STATE_UNSPECIFIED",
                State::Preparing => "STATE_PREPARING",
                State::TransferingData => "STATE_TRANSFERING_DATA",
                State::Applying => "STATE_APPLYING",
                State::Done => "STATE_DONE",
                State::Cancellation => "STATE_CANCELLATION",
                State::Cancelled => "STATE_CANCELLED",
                State::Rejection => "STATE_REJECTION",
                State::Rejected => "STATE_REJECTED",
            }
        }
    }
}
/// Description of index building operation
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexBuildDescription {
    #[prost(string, tag="1")]
    pub path: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub index: ::core::option::Option<TableIndex>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexBuildMetadata {
    #[prost(message, optional, tag="1")]
    pub description: ::core::option::Option<IndexBuildDescription>,
    #[prost(enumeration="index_build_state::State", tag="2")]
    pub state: i32,
    #[prost(float, tag="3")]
    pub progress: f32,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChangefeedMode {
}
/// Nested message and enum types in `ChangefeedMode`.
pub mod changefeed_mode {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Mode {
        Unspecified = 0,
        /// Only the key component of the modified row
        KeysOnly = 1,
        /// Updated columns
        Updates = 2,
        /// The entire row, as it appears after it was modified
        NewImage = 3,
        /// The entire row, as it appeared before it was modified
        OldImage = 4,
        /// Both new and old images of the row
        NewAndOldImages = 5,
    }
    impl Mode {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Mode::Unspecified => "MODE_UNSPECIFIED",
                Mode::KeysOnly => "MODE_KEYS_ONLY",
                Mode::Updates => "MODE_UPDATES",
                Mode::NewImage => "MODE_NEW_IMAGE",
                Mode::OldImage => "MODE_OLD_IMAGE",
                Mode::NewAndOldImages => "MODE_NEW_AND_OLD_IMAGES",
            }
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChangefeedFormat {
}
/// Nested message and enum types in `ChangefeedFormat`.
pub mod changefeed_format {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Format {
        Unspecified = 0,
        Json = 1,
    }
    impl Format {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Format::Unspecified => "FORMAT_UNSPECIFIED",
                Format::Json => "FORMAT_JSON",
            }
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Changefeed {
    /// Name of the feed
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// Mode specifies the information that will be written to the feed
    #[prost(enumeration="changefeed_mode::Mode", tag="2")]
    pub mode: i32,
    /// Format of the data
    #[prost(enumeration="changefeed_format::Format", tag="3")]
    pub format: i32,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChangefeedDescription {
    /// Name of the feed
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// Mode specifies the information that will be written to the feed
    #[prost(enumeration="changefeed_mode::Mode", tag="2")]
    pub mode: i32,
    /// Format of the data
    #[prost(enumeration="changefeed_format::Format", tag="3")]
    pub format: i32,
    /// State of the feed
    #[prost(enumeration="changefeed_description::State", tag="4")]
    pub state: i32,
}
/// Nested message and enum types in `ChangefeedDescription`.
pub mod changefeed_description {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum State {
        Unspecified = 0,
        Enabled = 1,
        Disabled = 2,
    }
    impl State {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                State::Unspecified => "STATE_UNSPECIFIED",
                State::Enabled => "STATE_ENABLED",
                State::Disabled => "STATE_DISABLED",
            }
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StoragePool {
    #[prost(string, tag="1")]
    pub media: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StoragePolicy {
    #[prost(string, tag="1")]
    pub preset_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub syslog: ::core::option::Option<StoragePool>,
    #[prost(message, optional, tag="3")]
    pub log: ::core::option::Option<StoragePool>,
    #[prost(message, optional, tag="4")]
    pub data: ::core::option::Option<StoragePool>,
    #[prost(message, optional, tag="5")]
    pub external: ::core::option::Option<StoragePool>,
    #[prost(enumeration="super::feature_flag::Status", tag="6")]
    pub keep_in_memory: i32,
    #[prost(message, repeated, tag="7")]
    pub column_families: ::prost::alloc::vec::Vec<ColumnFamilyPolicy>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnFamilyPolicy {
    /// Name of the column family, the name "default" must be used for the
    /// primary column family that contains as least primary key columns
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// Storage settings for the column group (default to values in storage policy)
    #[prost(message, optional, tag="2")]
    pub data: ::core::option::Option<StoragePool>,
    #[prost(message, optional, tag="3")]
    pub external: ::core::option::Option<StoragePool>,
    /// When enabled table data will be kept in memory
    /// WARNING: DO NOT USE
    #[prost(enumeration="super::feature_flag::Status", tag="4")]
    pub keep_in_memory: i32,
    /// Optionally specify whether data should be compressed
    #[prost(enumeration="column_family_policy::Compression", tag="5")]
    pub compression: i32,
}
/// Nested message and enum types in `ColumnFamilyPolicy`.
pub mod column_family_policy {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Compression {
        Unspecified = 0,
        Uncompressed = 1,
        Compressed = 2,
    }
    impl Compression {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Compression::Unspecified => "COMPRESSION_UNSPECIFIED",
                Compression::Uncompressed => "UNCOMPRESSED",
                Compression::Compressed => "COMPRESSED",
            }
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CompactionPolicy {
    #[prost(string, tag="1")]
    pub preset_name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExplicitPartitions {
    /// Specify key values used to split table into partitions.
    /// Each value becomes the first key of a new partition.
    /// Key values should go in ascending order.
    /// Total number of created partitions is number of specified
    /// keys + 1.
    #[prost(message, repeated, tag="1")]
    pub split_points: ::prost::alloc::vec::Vec<super::TypedValue>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionStats {
    /// Approximate number of rows in shard
    #[prost(uint64, tag="1")]
    pub rows_estimate: u64,
    /// Approximate size of shard (bytes)
    #[prost(uint64, tag="2")]
    pub store_size: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableStats {
    /// Stats for each partition
    #[prost(message, repeated, tag="1")]
    pub partition_stats: ::prost::alloc::vec::Vec<PartitionStats>,
    /// Approximate number of rows in table
    #[prost(uint64, tag="2")]
    pub rows_estimate: u64,
    /// Approximate size of table (bytes)
    #[prost(uint64, tag="3")]
    pub store_size: u64,
    /// Number of partitions in table
    #[prost(uint64, tag="4")]
    pub partitions: u64,
    /// Timestamp of table creation
    #[prost(message, optional, tag="5")]
    pub creation_time: ::core::option::Option<::pbjson_types::Timestamp>,
    /// Timestamp of last modification
    #[prost(message, optional, tag="6")]
    pub modification_time: ::core::option::Option<::pbjson_types::Timestamp>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitioningPolicy {
    #[prost(string, tag="1")]
    pub preset_name: ::prost::alloc::string::String,
    #[prost(enumeration="partitioning_policy::AutoPartitioningPolicy", tag="2")]
    pub auto_partitioning: i32,
    #[prost(oneof="partitioning_policy::Partitions", tags="3, 4")]
    pub partitions: ::core::option::Option<partitioning_policy::Partitions>,
}
/// Nested message and enum types in `PartitioningPolicy`.
pub mod partitioning_policy {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum AutoPartitioningPolicy {
        Unspecified = 0,
        Disabled = 1,
        AutoSplit = 2,
        AutoSplitMerge = 3,
    }
    impl AutoPartitioningPolicy {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                AutoPartitioningPolicy::Unspecified => "AUTO_PARTITIONING_POLICY_UNSPECIFIED",
                AutoPartitioningPolicy::Disabled => "DISABLED",
                AutoPartitioningPolicy::AutoSplit => "AUTO_SPLIT",
                AutoPartitioningPolicy::AutoSplitMerge => "AUTO_SPLIT_MERGE",
            }
        }
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Partitions {
        /// Allows to enable uniform sharding using given shards number.
        /// The first components of primary key must have Uint32/Uint64 type.
        #[prost(uint64, tag="3")]
        UniformPartitions(u64),
        /// Explicitly specify key values which are used as borders for
        /// created partitions.
        #[prost(message, tag="4")]
        ExplicitPartitions(super::ExplicitPartitions),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionPolicy {
    #[prost(string, tag="1")]
    pub preset_name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicationPolicy {
    #[prost(string, tag="1")]
    pub preset_name: ::prost::alloc::string::String,
    /// If value is non-zero then it specifies a number of read-only
    /// replicas to create for a table. Zero value means preset
    /// setting usage.
    #[prost(uint32, tag="2")]
    pub replicas_count: u32,
    /// If this feature in enabled then requested number of replicas
    /// will be created in each availability zone.
    #[prost(enumeration="super::feature_flag::Status", tag="3")]
    pub create_per_availability_zone: i32,
    /// If this feature in enabled then read-only replicas can be promoted
    /// to leader.
    #[prost(enumeration="super::feature_flag::Status", tag="4")]
    pub allow_promotion: i32,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CachingPolicy {
    #[prost(string, tag="1")]
    pub preset_name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableProfile {
    #[prost(string, tag="1")]
    pub preset_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub storage_policy: ::core::option::Option<StoragePolicy>,
    #[prost(message, optional, tag="3")]
    pub compaction_policy: ::core::option::Option<CompactionPolicy>,
    #[prost(message, optional, tag="4")]
    pub partitioning_policy: ::core::option::Option<PartitioningPolicy>,
    #[prost(message, optional, tag="5")]
    pub execution_policy: ::core::option::Option<ExecutionPolicy>,
    #[prost(message, optional, tag="6")]
    pub replication_policy: ::core::option::Option<ReplicationPolicy>,
    #[prost(message, optional, tag="7")]
    pub caching_policy: ::core::option::Option<CachingPolicy>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnMeta {
    /// Name of column
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// Type of column
    #[prost(message, optional, tag="2")]
    pub r#type: ::core::option::Option<super::Type>,
    /// Column family name of the column
    #[prost(string, tag="3")]
    pub family: ::prost::alloc::string::String,
}
/// The row will be considered as expired at the moment of time, when the value
/// stored in <column_name> is less than or equal to the current time (in epoch
/// time format), and <expire_after_seconds> has passed since that moment;
/// i.e. the expiration threshold is the value of <column_name> plus <expire_after_seconds>.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DateTypeColumnModeSettings {
    /// The column type must be a date type
    #[prost(string, tag="1")]
    pub column_name: ::prost::alloc::string::String,
    #[prost(uint32, tag="2")]
    pub expire_after_seconds: u32,
}
/// Same as DateTypeColumnModeSettings (above), but useful when type of the
/// value stored in <column_name> is not a date type.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValueSinceUnixEpochModeSettings {
    /// The column type must be one of:
    /// - Uint32
    /// - Uint64
    /// - DyNumber
    #[prost(string, tag="1")]
    pub column_name: ::prost::alloc::string::String,
    /// Interpretation of the value stored in <column_name>
    #[prost(enumeration="value_since_unix_epoch_mode_settings::Unit", tag="2")]
    pub column_unit: i32,
    /// This option is always interpreted as seconds regardless of the
    /// <column_unit> value.
    #[prost(uint32, tag="3")]
    pub expire_after_seconds: u32,
}
/// Nested message and enum types in `ValueSinceUnixEpochModeSettings`.
pub mod value_since_unix_epoch_mode_settings {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Unit {
        Unspecified = 0,
        Seconds = 1,
        Milliseconds = 2,
        Microseconds = 3,
        Nanoseconds = 4,
    }
    impl Unit {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Unit::Unspecified => "UNIT_UNSPECIFIED",
                Unit::Seconds => "UNIT_SECONDS",
                Unit::Milliseconds => "UNIT_MILLISECONDS",
                Unit::Microseconds => "UNIT_MICROSECONDS",
                Unit::Nanoseconds => "UNIT_NANOSECONDS",
            }
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TtlSettings {
    // There is no guarantee that expired row will be deleted immediately upon
    // expiration. There may be a delay between the time a row expires and the
    // time that server deletes the row from the table.

    // Ttl periodically runs background removal operations (BRO) on table's partitions.
    // By default, there is:
    // - no more than one BRO on the table;
    // - BRO is started no more than once an hour on the same partition.
    // Use options below to change that behavior.

    /// How often to run BRO on the same partition.
    /// BRO will not be started more often, but may be started less often.
    #[prost(uint32, tag="3")]
    pub run_interval_seconds: u32,
    #[prost(oneof="ttl_settings::Mode", tags="1, 2")]
    pub mode: ::core::option::Option<ttl_settings::Mode>,
}
/// Nested message and enum types in `TtlSettings`.
pub mod ttl_settings {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Mode {
        #[prost(message, tag="1")]
        DateTypeColumn(super::DateTypeColumnModeSettings),
        #[prost(message, tag="2")]
        ValueSinceUnixEpoch(super::ValueSinceUnixEpochModeSettings),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StorageSettings {
    /// This specifies internal channel 0 commit log storage pool
    /// Fastest available storage recommended, negligible amounts of short-lived data
    #[prost(message, optional, tag="1")]
    pub tablet_commit_log0: ::core::option::Option<StoragePool>,
    /// This specifies internal channel 1 commit log storage pool
    /// Fastest available storage recommended, small amounts of short-lived data
    #[prost(message, optional, tag="2")]
    pub tablet_commit_log1: ::core::option::Option<StoragePool>,
    /// This specifies external blobs storage pool
    #[prost(message, optional, tag="4")]
    pub external: ::core::option::Option<StoragePool>,
    /// Optionally store large values in "external blobs"
    /// WARNING: DO NOT USE
    /// This feature is experimental and should not be used, restrictions apply:
    /// * Table cannot split/merge when this is enabled
    /// * Table cannot be copied or backed up when this is enabled
    /// * This feature cannot be disabled once enabled for a table
    #[prost(enumeration="super::feature_flag::Status", tag="5")]
    pub store_external_blobs: i32,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnFamily {
    /// Name of the column family, the name "default" must be used for the
    /// primary column family that contains at least primary key columns
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// This specifies data storage settings for column family
    #[prost(message, optional, tag="2")]
    pub data: ::core::option::Option<StoragePool>,
    /// Optionally specify how data should be compressed
    #[prost(enumeration="column_family::Compression", tag="3")]
    pub compression: i32,
    /// When enabled table data will be kept in memory
    /// WARNING: DO NOT USE
    #[prost(enumeration="super::feature_flag::Status", tag="4")]
    pub keep_in_memory: i32,
}
/// Nested message and enum types in `ColumnFamily`.
pub mod column_family {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Compression {
        Unspecified = 0,
        None = 1,
        Lz4 = 2,
    }
    impl Compression {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Compression::Unspecified => "COMPRESSION_UNSPECIFIED",
                Compression::None => "COMPRESSION_NONE",
                Compression::Lz4 => "COMPRESSION_LZ4",
            }
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitioningSettings {
    /// Enable auto partitioning on reaching upper or lower partition size bound
    #[prost(enumeration="super::feature_flag::Status", tag="2")]
    pub partitioning_by_size: i32,
    /// Preferred partition size for auto partitioning by size, Mb
    #[prost(uint64, tag="3")]
    pub partition_size_mb: u64,
    /// Enable auto partitioning based on load on each partition
    #[prost(enumeration="super::feature_flag::Status", tag="4")]
    pub partitioning_by_load: i32,
    /// Minimum partitions count auto merge would stop working at
    #[prost(uint64, tag="6")]
    pub min_partitions_count: u64,
    /// Maximum partitions count auto split would stop working at
    #[prost(uint64, tag="7")]
    pub max_partitions_count: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AzReadReplicasSettings {
    /// AZ name
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// Read replicas count in this AZ
    #[prost(uint64, tag="2")]
    pub read_replicas_count: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClusterReplicasSettings {
    /// List of read replicas settings for each AZ
    #[prost(message, repeated, tag="2")]
    pub az_read_replicas_settings: ::prost::alloc::vec::Vec<AzReadReplicasSettings>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadReplicasSettings {
    #[prost(oneof="read_replicas_settings::Settings", tags="1, 2")]
    pub settings: ::core::option::Option<read_replicas_settings::Settings>,
}
/// Nested message and enum types in `ReadReplicasSettings`.
pub mod read_replicas_settings {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Settings {
        /// Set equal read replicas count for every AZ
        #[prost(uint64, tag="1")]
        PerAzReadReplicasCount(u64),
        /// Set total replicas count between all AZs
        #[prost(uint64, tag="2")]
        AnyAzReadReplicasCount(u64),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateTableRequest {
    /// Session identifier
    #[prost(string, tag="1")]
    pub session_id: ::prost::alloc::string::String,
    /// Full path
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
    /// Columns (name, type)
    #[prost(message, repeated, tag="3")]
    pub columns: ::prost::alloc::vec::Vec<ColumnMeta>,
    /// List of columns used as primary key
    #[prost(string, repeated, tag="4")]
    pub primary_key: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Table profile
    #[prost(message, optional, tag="5")]
    pub profile: ::core::option::Option<TableProfile>,
    #[prost(message, optional, tag="6")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// List of secondary indexes
    #[prost(message, repeated, tag="7")]
    pub indexes: ::prost::alloc::vec::Vec<TableIndex>,
    /// Table rows time to live settings
    #[prost(message, optional, tag="8")]
    pub ttl_settings: ::core::option::Option<TtlSettings>,
    /// Storage settings for table
    #[prost(message, optional, tag="9")]
    pub storage_settings: ::core::option::Option<StorageSettings>,
    /// Column families
    #[prost(message, repeated, tag="10")]
    pub column_families: ::prost::alloc::vec::Vec<ColumnFamily>,
    /// Attributes. Total size is limited to 10 KB.
    #[prost(map="string, string", tag="11")]
    pub attributes: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    /// Predefined named set of settings for table compaction ["default", "small_table", "log_table"].
    #[prost(string, tag="12")]
    pub compaction_policy: ::prost::alloc::string::String,
    /// Partitioning settings for table
    #[prost(message, optional, tag="15")]
    pub partitioning_settings: ::core::option::Option<PartitioningSettings>,
    /// Bloom filter by key
    #[prost(enumeration="super::feature_flag::Status", tag="16")]
    pub key_bloom_filter: i32,
    /// Read replicas settings for table
    #[prost(message, optional, tag="17")]
    pub read_replicas_settings: ::core::option::Option<ReadReplicasSettings>,
    /// Either one of the following partitions options can be specified
    #[prost(oneof="create_table_request::Partitions", tags="13, 14")]
    pub partitions: ::core::option::Option<create_table_request::Partitions>,
}
/// Nested message and enum types in `CreateTableRequest`.
pub mod create_table_request {
    /// Either one of the following partitions options can be specified
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Partitions {
        /// Enable uniform partitioning using given partitions count.
        /// The first components of primary key must have Uint32/Uint64 type.
        #[prost(uint64, tag="13")]
        UniformPartitions(u64),
        /// Explicitly specify key values which are used as borders for created partitions.
        #[prost(message, tag="14")]
        PartitionAtKeys(super::ExplicitPartitions),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateTableResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// Drop table with given path
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropTableRequest {
    /// Session identifier
    #[prost(string, tag="1")]
    pub session_id: ::prost::alloc::string::String,
    /// Full path
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
    #[prost(message, optional, tag="4")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropTableResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenameIndexItem {
    /// Index name to rename
    #[prost(string, tag="1")]
    pub source_name: ::prost::alloc::string::String,
    /// Target index name
    #[prost(string, tag="2")]
    pub destination_name: ::prost::alloc::string::String,
    /// Move options
    #[prost(bool, tag="3")]
    pub replace_destination: bool,
}
/// Alter table with given path
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterTableRequest {
    /// Session identifier
    #[prost(string, tag="1")]
    pub session_id: ::prost::alloc::string::String,
    /// Full path
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
    /// Columns (name, type) to add
    #[prost(message, repeated, tag="3")]
    pub add_columns: ::prost::alloc::vec::Vec<ColumnMeta>,
    /// Columns to remove
    #[prost(string, repeated, tag="4")]
    pub drop_columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag="5")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Columns to alter
    #[prost(message, repeated, tag="6")]
    pub alter_columns: ::prost::alloc::vec::Vec<ColumnMeta>,
    /// Add secondary indexes
    #[prost(message, repeated, tag="9")]
    pub add_indexes: ::prost::alloc::vec::Vec<TableIndex>,
    /// Remove secondary indexes
    #[prost(string, repeated, tag="10")]
    pub drop_indexes: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Change table storage settings
    #[prost(message, optional, tag="11")]
    pub alter_storage_settings: ::core::option::Option<StorageSettings>,
    /// Add/alter column families
    #[prost(message, repeated, tag="12")]
    pub add_column_families: ::prost::alloc::vec::Vec<ColumnFamily>,
    #[prost(message, repeated, tag="13")]
    pub alter_column_families: ::prost::alloc::vec::Vec<ColumnFamily>,
    /// Alter attributes. Leave the value blank to drop an attribute.
    /// Cannot be used in combination with other fields (except session_id and path) at the moment.
    #[prost(map="string, string", tag="14")]
    pub alter_attributes: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    /// Set predefined named set of settings for table compaction ["default", "small_table", "log_table"].
    /// Set "default" to use default preset.
    #[prost(string, tag="15")]
    pub set_compaction_policy: ::prost::alloc::string::String,
    /// Change table partitioning settings
    #[prost(message, optional, tag="16")]
    pub alter_partitioning_settings: ::core::option::Option<PartitioningSettings>,
    /// Enable/disable bloom filter by key
    #[prost(enumeration="super::feature_flag::Status", tag="17")]
    pub set_key_bloom_filter: i32,
    /// Set read replicas settings for table
    #[prost(message, optional, tag="18")]
    pub set_read_replicas_settings: ::core::option::Option<ReadReplicasSettings>,
    /// Add change feeds
    #[prost(message, repeated, tag="19")]
    pub add_changefeeds: ::prost::alloc::vec::Vec<Changefeed>,
    /// Remove change feeds (by its names)
    #[prost(string, repeated, tag="20")]
    pub drop_changefeeds: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Rename existed index
    #[prost(message, repeated, tag="21")]
    pub rename_indexes: ::prost::alloc::vec::Vec<RenameIndexItem>,
    /// Setup or remove time to live settings
    #[prost(oneof="alter_table_request::TtlAction", tags="7, 8")]
    pub ttl_action: ::core::option::Option<alter_table_request::TtlAction>,
}
/// Nested message and enum types in `AlterTableRequest`.
pub mod alter_table_request {
    /// Setup or remove time to live settings
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum TtlAction {
        #[prost(message, tag="7")]
        SetTtlSettings(super::TtlSettings),
        #[prost(message, tag="8")]
        DropTtlSettings(::pbjson_types::Empty),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterTableResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// Copy table with given path
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CopyTableRequest {
    /// Session identifier
    #[prost(string, tag="1")]
    pub session_id: ::prost::alloc::string::String,
    /// Copy from path
    #[prost(string, tag="2")]
    pub source_path: ::prost::alloc::string::String,
    /// Copy to path
    #[prost(string, tag="3")]
    pub destination_path: ::prost::alloc::string::String,
    #[prost(message, optional, tag="4")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CopyTableResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CopyTableItem {
    /// Copy from path
    #[prost(string, tag="1")]
    pub source_path: ::prost::alloc::string::String,
    /// Copy to path
    #[prost(string, tag="2")]
    pub destination_path: ::prost::alloc::string::String,
    /// Copy options
    #[prost(bool, tag="3")]
    pub omit_indexes: bool,
}
/// Creates consistent copy of given tables.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CopyTablesRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Session identifier
    #[prost(string, tag="2")]
    pub session_id: ::prost::alloc::string::String,
    /// Source and destination paths which describe copies
    #[prost(message, repeated, tag="3")]
    pub tables: ::prost::alloc::vec::Vec<CopyTableItem>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CopyTablesResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenameTableItem {
    /// Full path
    #[prost(string, tag="1")]
    pub source_path: ::prost::alloc::string::String,
    /// Full path
    #[prost(string, tag="2")]
    pub destination_path: ::prost::alloc::string::String,
    /// Move options
    #[prost(bool, tag="3")]
    pub replace_destination: bool,
}
/// Moves given tables
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenameTablesRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Session identifier
    #[prost(string, tag="2")]
    pub session_id: ::prost::alloc::string::String,
    /// Source and destination paths inside RenameTableItem describe rename actions
    #[prost(message, repeated, tag="3")]
    pub tables: ::prost::alloc::vec::Vec<RenameTableItem>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenameTablesResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// Describe table with given path
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeTableRequest {
    /// Session identifier
    #[prost(string, tag="1")]
    pub session_id: ::prost::alloc::string::String,
    /// Full path
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
    #[prost(message, optional, tag="4")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Includes shard key distribution info
    #[prost(bool, tag="5")]
    pub include_shard_key_bounds: bool,
    /// Includes table statistics
    #[prost(bool, tag="6")]
    pub include_table_stats: bool,
    /// Includes partition statistics (required include_table_statistics)
    #[prost(bool, tag="7")]
    pub include_partition_stats: bool,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeTableResponse {
    /// Holds DescribeTableResult in case of successful call
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeTableResult {
    /// Description of scheme object
    #[prost(message, optional, tag="1")]
    pub self_: ::core::option::Option<super::scheme::Entry>,
    /// List of columns
    #[prost(message, repeated, tag="2")]
    pub columns: ::prost::alloc::vec::Vec<ColumnMeta>,
    /// List of primary key columns
    #[prost(string, repeated, tag="3")]
    pub primary_key: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// List of key ranges for shard
    #[prost(message, repeated, tag="4")]
    pub shard_key_bounds: ::prost::alloc::vec::Vec<super::TypedValue>,
    /// List of indexes
    #[prost(message, repeated, tag="5")]
    pub indexes: ::prost::alloc::vec::Vec<TableIndexDescription>,
    /// Statistics of table
    #[prost(message, optional, tag="6")]
    pub table_stats: ::core::option::Option<TableStats>,
    /// TTL params
    #[prost(message, optional, tag="7")]
    pub ttl_settings: ::core::option::Option<TtlSettings>,
    /// Storage settings for table
    #[prost(message, optional, tag="8")]
    pub storage_settings: ::core::option::Option<StorageSettings>,
    /// Column families
    #[prost(message, repeated, tag="9")]
    pub column_families: ::prost::alloc::vec::Vec<ColumnFamily>,
    /// Attributes
    #[prost(map="string, string", tag="10")]
    pub attributes: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    /// Partitioning settings for table
    #[prost(message, optional, tag="12")]
    pub partitioning_settings: ::core::option::Option<PartitioningSettings>,
    /// Bloom filter by key
    #[prost(enumeration="super::feature_flag::Status", tag="13")]
    pub key_bloom_filter: i32,
    /// Read replicas settings for table
    #[prost(message, optional, tag="14")]
    pub read_replicas_settings: ::core::option::Option<ReadReplicasSettings>,
    /// List of changefeeds
    #[prost(message, repeated, tag="15")]
    pub changefeeds: ::prost::alloc::vec::Vec<ChangefeedDescription>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Query {
    /// Text of query or id prepared query
    #[prost(oneof="query::Query", tags="1, 2")]
    pub query: ::core::option::Option<query::Query>,
}
/// Nested message and enum types in `Query`.
pub mod query {
    /// Text of query or id prepared query
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Query {
        /// SQL program
        #[prost(string, tag="1")]
        YqlText(::prost::alloc::string::String),
        /// Prepared query id
        #[prost(string, tag="2")]
        Id(::prost::alloc::string::String),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SerializableModeSettings {
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OnlineModeSettings {
    #[prost(bool, tag="1")]
    pub allow_inconsistent_reads: bool,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StaleModeSettings {
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionSettings {
    #[prost(oneof="transaction_settings::TxMode", tags="1, 2, 3")]
    pub tx_mode: ::core::option::Option<transaction_settings::TxMode>,
}
/// Nested message and enum types in `TransactionSettings`.
pub mod transaction_settings {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum TxMode {
        #[prost(message, tag="1")]
        SerializableReadWrite(super::SerializableModeSettings),
        #[prost(message, tag="2")]
        OnlineReadOnly(super::OnlineModeSettings),
        #[prost(message, tag="3")]
        StaleReadOnly(super::StaleModeSettings),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionControl {
    #[prost(bool, tag="10")]
    pub commit_tx: bool,
    #[prost(oneof="transaction_control::TxSelector", tags="1, 2")]
    pub tx_selector: ::core::option::Option<transaction_control::TxSelector>,
}
/// Nested message and enum types in `TransactionControl`.
pub mod transaction_control {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum TxSelector {
        #[prost(string, tag="1")]
        TxId(::prost::alloc::string::String),
        #[prost(message, tag="2")]
        BeginTx(super::TransactionSettings),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryCachePolicy {
    #[prost(bool, tag="1")]
    pub keep_in_cache: bool,
}
/// Collect and return query execution stats
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryStatsCollection {
}
/// Nested message and enum types in `QueryStatsCollection`.
pub mod query_stats_collection {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Mode {
        StatsCollectionUnspecified = 0,
        /// Stats collection is disabled
        StatsCollectionNone = 1,
        /// Aggregated stats of reads, updates and deletes per table
        StatsCollectionBasic = 2,
        /// Add execution stats and plan on top of STATS_COLLECTION_BASIC
        StatsCollectionFull = 3,
        /// Detailed execution stats including stats for individual tasks and channels
        StatsCollectionProfile = 4,
    }
    impl Mode {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Mode::StatsCollectionUnspecified => "STATS_COLLECTION_UNSPECIFIED",
                Mode::StatsCollectionNone => "STATS_COLLECTION_NONE",
                Mode::StatsCollectionBasic => "STATS_COLLECTION_BASIC",
                Mode::StatsCollectionFull => "STATS_COLLECTION_FULL",
                Mode::StatsCollectionProfile => "STATS_COLLECTION_PROFILE",
            }
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteDataQueryRequest {
    /// Session identifier
    #[prost(string, tag="1")]
    pub session_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub tx_control: ::core::option::Option<TransactionControl>,
    #[prost(message, optional, tag="3")]
    pub query: ::core::option::Option<Query>,
    /// Map of query parameters (optional)
    #[prost(map="string, message", tag="4")]
    pub parameters: ::std::collections::HashMap<::prost::alloc::string::String, super::TypedValue>,
    #[prost(message, optional, tag="5")]
    pub query_cache_policy: ::core::option::Option<QueryCachePolicy>,
    #[prost(message, optional, tag="6")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(enumeration="query_stats_collection::Mode", tag="7")]
    pub collect_stats: i32,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteDataQueryResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteSchemeQueryRequest {
    /// Session identifier
    #[prost(string, tag="1")]
    pub session_id: ::prost::alloc::string::String,
    /// SQL text
    #[prost(string, tag="2")]
    pub yql_text: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteSchemeQueryResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// Holds transaction id
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionMeta {
    /// Transaction identifier
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
}
/// Holds query id and type of parameters
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryMeta {
    /// Query identifier
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    /// Type of parameters
    #[prost(map="string, message", tag="2")]
    pub parameters_types: ::std::collections::HashMap<::prost::alloc::string::String, super::Type>,
}
/// One QueryResult can contain multiple tables
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteQueryResult {
    /// Result rets (for each table)
    #[prost(message, repeated, tag="1")]
    pub result_sets: ::prost::alloc::vec::Vec<super::ResultSet>,
    /// Transaction metadata
    #[prost(message, optional, tag="2")]
    pub tx_meta: ::core::option::Option<TransactionMeta>,
    /// Query metadata
    #[prost(message, optional, tag="3")]
    pub query_meta: ::core::option::Option<QueryMeta>,
    /// Query execution statistics
    #[prost(message, optional, tag="4")]
    pub query_stats: ::core::option::Option<super::table_stats::QueryStats>,
}
/// Explain data query
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExplainDataQueryRequest {
    /// Session identifier
    #[prost(string, tag="1")]
    pub session_id: ::prost::alloc::string::String,
    /// SQL text to explain
    #[prost(string, tag="2")]
    pub yql_text: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExplainDataQueryResponse {
    /// Holds ExplainQueryResult in case of successful call
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExplainQueryResult {
    #[prost(string, tag="1")]
    pub query_ast: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub query_plan: ::prost::alloc::string::String,
}
/// Prepare given program to execute
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PrepareDataQueryRequest {
    /// Session identifier
    #[prost(string, tag="1")]
    pub session_id: ::prost::alloc::string::String,
    /// SQL text
    #[prost(string, tag="2")]
    pub yql_text: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PrepareDataQueryResponse {
    /// Holds PrepareQueryResult in case of successful call
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PrepareQueryResult {
    /// Query id, used to perform ExecuteDataQuery
    #[prost(string, tag="1")]
    pub query_id: ::prost::alloc::string::String,
    /// Parameters type, used to fill in parameter values
    #[prost(map="string, message", tag="2")]
    pub parameters_types: ::std::collections::HashMap<::prost::alloc::string::String, super::Type>,
}
/// Keep session alive
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeepAliveRequest {
    /// Session identifier
    #[prost(string, tag="1")]
    pub session_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeepAliveResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeepAliveResult {
    #[prost(enumeration="keep_alive_result::SessionStatus", tag="1")]
    pub session_status: i32,
}
/// Nested message and enum types in `KeepAliveResult`.
pub mod keep_alive_result {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum SessionStatus {
        Unspecified = 0,
        Ready = 1,
        Busy = 2,
    }
    impl SessionStatus {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                SessionStatus::Unspecified => "SESSION_STATUS_UNSPECIFIED",
                SessionStatus::Ready => "SESSION_STATUS_READY",
                SessionStatus::Busy => "SESSION_STATUS_BUSY",
            }
        }
    }
}
/// Begin transaction on given session with given settings
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BeginTransactionRequest {
    /// Session identifier
    #[prost(string, tag="1")]
    pub session_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub tx_settings: ::core::option::Option<TransactionSettings>,
    #[prost(message, optional, tag="3")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BeginTransactionResponse {
    /// Holds BeginTransactionResult in case of successful call
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BeginTransactionResult {
    #[prost(message, optional, tag="1")]
    pub tx_meta: ::core::option::Option<TransactionMeta>,
}
/// Commit transaction with given session and tx id
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitTransactionRequest {
    /// Session identifier
    #[prost(string, tag="1")]
    pub session_id: ::prost::alloc::string::String,
    /// Transaction identifier
    #[prost(string, tag="2")]
    pub tx_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(enumeration="query_stats_collection::Mode", tag="4")]
    pub collect_stats: i32,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitTransactionResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitTransactionResult {
    #[prost(message, optional, tag="1")]
    pub query_stats: ::core::option::Option<super::table_stats::QueryStats>,
}
/// Rollback transaction with given session and tx id
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RollbackTransactionRequest {
    /// Session identifier
    #[prost(string, tag="1")]
    pub session_id: ::prost::alloc::string::String,
    /// Transaction identifier
    #[prost(string, tag="2")]
    pub tx_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RollbackTransactionResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StoragePolicyDescription {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="2")]
    pub labels: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CompactionPolicyDescription {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="2")]
    pub labels: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitioningPolicyDescription {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="2")]
    pub labels: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionPolicyDescription {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="2")]
    pub labels: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicationPolicyDescription {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="2")]
    pub labels: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CachingPolicyDescription {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="2")]
    pub labels: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableProfileDescription {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="2")]
    pub labels: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(string, tag="3")]
    pub default_storage_policy: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="4")]
    pub allowed_storage_policies: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag="5")]
    pub default_compaction_policy: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="6")]
    pub allowed_compaction_policies: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag="7")]
    pub default_partitioning_policy: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="8")]
    pub allowed_partitioning_policies: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag="9")]
    pub default_execution_policy: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="10")]
    pub allowed_execution_policies: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag="11")]
    pub default_replication_policy: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="12")]
    pub allowed_replication_policies: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag="13")]
    pub default_caching_policy: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="14")]
    pub allowed_caching_policies: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeTableOptionsRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeTableOptionsResponse {
    /// operation.result holds ListTableParametersResult
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeTableOptionsResult {
    #[prost(message, repeated, tag="1")]
    pub table_profile_presets: ::prost::alloc::vec::Vec<TableProfileDescription>,
    #[prost(message, repeated, tag="2")]
    pub storage_policy_presets: ::prost::alloc::vec::Vec<StoragePolicyDescription>,
    #[prost(message, repeated, tag="3")]
    pub compaction_policy_presets: ::prost::alloc::vec::Vec<CompactionPolicyDescription>,
    #[prost(message, repeated, tag="4")]
    pub partitioning_policy_presets: ::prost::alloc::vec::Vec<PartitioningPolicyDescription>,
    #[prost(message, repeated, tag="5")]
    pub execution_policy_presets: ::prost::alloc::vec::Vec<ExecutionPolicyDescription>,
    #[prost(message, repeated, tag="6")]
    pub replication_policy_presets: ::prost::alloc::vec::Vec<ReplicationPolicyDescription>,
    #[prost(message, repeated, tag="7")]
    pub caching_policy_presets: ::prost::alloc::vec::Vec<CachingPolicyDescription>,
}
// ReadTable request/response

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyRange {
    /// Left border
    #[prost(oneof="key_range::FromBound", tags="1, 2")]
    pub from_bound: ::core::option::Option<key_range::FromBound>,
    /// Right border
    #[prost(oneof="key_range::ToBound", tags="3, 4")]
    pub to_bound: ::core::option::Option<key_range::ToBound>,
}
/// Nested message and enum types in `KeyRange`.
pub mod key_range {
    /// Left border
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum FromBound {
        /// Specify if we don't want to include given key
        #[prost(message, tag="1")]
        Greater(super::super::TypedValue),
        /// Specify if we want to include given key
        #[prost(message, tag="2")]
        GreaterOrEqual(super::super::TypedValue),
    }
    /// Right border
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ToBound {
        /// Specify if we don't want to include given key
        #[prost(message, tag="3")]
        Less(super::super::TypedValue),
        /// Specify if we want to include given key
        #[prost(message, tag="4")]
        LessOrEqual(super::super::TypedValue),
    }
}
/// Request to read table (without SQL)
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadTableRequest {
    /// Session identifier
    #[prost(string, tag="1")]
    pub session_id: ::prost::alloc::string::String,
    /// Path to table to read
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
    /// Primary key range to read
    #[prost(message, optional, tag="3")]
    pub key_range: ::core::option::Option<KeyRange>,
    /// Output columns
    #[prost(string, repeated, tag="4")]
    pub columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Require ordered reading
    #[prost(bool, tag="5")]
    pub ordered: bool,
    /// Limits row count to read
    #[prost(uint64, tag="6")]
    pub row_limit: u64,
    /// Use a server-side snapshot
    #[prost(enumeration="super::feature_flag::Status", tag="7")]
    pub use_snapshot: i32,
}
/// ReadTable doesn't use Operation, returns result directly
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadTableResponse {
    /// Status of request (same as other statuses)
    #[prost(enumeration="super::status_ids::StatusCode", tag="1")]
    pub status: i32,
    /// Issues
    #[prost(message, repeated, tag="2")]
    pub issues: ::prost::alloc::vec::Vec<super::issue::IssueMessage>,
    /// Read table result
    #[prost(message, optional, tag="3")]
    pub result: ::core::option::Option<ReadTableResult>,
}
/// Result of read table request
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadTableResult {
    /// Result set (same as result of sql request)
    #[prost(message, optional, tag="1")]
    pub result_set: ::core::option::Option<super::ResultSet>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BulkUpsertRequest {
    #[prost(string, tag="1")]
    pub table: ::prost::alloc::string::String,
    /// "rows" parameter must be a list of structs where each stuct represents one row.
    /// It must contain all key columns but not necessarily all non-key columns.
    /// Similar to UPSERT statement only values of specified columns will be updated.
    #[prost(message, optional, tag="2")]
    pub rows: ::core::option::Option<super::TypedValue>,
    #[prost(message, optional, tag="3")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// It's last in the definition to help with sidecar patterns
    #[prost(bytes="vec", tag="1000")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    /// You may set data_format + data instead of rows to insert data in serialized formats.
    #[prost(oneof="bulk_upsert_request::DataFormat", tags="7, 8")]
    pub data_format: ::core::option::Option<bulk_upsert_request::DataFormat>,
}
/// Nested message and enum types in `BulkUpsertRequest`.
pub mod bulk_upsert_request {
    /// You may set data_format + data instead of rows to insert data in serialized formats.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum DataFormat {
        #[prost(message, tag="7")]
        ArrowBatchSettings(super::super::formats::ArrowBatchSettings),
        #[prost(message, tag="8")]
        CsvSettings(super::super::formats::CsvSettings),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BulkUpsertResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BulkUpsertResult {
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteScanQueryRequest {
    #[prost(message, optional, tag="3")]
    pub query: ::core::option::Option<Query>,
    #[prost(map="string, message", tag="4")]
    pub parameters: ::std::collections::HashMap<::prost::alloc::string::String, super::TypedValue>,
    #[prost(enumeration="execute_scan_query_request::Mode", tag="6")]
    pub mode: i32,
    #[prost(enumeration="query_stats_collection::Mode", tag="8")]
    pub collect_stats: i32,
}
/// Nested message and enum types in `ExecuteScanQueryRequest`.
pub mod execute_scan_query_request {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Mode {
        Unspecified = 0,
        Explain = 1,
        /// MODE_PREPARE = 2;
        Exec = 3,
    }
    impl Mode {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Mode::Unspecified => "MODE_UNSPECIFIED",
                Mode::Explain => "MODE_EXPLAIN",
                Mode::Exec => "MODE_EXEC",
            }
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteScanQueryPartialResponse {
    #[prost(enumeration="super::status_ids::StatusCode", tag="1")]
    pub status: i32,
    #[prost(message, repeated, tag="2")]
    pub issues: ::prost::alloc::vec::Vec<super::issue::IssueMessage>,
    #[prost(message, optional, tag="3")]
    pub result: ::core::option::Option<ExecuteScanQueryPartialResult>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteScanQueryPartialResult {
    #[prost(message, optional, tag="1")]
    pub result_set: ::core::option::Option<super::ResultSet>,
    #[prost(message, optional, tag="6")]
    pub query_stats: ::core::option::Option<super::table_stats::QueryStats>,
}