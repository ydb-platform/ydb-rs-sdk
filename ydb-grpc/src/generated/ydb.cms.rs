/// A set of uniform storage units.
/// Single storage unit can be thought of as a reserved part of a RAID.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StorageUnits {
    /// Required. Kind of the storage unit. Determine guarantees
    /// for all main unit parameters: used hard disk type, capacity
    /// throughput, IOPS etc.
    #[prost(string, tag="1")]
    pub unit_kind: ::prost::alloc::string::String,
    /// Required. The number of units in this set.
    #[prost(uint64, tag="2")]
    pub count: u64,
}
/// A set of uniform computational units.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ComputationalUnits {
    /// Required. Kind of the computational unit. Determine main
    /// unit parameters like available memory, CPU, etc.
    #[prost(string, tag="1")]
    pub unit_kind: ::prost::alloc::string::String,
    /// The availability zone all unit should be located in.
    /// By default any availability zone can be used.
    #[prost(string, tag="2")]
    pub availability_zone: ::prost::alloc::string::String,
    /// Required. The number of units in this set.
    #[prost(uint64, tag="3")]
    pub count: u64,
}
/// Computational unit allocated for database. Used to register
/// externally allocated computational resources in CMS.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AllocatedComputationalUnit {
    /// Required. Computational unit host name.
    #[prost(string, tag="1")]
    pub host: ::prost::alloc::string::String,
    /// Required. Computational unit port.
    #[prost(uint32, tag="2")]
    pub port: u32,
    /// Required. Computational unit kind.
    #[prost(string, tag="3")]
    pub unit_kind: ::prost::alloc::string::String,
}
/// A set of computational and storage resources.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Resources {
    /// Storage resources.
    #[prost(message, repeated, tag="1")]
    pub storage_units: ::prost::alloc::vec::Vec<StorageUnits>,
    /// Computational resources.
    #[prost(message, repeated, tag="2")]
    pub computational_units: ::prost::alloc::vec::Vec<ComputationalUnits>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ServerlessResources {
    /// Full path to shared database's home dir whose resources will be used.
    #[prost(string, tag="1")]
    pub shared_database_path: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatabaseOptions {
    /// Do not initialize services required for transactions processing.
    #[prost(bool, tag="1")]
    pub disable_tx_service: bool,
    /// Old-style database, do not create external schemeshard for database
    #[prost(bool, tag="2")]
    pub disable_external_subdomain: bool,
    /// Transaction plan resolution in milliseconds
    #[prost(uint32, tag="3")]
    pub plan_resolution: u32,
}
/// A set of quotas for schema operations
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SchemaOperationQuotas {
    /// Leaky bucket based quotas
    #[prost(message, repeated, tag="1")]
    pub leaky_bucket_quotas: ::prost::alloc::vec::Vec<schema_operation_quotas::LeakyBucket>,
}
/// Nested message and enum types in `SchemaOperationQuotas`.
pub mod schema_operation_quotas {
    /// A single quota based on leaky bucket
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct LeakyBucket {
        /// Bucket size, e.g. <1000> per day
        #[prost(double, tag="1")]
        pub bucket_size: f64,
        /// Bucket duration in seconds, e.g. 1000 per <day>
        #[prost(uint64, tag="2")]
        pub bucket_seconds: u64,
    }
}
/// A set of quotas for the database
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatabaseQuotas {
    /// A maximum data size in bytes, new data will be rejected when exceeded
    #[prost(uint64, tag="1")]
    pub data_size_hard_quota: u64,
    /// An optional size in bytes (lower than data_size_hard_quota). When data
    /// size becomes lower than this value new data ingestion is re-enabled
    /// again. This is useful to help avoid database from rapidly entering and
    /// exiting from the overloaded state.
    #[prost(uint64, tag="2")]
    pub data_size_soft_quota: u64,
    /// A maximum count of shards in all data streams.
    #[prost(uint64, tag="3")]
    pub data_stream_shards_quota: u64,
    /// A maximum storage that will be reserved for all data stream shards.
    #[prost(uint64, tag="5")]
    pub data_stream_reserved_storage_quota: u64,
    /// A minimum value of `TtlSettings.run_interval_seconds` that can be specified.
    /// Default is 1800 (15 minutes).
    #[prost(uint32, tag="4")]
    pub ttl_min_run_internal_seconds: u32,
}
/// Request to create a new database. For successfull creation
/// specified database shouldn't exist. At least one storage
/// unit should be requested for the database.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateDatabaseRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Required. Full path to database's home dir. Used as database ID.
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
    /// Additional database options.
    #[prost(message, optional, tag="4")]
    pub options: ::core::option::Option<DatabaseOptions>,
    /// Attach attributes to database.
    #[prost(map="string, string", tag="5")]
    pub attributes: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    /// Optional quotas for schema operations
    #[prost(message, optional, tag="8")]
    pub schema_operation_quotas: ::core::option::Option<SchemaOperationQuotas>,
    /// Optional idempotency key
    #[prost(string, tag="9")]
    pub idempotency_key: ::prost::alloc::string::String,
    /// Optional quotas for the database
    #[prost(message, optional, tag="10")]
    pub database_quotas: ::core::option::Option<DatabaseQuotas>,
    #[prost(oneof="create_database_request::ResourcesKind", tags="3, 6, 7")]
    pub resources_kind: ::core::option::Option<create_database_request::ResourcesKind>,
}
/// Nested message and enum types in `CreateDatabaseRequest`.
pub mod create_database_request {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ResourcesKind {
        /// Resources to allocate for database by CMS.
        #[prost(message, tag="3")]
        Resources(super::Resources),
        /// Shared resources can be used by serverless databases.
        #[prost(message, tag="6")]
        SharedResources(super::Resources),
        /// If specified, the created database will be "serverless".
        #[prost(message, tag="7")]
        ServerlessResources(super::ServerlessResources),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateDatabaseResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// Get current database status.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetDatabaseStatusRequest {
    /// Required. Full path to database's home dir.
    #[prost(string, tag="1")]
    pub path: ::prost::alloc::string::String,
    /// Operation parameters
    #[prost(message, optional, tag="2")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetDatabaseStatusResponse {
    /// operation.result holds GetDatabaseStatusResult
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetDatabaseStatusResult {
    /// Full path to database's home dir.
    #[prost(string, tag="1")]
    pub path: ::prost::alloc::string::String,
    /// Current database state.
    #[prost(enumeration="get_database_status_result::State", tag="2")]
    pub state: i32,
    /// Database resources allocated by CMS.
    #[prost(message, optional, tag="4")]
    pub allocated_resources: ::core::option::Option<Resources>,
    /// Externally allocated database resources registered in CMS.
    #[prost(message, repeated, tag="5")]
    pub registered_resources: ::prost::alloc::vec::Vec<AllocatedComputationalUnit>,
    /// Current database generation. Incremented at each successful
    /// alter request.
    #[prost(uint64, tag="6")]
    pub generation: u64,
    /// Current quotas for schema operations
    #[prost(message, optional, tag="9")]
    pub schema_operation_quotas: ::core::option::Option<SchemaOperationQuotas>,
    /// Current quotas for the database
    #[prost(message, optional, tag="10")]
    pub database_quotas: ::core::option::Option<DatabaseQuotas>,
    #[prost(oneof="get_database_status_result::ResourcesKind", tags="3, 7, 8")]
    pub resources_kind: ::core::option::Option<get_database_status_result::ResourcesKind>,
}
/// Nested message and enum types in `GetDatabaseStatusResult`.
pub mod get_database_status_result {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum State {
        Unspecified = 0,
        Creating = 1,
        Running = 2,
        Removing = 3,
        PendingResources = 4,
        Configuring = 5,
    }
    impl State {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                State::Unspecified => "STATE_UNSPECIFIED",
                State::Creating => "CREATING",
                State::Running => "RUNNING",
                State::Removing => "REMOVING",
                State::PendingResources => "PENDING_RESOURCES",
                State::Configuring => "CONFIGURING",
            }
        }
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ResourcesKind {
        /// Database resources requested for allocation.
        #[prost(message, tag="3")]
        RequiredResources(super::Resources),
        #[prost(message, tag="7")]
        RequiredSharedResources(super::Resources),
        #[prost(message, tag="8")]
        ServerlessResources(super::ServerlessResources),
    }
}
/// Change resources allocated for database.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterDatabaseRequest {
    /// Required. Full path to database's home dir.
    #[prost(string, tag="1")]
    pub path: ::prost::alloc::string::String,
    /// Additional computational units to allocate for database.
    #[prost(message, repeated, tag="2")]
    pub computational_units_to_add: ::prost::alloc::vec::Vec<ComputationalUnits>,
    /// Computational units to deallocate.
    #[prost(message, repeated, tag="3")]
    pub computational_units_to_remove: ::prost::alloc::vec::Vec<ComputationalUnits>,
    /// Additional storage units to allocate for database.
    #[prost(message, repeated, tag="4")]
    pub storage_units_to_add: ::prost::alloc::vec::Vec<StorageUnits>,
    /// Externally allocated computational units to register for database.
    #[prost(message, repeated, tag="5")]
    pub computational_units_to_register: ::prost::alloc::vec::Vec<AllocatedComputationalUnit>,
    /// Externally allocated computational units to deregister.
    #[prost(message, repeated, tag="6")]
    pub computational_units_to_deregister: ::prost::alloc::vec::Vec<AllocatedComputationalUnit>,
    /// Operation parameters.
    #[prost(message, optional, tag="7")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Current generation of altered database.
    #[prost(uint64, tag="8")]
    pub generation: u64,
    /// Change quotas for schema operations
    #[prost(message, optional, tag="9")]
    pub schema_operation_quotas: ::core::option::Option<SchemaOperationQuotas>,
    /// Optional idempotency key
    #[prost(string, tag="10")]
    pub idempotency_key: ::prost::alloc::string::String,
    /// Change quotas for the database
    #[prost(message, optional, tag="11")]
    pub database_quotas: ::core::option::Option<DatabaseQuotas>,
    /// Alter attributes. Leave the value blank to drop an attribute.
    #[prost(map="string, string", tag="12")]
    pub alter_attributes: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterDatabaseResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// List all databases known by CMS.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDatabasesRequest {
    /// Operation parameters
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDatabasesResponse {
    /// operation.result holds ListDatabasesResult
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDatabasesResult {
    #[prost(string, repeated, tag="1")]
    pub paths: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// Completely remove database and all his data.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveDatabaseRequest {
    /// Required. Full path to database's home dir.
    #[prost(string, tag="1")]
    pub path: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveDatabaseResponse {
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StorageUnitDescription {
    #[prost(string, tag="1")]
    pub kind: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="2")]
    pub labels: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AvailabilityZoneDescription {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="2")]
    pub labels: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ComputationalUnitDescription {
    #[prost(string, tag="1")]
    pub kind: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="2")]
    pub labels: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(string, repeated, tag="3")]
    pub allowed_availability_zones: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeDatabaseOptionsRequest {
    /// Operation parameters
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeDatabaseOptionsResponse {
    /// operation.result holds DescribeDatabaseOptionsResult
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeDatabaseOptionsResult {
    #[prost(message, repeated, tag="1")]
    pub storage_units: ::prost::alloc::vec::Vec<StorageUnitDescription>,
    #[prost(message, repeated, tag="2")]
    pub availability_zones: ::prost::alloc::vec::Vec<AvailabilityZoneDescription>,
    #[prost(message, repeated, tag="3")]
    pub computational_units: ::prost::alloc::vec::Vec<ComputationalUnitDescription>,
}