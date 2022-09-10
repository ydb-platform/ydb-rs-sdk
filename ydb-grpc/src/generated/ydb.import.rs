/// / Common
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ImportProgress {
}
/// Nested message and enum types in `ImportProgress`.
pub mod import_progress {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Progress {
        Unspecified = 0,
        Preparing = 1,
        TransferData = 2,
        BuildIndexes = 3,
        Done = 4,
        Cancellation = 5,
        Cancelled = 6,
    }
    impl Progress {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Progress::Unspecified => "PROGRESS_UNSPECIFIED",
                Progress::Preparing => "PROGRESS_PREPARING",
                Progress::TransferData => "PROGRESS_TRANSFER_DATA",
                Progress::BuildIndexes => "PROGRESS_BUILD_INDEXES",
                Progress::Done => "PROGRESS_DONE",
                Progress::Cancellation => "PROGRESS_CANCELLATION",
                Progress::Cancelled => "PROGRESS_CANCELLED",
            }
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ImportItemProgress {
    #[prost(uint32, tag="1")]
    pub parts_total: u32,
    #[prost(uint32, tag="2")]
    pub parts_completed: u32,
    #[prost(message, optional, tag="3")]
    pub start_time: ::core::option::Option<::pbjson_types::Timestamp>,
    #[prost(message, optional, tag="4")]
    pub end_time: ::core::option::Option<::pbjson_types::Timestamp>,
}
/// / S3
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ImportFromS3Settings {
    #[prost(string, tag="1")]
    pub endpoint: ::prost::alloc::string::String,
    /// HTTPS if not specified
    #[prost(enumeration="import_from_s3_settings::Scheme", tag="2")]
    pub scheme: i32,
    #[prost(string, tag="3")]
    pub bucket: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub access_key: ::prost::alloc::string::String,
    #[prost(string, tag="5")]
    pub secret_key: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="6")]
    pub items: ::prost::alloc::vec::Vec<import_from_s3_settings::Item>,
    #[prost(string, tag="7")]
    pub description: ::prost::alloc::string::String,
    #[prost(uint32, tag="8")]
    pub number_of_retries: u32,
}
/// Nested message and enum types in `ImportFromS3Settings`.
pub mod import_from_s3_settings {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Item {
        /// YDB tables in S3 are stored in one or more objects (see ydb_export.proto).
        /// The object name begins with 'source_prefix'.
        /// This prefix is followed by:
        /// '/data_PartNumber', where 'PartNumber' represents the index of the part, starting at zero;
        /// '/scheme.pb' - object with information about scheme, indexes, etc.
        #[prost(string, tag="1")]
        pub source_prefix: ::prost::alloc::string::String,
        /// Database path to a table to import to.
        #[prost(string, tag="2")]
        pub destination_path: ::prost::alloc::string::String,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Scheme {
        Unspecified = 0,
        Http = 1,
        Https = 2,
    }
    impl Scheme {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Scheme::Unspecified => "UNSPECIFIED",
                Scheme::Http => "HTTP",
                Scheme::Https => "HTTPS",
            }
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ImportFromS3Result {
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ImportFromS3Metadata {
    #[prost(message, optional, tag="1")]
    pub settings: ::core::option::Option<ImportFromS3Settings>,
    #[prost(enumeration="import_progress::Progress", tag="2")]
    pub progress: i32,
    #[prost(message, repeated, tag="3")]
    pub items_progress: ::prost::alloc::vec::Vec<ImportItemProgress>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ImportFromS3Request {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(message, optional, tag="2")]
    pub settings: ::core::option::Option<ImportFromS3Settings>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ImportFromS3Response {
    /// operation.result = ImportFromS3Result
    /// operation.metadata = ImportFromS3Metadata
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// / Data
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct YdbDumpFormat {
    #[prost(string, repeated, tag="1")]
    pub columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ImportDataResult {
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ImportDataRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Full path to table
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
    /// Data serialized in the selected format. Restrictions:
    /// - sorted by primary key;
    /// - all keys must be from the same partition;
    /// - table has no global secondary indexes;
    /// - size of serialized data is limited to 8 MB.
    #[prost(bytes="vec", tag="3")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    #[prost(oneof="import_data_request::Format", tags="4")]
    pub format: ::core::option::Option<import_data_request::Format>,
}
/// Nested message and enum types in `ImportDataRequest`.
pub mod import_data_request {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Format {
        /// Result of `ydb tools dump`
        #[prost(message, tag="4")]
        YdbDump(super::YdbDumpFormat),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ImportDataResponse {
    /// operation.result = ImportDataResult
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}