//! YDB SDK - a client for YDB.
//!
//! # Example
//!
//! ```no_run
//! # use ydb::{AccessTokenCredentials, ClientBuilder, YdbResult};
//! #
//! # #[tokio::main]
//! # async fn main() -> YdbResult<()> {
//!
//!  // create driver
//!  let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?
//!     .with_credentials(AccessTokenCredentials::from("asd"))
//!     .build()
//!     .await?;
//!
//!  // read query result via Query API
//!  let mut row = client
//!     .query_client()
//!     .query_row("SELECT 1 + 1 AS sum")
//!     .await?;
//!  let sum: i32 = row.remove_field_by_name("sum")?.try_into()?;
//!
//!  // it will print "sum: 2"
//!  println!("sum: {}", sum);
//! #    return Ok(());
//! # }
//! ```
//!
//! # Metrics
//!
//! Install a global `metrics` recorder before building the YDB client. The SDK
//! registers driver metrics at client creation and [topic counters](TopicClient#message-counters)
//! as writers, readers and their partition sessions open.
//!
//! The default recorder in `metrics-prometheus 0.11.2` rejects dotted names.
//! Opening a topic reader or writer panics in debug builds and drops the new
//! counters in release builds. To export them, install `metrics-exporter-prometheus`
//! in the application. It replaces dots with underscores in metric and label names.
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let handle = metrics_exporter_prometheus::PrometheusBuilder::new()
//!     .set_buckets(&[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])?
//!     .install_recorder()?;
//! # Ok(())
//! # }
//! ```
//!
//! These buckets preserve the previous recorder's default Query histograms.
//! Serve `handle.render()` alongside any existing Prometheus collectors and
//! call `handle.run_upkeep()` periodically. Applications using the exporter's
//! built-in HTTP listener can use `install()` to run upkeep automatically.
//! Leave counter expiration disabled (the default) while the SDK holds counter handles.
//!
//! To keep using `metrics-prometheus 0.11.2` without exporting the new topic
//! counters, configure its recorder to ignore registration errors. This policy
//! applies to all metrics recorded through it.
//!
//! ```no_run
//! use metrics_prometheus::{Recorder, failure::strategy::NoOp};
//! Recorder::builder().with_failure_strategy(NoOp).build_and_install();
//! ```
//!
//! # More examples
//! [Examples](https://github.com/ydb-platform/ydb-rs-sdk/tree/master/ydb/examples)

extern crate core;

mod byte_units;
pub(crate) mod client;
mod client_builder;
pub(crate) mod client_common;
pub(crate) mod client_coordination;
#[cfg(test)]
mod client_directory_test_integration;
pub(crate) mod client_operation;
pub(crate) mod client_query;
pub(crate) mod client_scheme;
pub(crate) mod client_table;
#[cfg(test)]
mod client_table_test_integration;
pub(crate) mod client_topic;
pub(crate) mod connection_pool;
mod credentials;
pub(crate) mod discovery;
mod errors;
mod grpc;
pub(crate) mod grpc_connection_manager;
mod grpc_options;
mod grpc_wrapper;
mod load_balancer;
mod pub_traits;
pub(crate) mod query;
pub(crate) mod result;
mod retry_settings;
mod session;
mod session_pool;
mod sugar;

#[cfg(test)]
pub(crate) mod auth_test;

#[cfg(test)]
pub(crate) mod custom_ca_test;

#[cfg(test)]
mod test_helpers;

mod async_closure;
#[cfg(test)]
pub(crate) mod coordination_test;
pub(crate) mod discovery_pessimization_interceptor;
mod table_requests;
mod table_service_types;
#[cfg(test)]
mod test_integration_helper;
#[cfg(test)]
pub(crate) mod topic_reader_tx_test;
#[cfg(test)]
pub(crate) mod topics_compression_test;
#[cfg(test)]
pub(crate) mod topics_test;
#[cfg(test)]
pub(crate) mod topics_writer_tx_test;
pub mod traces;
mod trait_operation;
mod types;
mod types_converters;
pub(crate) mod waiter;

#[cfg(test)]
mod types_test;

mod client_metrics;
#[cfg(test)]
mod connection_pool_test;

pub use async_closure::{
    __make_closure, AsyncFnMut, DynAsyncFnMut,
    with_lifetime::{MutWithLifetime, OwnedWithLifetime, RefWithLifetime, WithLifetime},
};

pub use client_coordination::client::CoordinationClient;
pub use client_coordination::list_types::{
    ConsistencyMode, NodeConfig, NodeConfigBuilder, NodeDescription, RateLimiterCountersMode,
};
pub use client_coordination::session::acquire_options::{AcquireOptions, AcquireOptionsBuilder};
pub use client_coordination::session::coordination_session::CoordinationSession;
pub use client_coordination::session::describe_options::{
    DescribeOptions, DescribeOptionsBuilder, WatchMode, WatchOptions, WatchOptionsBuilder,
};
pub use client_coordination::session::lease::Lease;
pub use client_coordination::session::session_options::{SessionOptions, SessionOptionsBuilder};

// full enum pub types
pub use client_topic::client::{
    AlterTopicOptions, AlterTopicOptionsBuilder, CreateTopicOptions, CreateTopicOptionsBuilder,
    DescribeConsumerOptions, DescribeConsumerOptionsBuilder, DescribeTopicOptions,
    DescribeTopicOptionsBuilder, TopicClient,
};
pub use client_topic::list_types::{
    AlterConsumer, AlterConsumerBuilder, Codec, Consumer, ConsumerBuilder, ConsumerDescription,
    MeteringMode, PartitionInfo, PartitionLocation, PartitionStats, PartitioningSettings,
    TopicDescription, TopicStats,
};
// full enum pub types
pub use client_topic::topicreader::messages::{
    PartitionSessionKey, TopicReaderBatch, TopicReaderMessage,
};
// full enum pub types
pub use client_topic::topicreader::reader::{
    TopicReader, TopicReaderCommitMarker, TopicSelector, TopicSelectorBuilder, TopicSelectors,
};
pub use client_topic::topicreader::reader_options::{
    TopicReaderOptions, TopicReaderOptionsBuilder,
};
pub use client_topic::topicreader::reader_tx::TopicReaderTx;
// full enum pub types
pub use client_topic::topicwriter::message::{TopicWriterMessage, TopicWriterMessageBuilder};
// full enum pub types
pub use client_topic::topicwriter::partitioning::PartitioningStrategy;
// full enum pub types
pub use client_topic::compression::{
    CodecSelection, CompressionDecoder, CompressionEncoder, Executor,
};
pub use client_topic::topicwriter::writer::TopicWriter;
pub use client_topic::topicwriter::writer_options::{
    TopicWriterOptions, TopicWriterOptionsBuilder,
};
pub use client_topic::topicwriter::writer_tx::TopicWriterTx;
pub use client_topic::topicwriter::writer_tx_options::{
    TopicWriterTxOptions, TopicWriterTxOptionsBuilder,
};
// full enum pub types
pub use client::{Client, SessionPoolSettings, SessionPoolStats};

pub use retry_settings::{
    DontRetry, ExponentialBackoff, RetriesPerSecond, RetryDeadline, RetryProbability,
    RetrySettings, RetryState, RetryStrategy,
};

// full enum pub types
pub use client_builder::ClientBuilder;
pub use grpc_options::{GrpcOptions, HasGrpcOptions};

// full enum pub types
pub use client_query::{
    CallBuilder, ClientOneShot, ExecBuilder, ExecCall, ExecuteScriptBuilder,
    ExecuteScriptOperation, ExplainQueryBuilder, ExplainResult, FetchScriptResult,
    FetchScriptResultsBuilder, FromYdbRow, Interactive, OneResultSet, OneRow, OptionalRow,
    OptionalRowBuilder, QueryClient, QueryExecutor, QueryRowBuilder, QueryStats, QueryStream,
    QueryStreamBuilder, ResultSetBuilder, RetryTxAttempt, RetryTxBuilder, Streamed, Transaction,
    TransactionOptions, TxMode,
};

// full enum pub types
pub use client_table::TableClient;

// full enum pub types
pub use table_service_types::{
    ColumnDescription, CopyTableItem, IndexDescription, IndexStatus, IndexType, RenameTableItem,
    StoreType, TableDescription, UnknownTypeDescription,
};

// full enum pub types
pub use client_scheme::client::SchemeClient;
pub use client_scheme::list_types::{SchemeEntry, SchemeEntryType, SchemePermissions};

pub use client_operation::{
    ListOperationsRequest, ListOperationsResult, OperationClient, OperationInfo, OperationKind,
};

// full enum pub types
pub use discovery::{Discovery, DiscoveryState, StaticDiscovery};
// full enum pub types
pub use query::Query;
// full enum pub types
pub use result::{ResultSet, ResultSetRowsIter, Row};
pub use table_requests::{
    AlterTableRequest, CreateTableRequest, DropTableRequest, NamedPolicyDescription,
    ReadRowsRequest, TableColumn, TableOptionsDescription,
};
// full enum pub types
pub use waiter::Waiter;
// full enum pub types
pub use crate::{
    credentials::{
        AccessTokenCredentials, AnonymousCredentials, CommandLineCredentials, FromEnvCredentials,
        GCEMetadata, MetadataUrlCredentials, ServiceAccountCredentials, StaticCredentials,
    },
    errors::{
        YdbError, YdbIssue, YdbIssueSeverity, YdbOrCustomerError, YdbResult,
        YdbResultWithCustomerErr, YdbStatusError,
    },
    pub_traits::{Credentials, TokenInfo},
    types::{
        Bytes, Sign, SignedInterval, Value, ValueList, ValueOptional, ValueStruct, YdbDecimal,
    },
};

// deprecated types

#[allow(deprecated)]
pub use crate::credentials::{
    CommandLineYcToken, StaticCredentialsAuth, StaticToken, YandexMetadata,
};
