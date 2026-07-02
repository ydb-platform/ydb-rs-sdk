//! YDB SDK - a client for YDB.
//!
//! # Example
//!
//! ```no_run
//! # use ydb::{ClientBuilder, Query, StaticToken, YdbResult};
//! #
//! # #[tokio::main]
//! # async fn main() -> YdbResult<()> {
//!
//!  // create driver
//!  let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?
//!     .with_credentials(StaticToken::from("asd"))
//!     .client()?;
//!
//!  // wait until driver background initialization finish
//!  client.wait().await?;
//!
//!  // read query result via Query API
//!  let sum: i32 = client
//!     .query_client()
//!     .query_row("SELECT 1 + 1 AS sum", &())
//!     .await?
//!     .ok_or_else(|| ydb::YdbError::custom("no row"))?;
//!
//!  // it will print "sum: 2"
//!  println!("sum: {}", sum);
//! #    return Ok(());
//! # }
//! ```
//!
//! # More examples
//! [Examples](https://github.com/ydb-platform/ydb-rs-sdk/tree/master/ydb/examples)
//!
extern crate core;

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
mod grpc_wrapper;
mod load_balancer;
mod pub_traits;
pub(crate) mod query;
pub(crate) mod result;
mod retry;
mod session;
mod session_pool;
mod sugar;

#[cfg(test)]
pub(crate) mod auth_test;

#[cfg(test)]
pub(crate) mod custom_ca_test;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
pub(crate) mod coordination_test;
pub(crate) mod dicovery_pessimization_interceptor;
mod table_requests;
mod table_service_types;
#[cfg(test)]
mod test_integration_helper;
#[cfg(test)]
pub(crate) mod topics_compression_test;
#[cfg(test)]
pub(crate) mod topics_test;
#[cfg(test)]
pub(crate) mod topics_writer_tx_test;
mod trace_helpers;
mod trait_operation;
mod types;
mod types_converters;
pub(crate) mod waiter;

#[cfg(test)]
mod types_test;

#[cfg(test)]
mod connection_pool_test;

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
    TopicReader, TopicReaderCommitMarker, TopicSelector, TopicSelectors,
};
pub use client_topic::topicreader::reader_options::{
    TopicReaderOptions, TopicReaderOptionsBuilder,
};
// full enum pub types
pub use client_topic::topicwriter::message::{TopicWriterMessage, TopicWriterMessageBuilder};
// full enum pub types
pub use client_topic::topicwriter::partitioning::PartitioningStrategy;
// full enum pub types
pub use client_topic::compression::{CompressionDecoder, CompressionEncoder, Executor};
pub use client_topic::topicwriter::writer::TopicWriter;
pub use client_topic::topicwriter::writer_options::{
    TopicWriterOptions, TopicWriterOptionsBuilder,
};
pub use client_topic::topicwriter::writer_tx::TopicWriterTx;
pub use client_topic::topicwriter::writer_tx_options::{
    TopicWriterTxOptions, TopicWriterTxOptionsBuilder,
};
// full enum pub types
pub use client::{Client, SessionPoolSettings, SessionPoolStats, TimeoutSettings};

// full enum pub types
pub use client_builder::ClientBuilder;

// full enum pub types
pub use client_query::{
    CallBuilder, ClientOneShot, ExecBuilder, ExecCall, ExecuteScriptBuilder,
    ExecuteScriptOperation, FetchScriptResult, FetchScriptResultsBuilder, FromYdbRow, Interactive,
    OneResultSet, OneRow, OptionalRow, OptionalRowBuilder, QueryClient, QueryExecutor,
    QueryRowBuilder, QueryStats, QueryStream, QueryStreamBuilder, ResultSetBuilder, Streamed,
    Transaction, TransactionOptions, TxMode,
};

// full enum pub types
pub use client_table::{RetryOptions, TableClient};

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
