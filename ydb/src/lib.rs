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
//!  let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
//!     .with_credentials(StaticToken::from("asd"))
//!     .client()?;
//!
//!  // wait until driver background initialization finish
//!  client.wait().await?;
//!
//!  // read query result
//!  let sum: i32 = client
//!     .table_client() // create table client
//!     .retry_transaction(|mut t| async move {
//!         // code in transaction can retry few times if was some retriable error
//!
//!         // send query to database
//!         let res = t.query(Query::from("SELECT 1 + 1 AS sum")).await?;
//!
//!         // read exact one result from db
//!         let field_val: i32 = res.into_only_row()?.remove_field_by_name("sum")?.try_into()?;
//!
//!         // return result
//!         return Ok(field_val);
//!     })
//!     .await?;
//!
//!  // it will print "sum: 2"
//!  println!("sum: {}", sum);
//! #    return Ok(());
//! # }
//! ```
//!
//! # More examples
//! [Url shorneter application](https://github.com/ydb-platform/ydb-rs-sdk/tree/master/ydb-example-urlshortener)
//!
//! [Many small examples](https://github.com/ydb-platform/ydb-rs-sdk/tree/master/ydb/examples)
//!
extern crate core;

pub(crate) mod client;
mod client_builder;
pub(crate) mod client_common;
pub(crate) mod client_coordination;
#[cfg(test)]
mod client_directory_test_integration;
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
mod table_service_types;
#[cfg(test)]
mod test_integration_helper;
#[cfg(test)]
pub(crate) mod topics_test;
mod trace_helpers;
mod trait_operation;
pub(crate) mod transaction;
mod types;
mod types_converters;
pub(crate) mod waiter;

#[cfg(test)]
mod types_test;

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
pub use client_topic::topicreader::messages::{TopicReaderBatch, TopicReaderMessage};
// full enum pub types
pub use client_topic::topicreader::reader::{
    TopicReader, TopicReaderCommitMarker, TopicSelector, TopicSelectors,
};
// full enum pub types
pub use client_topic::topicwriter::message::{TopicWriterMessage, TopicWriterMessageBuilder};
// full enum pub types
pub use client_topic::topicwriter::writer::TopicWriter;
// full enum pub types
pub use client_topic::topicwriter::writer_options::{
    TopicWriterConnectionOptions, TopicWriterOptions, TopicWriterOptionsBuilder,
    TopicWriterRetrySettings,
};
// full enum pub types
pub use client::Client;
// full enum pub types
pub use client_builder::ClientBuilder;
// full enum pub types
pub use client_table::{RetryOptions, TableClient, TransactionOptions};

// full enum pub types
pub use client_scheme::client::SchemeClient;
pub use client_scheme::list_types::{SchemeEntry, SchemeEntryType, SchemePermissions};

// full enum pub types
pub use discovery::{Discovery, DiscoveryState, StaticDiscovery};
// full enum pub types
pub use query::Query;
// full enum pub types
pub use result::{QueryResult, ResultSet, ResultSetRowsIter, Row, StreamResult};
// full enum pub types
pub use transaction::{Mode, Transaction, TransactionInfo};
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
    types::{BulkRows, Bytes, Sign, SignedInterval, Value, ValueList, ValueOptional, ValueStruct},
};

// deprecated types

#[allow(deprecated)]
pub use crate::credentials::{
    CommandLineYcToken, StaticCredentialsAuth, StaticToken, YandexMetadata,
};
