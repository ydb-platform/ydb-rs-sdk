//! YDB SDK - client for Yandex database.
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
//!  let client = ClientBuilder::from_str("grpc://localhost:2136?database=local")?
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
mod client_builder;
mod credentials;
mod errors;
mod pub_traits;
mod sugar;
mod types;
mod types_converters;
mod channel_pool;
pub(crate) mod client;
pub(crate) mod client_common;
pub(crate) mod client_table;
#[cfg(test)]
mod client_table_test_integration;
pub(crate) mod discovery;
mod grpc;
mod load_balancer;
mod middlewares;
pub(crate) mod query;
pub(crate) mod result;
mod session;
mod session_pool;
mod test_helpers;
mod trait_operation;
pub(crate) mod transaction;
pub(crate) mod waiter;

// full enum pub types
pub use client::Client;
// full enum pub types
pub use client_builder::ClientBuilder;
// full enum pub types
pub use client_table::{RetryOptions, TableClient, TransactionOptions};
// full enum pub types
pub use discovery::{Discovery, DiscoveryState, StaticDiscovery};
// full enum pub types
pub use query::Query;
// full enum pub types
pub use result::{QueryResult, ResultSet, ResultSetRowsIter, Row, StreamResult};
// full enum pub types
pub use transaction::{Mode, Transaction};
// full enum pub types
pub use waiter::Waiter;
// full enum pub types
pub use crate::{
    credentials::{CommandLineYcToken, GCEMetadata, StaticToken, YandexMetadata},
    errors::{
        YdbError, YdbIssue, YdbIssueSeverity, YdbOrCustomerError, YdbResult,
        YdbResultWithCustomerErr, YdbStatusError,
    },
    pub_traits::{Credentials, TokenInfo},
    types::{Bytes, Sign, SignedInterval, Value, ValueList, ValueOptional, ValueStruct},
};
