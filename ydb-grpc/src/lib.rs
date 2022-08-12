//! Crate contains generated low-level grpc code from YDB API protobuf, used as base for ydb crate
//! End customers should use crate ydb.

#[allow(clippy::derive_partial_eq_without_eq)]
mod generated;
pub use generated::ydb as ydb_proto;
