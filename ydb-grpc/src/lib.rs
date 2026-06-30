//! Crate contains generated low-level grpc code from YDB API protobuf, used as base for ydb crate
//! End customers should use crate ydb.

#[allow(
    warnings,
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo
)]
pub mod generated;
mod manual_workarounds;

pub use generated::google as google_proto_workaround;
pub use generated::ydb as ydb_proto;
