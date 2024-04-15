#[cfg(test)]
mod proto_test;

pub(crate) mod proto;
pub(crate) mod r#type;
pub(crate) mod value_ydb;

use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;
use crate::trace_helpers::ensure_len_string;
use std::fmt::{Debug, Formatter};

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub(crate) struct RawTypedValue {
    pub r#type: RawType,
    pub value: RawValue,
}

#[derive(Clone, Debug, PartialEq, strum::EnumCount, serde::Serialize)]
pub(crate) enum RawValue {
    Bool(bool),
    Int32(i32),
    UInt32(u32),
    Int64(i64),
    UInt64(u64),
    HighLow128(u64, u64), // high, low
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
    Text(String),
    NullFlag,
    // NestedValue(Box<Value>), return as Variant with 0 index
    Items(Vec<RawValue>),
    Pairs(Vec<RawValuePair>),
    Variant(Box<RawVariantValue>),
    Decimal(decimal_rs::Decimal),
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub(crate) struct RawValuePair {
    pub(in crate::grpc_wrapper::raw_table_service) key: RawValue,
    pub(in crate::grpc_wrapper::raw_table_service) payload: RawValue,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub(crate) struct RawVariantValue {
    pub(in crate::grpc_wrapper::raw_table_service) value: RawValue,
    pub(in crate::grpc_wrapper::raw_table_service) index: u32,
}

//
// internal to protobuf
//

#[derive(serde::Serialize)]
pub(crate) struct RawResultSet {
    pub columns: Vec<RawColumn>,
    pub rows: Vec<Vec<RawValue>>,
    pub truncated: bool,
}

impl Debug for RawResultSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(s) => f.write_str(ensure_len_string(s).as_str()),
            Err(_) => Err(std::fmt::Error),
        }
    }
}

#[derive(Clone, serde::Serialize)]
pub(crate) struct RawColumn {
    pub name: String,
    pub column_type: RawType,
}
