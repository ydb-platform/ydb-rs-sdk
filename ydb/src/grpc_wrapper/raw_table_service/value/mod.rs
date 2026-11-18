#[cfg(test)]
mod proto_test;

pub(crate) mod proto;
pub(crate) mod r#type;
pub(crate) mod value_ydb;

use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;
use crate::traces::helpers::ensure_len_string;
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
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub(crate) struct RawValuePair {
    pub(in crate::grpc_wrapper::raw_table_service) key: RawValue,
    pub(in crate::grpc_wrapper::raw_table_service) payload: RawValue,
}

impl RawValue {
    /// Approximate payload size in bytes, for result-size metrics.
    ///
    /// Fixed-size scalars count by their wire type width; containers sum their
    /// items. The value is a size signal for histograms, not an exact wire
    /// encoding length.
    pub(crate) fn size_bytes(&self) -> usize {
        match self {
            Self::Bool(_) => 1,
            Self::Int32(_) | Self::UInt32(_) | Self::Float(_) => 4,
            Self::Int64(_) | Self::UInt64(_) | Self::Double(_) => 8,
            Self::HighLow128(_, _) => 16,
            Self::Bytes(bytes) => bytes.len(),
            Self::Text(text) => text.len(),
            Self::NullFlag => 0,
            Self::Items(items) => items.iter().map(Self::size_bytes).sum(),
            Self::Pairs(pairs) => pairs
                .iter()
                .map(|pair| pair.key.size_bytes() + pair.payload.size_bytes())
                .sum(),
            Self::Variant(value) => value.value.size_bytes(),
        }
    }
}

impl RawResultSet {
    /// Total row count, for result-size metrics.
    pub(crate) fn rows_count(&self) -> u64 {
        self.rows.len() as u64
    }

    /// Approximate payload size of all rows, for result-size metrics.
    pub(crate) fn result_size_bytes(&self) -> u64 {
        self.rows
            .iter()
            .flat_map(|row| row.iter())
            .map(RawValue::size_bytes)
            .sum::<usize>() as u64
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub(crate) struct RawVariantValue {
    pub(in crate::grpc_wrapper::raw_table_service) value: RawValue,
    pub(in crate::grpc_wrapper::raw_table_service) index: u32,
}

//
// internal to protobuf
//

#[derive(serde::Serialize, Default)]
pub(crate) struct RawResultSet {
    pub columns: Vec<RawColumn>,
    pub rows: Vec<Vec<RawValue>>,
    pub truncated: bool,
}

impl Debug for RawResultSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(s) => f.write_str(&ensure_len_string(s)),
            Err(_) => Err(std::fmt::Error),
        }
    }
}

#[derive(Clone, serde::Serialize)]
pub(crate) struct RawColumn {
    pub name: String,
    pub column_type: RawType,
}

#[cfg(test)]
mod size_tests {
    use super::*;

    #[test]
    fn size_bytes_covers_scalar_and_nested_values() {
        assert_eq!(RawValue::Bool(true).size_bytes(), 1);
        assert_eq!(RawValue::Int32(-1).size_bytes(), 4);
        assert_eq!(RawValue::Int64(-1).size_bytes(), 8);
        assert_eq!(RawValue::HighLow128(1, 2).size_bytes(), 16);
        assert_eq!(RawValue::Text("abcd".to_string()).size_bytes(), 4);
        assert_eq!(RawValue::Bytes(vec![0; 3]).size_bytes(), 3);
        assert_eq!(RawValue::NullFlag.size_bytes(), 0);
        assert_eq!(
            RawValue::Items(vec![RawValue::Int64(1), RawValue::Text("ab".into())]).size_bytes(),
            10
        );
        assert_eq!(
            RawValue::Pairs(vec![
                RawValuePair {
                    key: RawValue::Text("k".into()),
                    payload: RawValue::Int32(1),
                },
                RawValuePair {
                    key: RawValue::Text("".into()),
                    payload: RawValue::Bool(false),
                },
            ])
            .size_bytes(),
            6
        );
    }

    #[test]
    fn result_set_size_totals_rows_and_bytes() {
        let set = RawResultSet {
            columns: vec![],
            rows: vec![
                vec![RawValue::Int64(1), RawValue::Text("abcd".into())],
                vec![RawValue::Int64(2)],
            ],
            truncated: false,
        };
        assert_eq!(set.rows_count(), 2);
        assert_eq!(set.result_size_bytes(), 20);
    }
}
