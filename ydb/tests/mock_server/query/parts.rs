//! Builders for `ExecuteQueryResponsePart` payloads and result sets shared by
//! the mock-server metric tests.

use ydb_grpc::ydb_proto::query::{ExecuteQueryResponsePart, TransactionMeta};
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::{Column, ResultSet, Type, Value, r#type};

const VALUE_COLUMN: &str = "val";

fn int64_column(name: &str) -> Column {
    Column {
        name: name.to_string(),
        r#type: Some(Type {
            r#type: Some(r#type::Type::TypeId(r#type::PrimitiveTypeId::Int64 as i32)),
        }),
    }
}

fn int64_value(value: i64) -> Value {
    Value {
        items: vec![Value {
            value: Some(ydb_grpc::ydb_proto::value::Value::Int64Value(value)),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Result set with a single `val: Int64` column and one row.
pub fn result_set_with_value(value: i64) -> ResultSet {
    ResultSet {
        columns: vec![int64_column(VALUE_COLUMN)],
        rows: vec![int64_value(value)],
        ..Default::default()
    }
}

/// Result set with a single `val: Int64` column and no rows.
pub fn empty_result_set() -> ResultSet {
    ResultSet {
        columns: vec![int64_column(VALUE_COLUMN)],
        rows: vec![],
        ..Default::default()
    }
}

/// A part carrying one row of the `val: Int64` column.
pub fn row_with_value(value: i64) -> ExecuteQueryResponsePart {
    ExecuteQueryResponsePart {
        status: StatusCode::Success as i32,
        issues: vec![],
        result_set_index: 0,
        result_set: Some(result_set_with_value(value)),
        exec_stats: None,
        tx_meta: None,
    }
}

/// A success part with no result set, optionally carrying the transaction id.
pub fn success_part(tx_id: Option<&str>) -> ExecuteQueryResponsePart {
    ExecuteQueryResponsePart {
        status: StatusCode::Success as i32,
        issues: vec![],
        result_set_index: 0,
        result_set: None,
        exec_stats: None,
        tx_meta: tx_id.map(|id| TransactionMeta { id: id.to_string() }),
    }
}

/// A part carrying an empty result set of the `val: Int64` column.
pub fn empty_row_part() -> ExecuteQueryResponsePart {
    ExecuteQueryResponsePart {
        status: StatusCode::Success as i32,
        issues: vec![],
        result_set_index: 0,
        result_set: Some(empty_result_set()),
        exec_stats: None,
        tx_meta: None,
    }
}

/// A part failed with the given YDB status (BAD_SESSION, ABORTED, ...).
pub fn status_part(status: StatusCode) -> ExecuteQueryResponsePart {
    ExecuteQueryResponsePart {
        status: status as i32,
        issues: vec![],
        result_set_index: 0,
        result_set: None,
        exec_stats: None,
        tx_meta: None,
    }
}
