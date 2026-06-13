use std::convert::TryFrom;

use ydb::{Row, Value, YdbError};

pub type RowID = u64;

#[derive(Debug, Clone)]
pub struct TestRow {
    pub id: RowID,
    pub payload_str: String,
    pub payload_double: f64,
    pub payload_timestamp: std::time::SystemTime,
}

impl TestRow {
    pub fn new(
        id: RowID,
        payload_str: String,
        payload_double: f64,
        payload_timestamp: std::time::SystemTime,
    ) -> Self {
        Self {
            id,
            payload_str,
            payload_double,
            payload_timestamp,
        }
    }
}

/// Map a YDB result row to [`TestRow`].
///
/// Table and Query APIs return column values wrapped in optional types even when
/// the underlying table column is non-nullable, so each field is decoded via
/// `Option<T>` first (same pattern as `examples/basic-select-upsert.rs`).
pub fn test_row_from_row(mut row: Row) -> Result<TestRow, String> {
    let id = required_ydb_field::<u64>(&mut row, "id")?;
    let payload_str = required_ydb_field::<String>(&mut row, "payload_str")?;
    let payload_double = required_ydb_field::<f64>(&mut row, "payload_double")?;
    let payload_timestamp =
        required_ydb_field::<std::time::SystemTime>(&mut row, "payload_timestamp")?;

    Ok(TestRow::new(
        id,
        payload_str,
        payload_double,
        payload_timestamp,
    ))
}

fn optional_ydb_field<T>(row: &mut Row, name: &str) -> Result<Option<T>, String>
where
    T: TryFrom<Value, Error = YdbError>,
    Option<T>: TryFrom<Value, Error = YdbError>,
{
    row.remove_field_by_name(name)
        .map_err(|err: YdbError| err.to_string())?
        .try_into()
        .map_err(|err: YdbError| err.to_string())
}

fn required_ydb_field<T>(row: &mut Row, name: &str) -> Result<T, String>
where
    T: TryFrom<Value, Error = YdbError>,
    Option<T>: TryFrom<Value, Error = YdbError>,
{
    optional_ydb_field(row, name)?
        .ok_or_else(|| format!("{name} is null"))
}
