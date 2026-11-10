use crate::errors;
use crate::errors::YdbError;
use crate::grpc_wrapper::raw_table_service::value::{RawResultSet, RawTypedValue, RawValue};
use crate::types::Value;
use itertools::Itertools;
use std::collections::HashMap;
use std::sync::Arc;
use std::vec::IntoIter;

#[derive(Debug, Default)]
pub struct ResultSet {
    columns: Vec<crate::types::Column>,
    columns_by_name: HashMap<String, usize>,
    raw_result_set: RawResultSet,
}

impl ResultSet {
    #[allow(dead_code)]
    pub(crate) fn columns(&self) -> &[crate::types::Column] {
        &self.columns
    }

    pub fn rows(self) -> ResultSetRowsIter {
        ResultSetRowsIter {
            columns: Arc::new(self.columns),
            columns_by_name: Arc::new(self.columns_by_name),
            row_iter: self.raw_result_set.rows.into_iter(),
        }
    }

    #[allow(dead_code)]
    pub fn is_truncated(&self) -> bool {
        self.raw_result_set.truncated
    }
}

impl TryFrom<RawResultSet> for ResultSet {
    type Error = YdbError;

    fn try_from(value: RawResultSet) -> Result<Self, Self::Error> {
        let columns_by_name: HashMap<String, usize> = value
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| (column.name.clone(), index))
            .collect();
        Ok(Self {
            columns: value
                .columns
                .iter()
                .map(|item| item.clone().try_into())
                .try_collect()?,
            columns_by_name,
            raw_result_set: value,
        })
    }
}

impl IntoIterator for ResultSet {
    type Item = Row;
    type IntoIter = ResultSetRowsIter;

    fn into_iter(self) -> Self::IntoIter {
        self.rows()
    }
}

#[derive(Debug)]
pub struct Row {
    columns: Arc<Vec<crate::types::Column>>,
    columns_by_name: Arc<HashMap<String, usize>>,
    raw_values: HashMap<usize, RawValue>,
}

/// A `ColumnIndex` bound makes the methods of `Sealed` callable outside the SDK, so they require
/// a `Token` that can't be constructed there:
///
/// ```compile_fail
/// fn resolve<I: ydb::ColumnIndex>(index: I, row: &ydb::Row) -> ydb::YdbResult<usize> {
///     index.column_index(row, Default::default())
/// }
/// ```
mod private {
    use super::Row;
    use crate::errors::YdbResult;

    pub struct Token(pub(super) ());

    pub trait Sealed {
        fn column_index(&self, row: &Row, token: Token) -> YdbResult<usize>;
    }
}

/// Selects a column of a [`Row`]: a column name (`&str`) or a zero-based column index (`usize`).
///
/// The trait is sealed and can't be implemented outside the SDK.
pub trait ColumnIndex: private::Sealed {}

impl private::Sealed for usize {
    fn column_index(&self, _row: &Row, _token: private::Token) -> errors::YdbResult<usize> {
        Ok(*self)
    }
}

impl ColumnIndex for usize {}

impl private::Sealed for str {
    fn column_index(&self, row: &Row, _token: private::Token) -> errors::YdbResult<usize> {
        row.columns_by_name
            .get(self)
            .copied()
            .ok_or_else(|| YdbError::Custom(format!("field not found: {self}")))
    }
}

impl ColumnIndex for str {}

impl<T: private::Sealed + ?Sized> private::Sealed for &T {
    fn column_index(&self, row: &Row, token: private::Token) -> errors::YdbResult<usize> {
        (**self).column_index(row, token)
    }
}

impl<T: ColumnIndex + ?Sized> ColumnIndex for &T {}

impl Row {
    /// Takes the value of a column out of the row and converts it to `R`.
    ///
    /// The column is selected by name or by zero-based index. A value can be taken only once:
    /// the next call for the same column returns an error, even if the previous conversion
    /// failed. Taking a column doesn't shift the indexes of other columns.
    ///
    /// `R` is any type convertible from [`Value`], including [`Value`] itself.
    ///
    /// ```no_run
    /// # use ydb::{Row, Value, YdbResult};
    /// # fn example(mut row: Row) -> YdbResult<()> {
    /// let id: i64 = row.take("id")?;
    /// let title: Option<String> = row.take(1)?;
    /// let raw: Value = row.take("payload")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn take<R, I>(&mut self, index: I) -> errors::YdbResult<R>
    where
        I: ColumnIndex,
        R: TryFrom<Value>,
        YdbError: From<R::Error>,
    {
        let index = index.column_index(self, private::Token(()))?;
        let column = self
            .columns
            .get(index)
            .ok_or_else(|| YdbError::Custom(format!("field index {index} is out of range")))?;
        let raw_value = self.raw_values.remove(&index).ok_or_else(|| {
            YdbError::Custom(format!(
                "field {index} is missing or has already been taken"
            ))
        })?;
        let value = Value::try_from(RawTypedValue {
            r#type: column.v_type.clone(),
            value: raw_value,
        })?;
        Ok(R::try_from(value)?)
    }

    #[deprecated(note = "use Row::take instead")]
    pub fn remove_field_by_name(&mut self, name: &str) -> errors::YdbResult<Value> {
        self.take(name)
    }

    #[deprecated(note = "use Row::take instead")]
    pub fn remove_field(&mut self, index: usize) -> errors::YdbResult<Value> {
        self.take(index)
    }
}

pub struct ResultSetRowsIter {
    columns: Arc<Vec<crate::types::Column>>,
    columns_by_name: Arc<HashMap<String, usize>>,
    row_iter: IntoIter<Vec<RawValue>>,
}

impl Iterator for ResultSetRowsIter {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        match self.row_iter.next() {
            None => None,
            Some(row) => Some(Row {
                columns: self.columns.clone(),
                columns_by_name: self.columns_by_name.clone(),
                raw_values: row.into_iter().enumerate().collect(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc_wrapper::raw_table_service::value::RawColumn;
    use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;

    fn test_row() -> Row {
        let result_set: ResultSet = RawResultSet {
            columns: vec![
                RawColumn {
                    name: "id".to_string(),
                    column_type: RawType::Int64,
                },
                RawColumn {
                    name: "title".to_string(),
                    column_type: RawType::UTF8,
                },
                RawColumn {
                    name: "comment".to_string(),
                    column_type: RawType::Optional(Box::new(RawType::UTF8)),
                },
            ],
            rows: vec![vec![
                RawValue::Int64(42),
                RawValue::Text("hello".to_string()),
                RawValue::NullFlag,
            ]],
            truncated: false,
        }
        .try_into()
        .expect("valid result set");
        result_set.into_iter().next().expect("one row")
    }

    #[test]
    fn take_by_name_converts_to_native_type() {
        let mut row = test_row();
        let id: i64 = row.take("id").unwrap();
        let title: String = row.take("title").unwrap();
        let comment: Option<String> = row.take("comment").unwrap();
        assert_eq!(id, 42);
        assert_eq!(title, "hello");
        assert_eq!(comment, None);
    }

    #[test]
    fn take_by_index_converts_to_native_type() {
        let mut row = test_row();
        let title: String = row.take(1).unwrap();
        let id = row.take::<i64, _>(0usize).unwrap();
        assert_eq!(title, "hello");
        assert_eq!(id, 42);
    }

    #[test]
    fn take_returns_value_without_conversion() {
        let mut row = test_row();
        let id: Value = row.take("id").unwrap();
        assert_eq!(id, Value::Int64(42));
    }

    #[test]
    fn take_accepts_name_borrowed_from_string() {
        let mut row = test_row();
        let name = String::from("id");
        let id: i64 = row.take(name.as_str()).unwrap();
        assert_eq!(id, 42);
        let name = String::from("title");
        let title: String = row.take(&*name).unwrap();
        assert_eq!(title, "hello");
    }

    #[test]
    fn take_same_column_twice_fails() {
        let mut row = test_row();
        let _: i64 = row.take("id").unwrap();
        let err = row.take::<i64, _>("id").unwrap_err();
        assert!(err.to_string().contains("already been taken"), "{err}");
        let err = row.take::<i64, _>(0).unwrap_err();
        assert!(err.to_string().contains("already been taken"), "{err}");
    }

    #[test]
    fn take_keeps_indexes_of_other_columns() {
        let mut row = test_row();
        let _: i64 = row.take(0).unwrap();
        let title: String = row.take(1).unwrap();
        assert_eq!(title, "hello");
    }

    #[test]
    fn take_unknown_column_fails() {
        let mut row = test_row();
        let err = row.take::<Value, _>("unknown_column").unwrap_err();
        assert!(
            err.to_string().contains("field not found: unknown_column"),
            "{err}"
        );
        let err = row.take::<Value, _>(3).unwrap_err();
        assert!(err.to_string().contains("out of range"), "{err}");
    }

    #[test]
    fn take_with_wrong_type_fails_and_consumes_value() {
        let mut row = test_row();
        assert!(matches!(
            row.take::<String, _>("id"),
            Err(YdbError::Convert(_))
        ));
        assert!(row.take::<i64, _>("id").is_err());
    }

    #[test]
    #[allow(deprecated)]
    fn deprecated_methods_delegate_to_take() {
        let mut row = test_row();
        assert_eq!(row.remove_field_by_name("id").unwrap(), Value::Int64(42));
        assert_eq!(
            row.remove_field(1).unwrap(),
            Value::Text("hello".to_string())
        );
        assert!(row.remove_field_by_name("id").is_err());
    }
}
