use crate::errors;
use crate::errors::YdbError;
use crate::grpc_wrapper::raw_table_service::value::{RawResultSet, RawTypedValue, RawValue};
use crate::types::Value;
use itertools::Itertools;
use std::collections::HashMap;
use std::sync::Arc;
use std::vec::IntoIter;

/// One result set of a query - a column schema plus the rows that match it
///
/// A single query can return several result sets; see
/// [`QueryStream`](crate::QueryStream) for reading them one by one, or
/// [`QueryClient::query_result_set`](crate::QueryClient::query_result_set)
/// when exactly one is expected.
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

    /// Consume the result set and iterate over its rows
    pub fn rows(self) -> ResultSetRowsIter {
        ResultSetRowsIter {
            columns: Arc::new(self.columns),
            columns_by_name: Arc::new(self.columns_by_name),
            row_iter: self.raw_result_set.rows.into_iter(),
        }
    }

    /// Whether the server cut the result set off at its row limit
    ///
    /// When `true`, more rows matched the query than were returned.
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

/// A single row of a [`ResultSet`]
///
/// Fields are taken out of the row by name or by index. Each field can be
/// removed only once, which lets the row hand out owned [`Value`]s without
/// cloning.
#[derive(Debug)]
pub struct Row {
    columns: Arc<Vec<crate::types::Column>>,
    columns_by_name: Arc<HashMap<String, usize>>,
    raw_values: HashMap<usize, RawValue>,
}

impl Row {
    /// Take a field out of the row by column name
    ///
    /// Returns an error if there is no such column, or if the field was
    /// already removed.
    pub fn remove_field_by_name(&mut self, name: &str) -> errors::YdbResult<Value> {
        if let Some(&index) = self.columns_by_name.get(name) {
            return self.remove_field(index);
        }
        Err(YdbError::Custom("field not found".into()))
    }

    /// Take a field out of the row by zero-based column index
    ///
    /// Returns an error if the index is out of range, or if the field was
    /// already removed.
    pub fn remove_field(&mut self, index: usize) -> errors::YdbResult<Value> {
        match self.raw_values.remove(&index) {
            Some(val) => Ok(Value::try_from(RawTypedValue {
                r#type: self.columns[index].v_type.clone(),
                value: val,
            })?),
            None => Err(YdbError::Custom("it has no the field".into())),
        }
    }
}

/// Iterator over the rows of a [`ResultSet`], created by [`ResultSet::rows`]
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
