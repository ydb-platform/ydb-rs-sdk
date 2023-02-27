
/// sugar for manual construct query params
///
/// similar to Query::new("SELECT 1").with_params(HashMap::<String, Value>::from_iter(
///     [
///         ("$val1".to_string(), 123.into()),
///         ("$val2".to_string(), "asdas".to_string().into()),
///     ]
/// )
///
/// Example:
/// ```no_run
/// use ydb::{Query, ydb_params};
///
/// Query::new("SELECT 1").with_params( ydb_params!( "$val1" => 123, "$val2" => "asdas" ));
/// ```
#[macro_export]
macro_rules! ydb_params {
    (
        $($name:expr => $val:expr ),+ $(,)?
    ) => {
        std::collections::HashMap::<String, $crate::Value>::from_iter([
            $( ($name.into(), $val.into()), )+
        ])
    };
}

///  Sugar for manual construct structs
///  Example:
/// ```
///  use ydb::{Value, ydb_struct};
///  let s_manual = Value::struct_from_fields(vec![
///     ("field1".to_string(), 1.into()),
///     ("field2".to_string(), "test".into()),
///  ]);
///
///  let s_macros = ydb_struct!(
///     "field1" => 1,
///     "field2" => "test"
///  );
///  assert!(s_manual == s_macros)
/// ```
#[macro_export]
macro_rules! ydb_struct {
    (
        $($field_name:expr => $val:expr),+ $(,)?
    ) => {
        ydb::Value::struct_from_fields(vec![
            $( ($field_name.into(), $val.into()), )+
        ])
    }
}