#[macro_export]

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
macro_rules! ydb_params {
    (
        $($name:expr => $val:expr ),+ $(,)?
    ) => {
        std::collections::HashMap::<String, $crate::Value>::from_iter([
            $( ($name.into(), $val.into()), )+
        ])
    };
}
