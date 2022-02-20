#[macro_export]

macro_rules! ydb_params {
    (
        $($name:expr => $val:expr ),+ $(,)?
    ) => {
        HashMap::<String, $crate::Value>::from_iter([
            $( ($name.into(), $val.into()), )+
        ])
    };
}
