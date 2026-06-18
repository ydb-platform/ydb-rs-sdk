pub const RUN_WITH_RETRY: &str = "ydb.RunWithRetry";
pub const TRY: &str = "ydb.Try";
pub const EXECUTE_QUERY: &str = "ydb.ExecuteQuery";
pub const BEGIN_TRANSACTION: &str = "ydb.BeginTransaction";
pub const COMMIT: &str = "ydb.Commit";
pub const ROLLBACK: &str = "ydb.Rollback";
pub const CREATE_SESSION: &str = "ydb.CreateSession";
pub const DRIVER_INITIALIZE: &str = "ydb.Driver.Initialize";

pub(crate) fn public_api(type_name: &str, method_name: &str) -> String {
    format!("ydb.{}.{}", type_name, method_name)
}

pub(crate) fn grpc(method_name: &str) -> String {
    format!("ydb.grpc.{}", method_name)
}
