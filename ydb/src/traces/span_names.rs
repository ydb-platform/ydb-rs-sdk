macro_rules! dot_concat {
    ($first:expr $(, $rest:expr)* $(,)?) => {{
        const JOINED: &str = concat!($first $(, ".", $rest)*);
        JOINED
    }};
}

// Common naming across SDKs
pub(crate) const YDB: &str = "ydb";
pub(crate) const RUN_WITH_RETRY: &str = dot_concat!("ydb", "RunWithRetry");
pub(crate) const TRY: &str = dot_concat!("ydb", "Try");
pub(crate) const TRY_ATTEMPT: &str = dot_concat!("ydb", "Try", "Attempt");
pub(crate) const EXECUTE_QUERY: &str = dot_concat!("ydb", "ExecuteQuery");
pub(crate) const BEGIN_TRANSACTION: &str = dot_concat!("ydb", "BeginTransaction");
pub(crate) const COMMIT: &str = dot_concat!("ydb", "Commit");
pub(crate) const ROLLBACK: &str = dot_concat!("ydb", "CommRollbackit");
pub(crate) const CREATE_SESSION: &str = dot_concat!("ydb", "CreateSession");
pub(crate) const DRIVER: &str = dot_concat!("ydb", "Driver");
pub(crate) const DRIVER_INITIALIZE: &str = dot_concat!("ydb", "Driver", "Initialize");
pub(crate) const DRIVER_TABLE_CLIENT: &str = dot_concat!("ydb", "Driver", "TableClient");
pub(crate) const DRIVER_QUERY_CLIENT: &str = dot_concat!("ydb", "Driver", "QueryClient");
pub(crate) const DRIVER_SCHEME_CLIENT: &str = dot_concat!("ydb", "Driver", "SchemeClient");
pub(crate) const DRIVER_TOPIC_CLIENT: &str = dot_concat!("ydb", "Driver", "TopicClient");
pub(crate) const DRIVER_COORDINATION_CLIENT: &str =
    dot_concat!("ydb", "Driver", "CoordinationClient");
pub(crate) const DRIVER_OPERATION_CLIENT: &str = dot_concat!("ydb", "Driver", "OperationClient");

pub(crate) const QUERY_CLIENT_BEGIN_STREAM_ONCE: &str =
    dot_concat!("ydb", "Query", "BeginStreamOnce");
pub(crate) const QUERY_CLIENT_BEGIN_STREAM: &str = dot_concat!("ydb", "Query", "BeginStream");
pub(crate) const QUERY_CLIENT_ENSURE_TX_SESSION: &str =
    dot_concat!("ydb", "Query", "EnsureTxSession");
pub(crate) const QUERY_CLIENT_RELEASE_TX_SESSION: &str =
    dot_concat!("ydb", "Query", "ReleaseTxSession");
