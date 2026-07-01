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
pub(crate) const DRIVER_INITIALIZE: &str = dot_concat!("ydb", "Driver", "Initialize");

// SDK-specific naming
pub const QUERY_CLIENT_BEGIN_STREAM_ONCE: &str = dot_concat!("ydb", "Query", "BeginStreamOnce");
pub const QUERY_CLIENT_BEGIN_STREAM: &str = dot_concat!("ydb", "Query", "BeginStream");
pub const QUERY_CLIENT_ENSURE_TX_SESSION: &str = dot_concat!("ydb", "Query", "EnsureTxSession");
pub const QUERY_CLIENT_RELEASE_TX_SESSION: &str = dot_concat!("ydb", "Query", "ReleaseTxSession");
