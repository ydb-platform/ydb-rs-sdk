mod query_pool;
mod table_pool;

pub use query_pool::{QuerySessionPoolSettings, QuerySessionPoolStats};

pub(crate) use query_pool::{QuerySessionLease, QuerySessionPool};

pub(crate) use table_pool::SessionPool;

/// Default driver session pool limit (matches legacy table client pool size).
const DEFAULT_DRIVER_POOL_LIMIT: usize = 1000;

/// Default session pool settings for a newly created [`crate::Client`].
pub fn default_session_pool_settings() -> QuerySessionPoolSettings {
    QuerySessionPoolSettings::default().with_limit(DEFAULT_DRIVER_POOL_LIMIT)
}
