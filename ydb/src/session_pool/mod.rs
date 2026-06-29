mod query_pool;
mod table_pool;

#[cfg(test)]
mod regression_tests;

pub use query_pool::{QuerySessionPoolSettings, QuerySessionPoolStats};

pub(crate) use query_pool::{QuerySessionLease, QuerySessionPool};

pub(crate) use table_pool::SessionPool;

/// Default session pool settings for a newly created [`crate::Client`].
pub fn default_session_pool_settings() -> QuerySessionPoolSettings {
    QuerySessionPoolSettings::default()
}
