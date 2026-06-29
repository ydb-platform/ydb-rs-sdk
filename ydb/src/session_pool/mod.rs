mod query_pool;
mod table_pool;

#[cfg(test)]
mod regression_tests;

pub use query_pool::{SessionPoolSettings, SessionPoolStats};

pub(crate) use query_pool::{QuerySessionLease, QuerySessionPool};

pub(crate) use table_pool::SessionPool;

/// Default session pool settings for a newly created [`crate::Client`].
pub fn default_session_pool_settings() -> SessionPoolSettings {
    SessionPoolSettings::default()
}

/// Deprecated alias for [`SessionPoolSettings`].
#[deprecated(note = "renamed to SessionPoolSettings")]
pub type QuerySessionPoolSettings = SessionPoolSettings;

/// Deprecated alias for [`SessionPoolStats`].
#[deprecated(note = "renamed to SessionPoolStats")]
pub type QuerySessionPoolStats = SessionPoolStats;
