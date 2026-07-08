mod pool;
mod table_pool;

#[cfg(test)]
mod regression_tests;

pub use pool::{SessionPoolSettings, SessionPoolStats};

pub(crate) use pool::{SessionPool, SessionPoolLease, spawn_pool_release};

pub(crate) use table_pool::TableSessionPool;

/// Default session pool settings for a newly created [`crate::Client`].
pub fn default_session_pool_settings() -> SessionPoolSettings {
    SessionPoolSettings::default()
}
