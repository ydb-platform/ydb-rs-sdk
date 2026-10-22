use crate::errors::YdbResult;
use secrecy::SecretString;
use std::fmt::{Debug, Formatter};
use std::ops::Add;
use std::time::{Duration, Instant};

pub(crate) const DEFAULT_TOKEN_RENEW_INTERVAL: Duration = Duration::from_secs(3600); // 1 hour

/// An auth token together with the moment it should be renewed
///
/// Returned by [`Credentials::create_token`]. The SDK caches the token and
/// refreshes it in background once `next_renew` passes.
#[derive(Debug, Clone)]
pub struct TokenInfo {
    pub(crate) token: SecretString,
    pub(crate) next_renew: Instant,
}

impl TokenInfo {
    pub(crate) fn token(token: String) -> Self {
        Self {
            token: SecretString::new(token),
            next_renew: Instant::now().add(DEFAULT_TOKEN_RENEW_INTERVAL),
        }
    }

    pub(crate) fn with_renew(mut self, next_renew: Instant) -> Self {
        self.next_renew = next_renew;
        self
    }
}

/// Source of auth tokens for the driver
///
/// Implement it to plug a custom auth scheme into
/// [`ClientBuilder::with_credentials`](crate::ClientBuilder::with_credentials).
/// The SDK ships implementations for static tokens, user/password, external
/// commands and cloud metadata services - see the crate root for the full list.
pub trait Credentials: Send + Sync {
    /// Produce a fresh token
    ///
    /// The implementation may block (spawn a command, make a network request);
    /// it is called from a thread where blocking is allowed.
    ///
    /// A successful result is cached until the returned renewal deadline, after
    /// which `create_token` is called again in background. While renewal keeps
    /// failing the previously cached token stays in use, so `next_renew`
    /// should leave room before the real expiration for retries.
    fn create_token(&self) -> YdbResult<TokenInfo>;

    /// Short description used in logs and in the `Debug` output
    ///
    /// Must not leak the token itself.
    fn debug_string(&self) -> String {
        "some credentials".to_string()
    }
}

impl Debug for dyn Credentials + 'static {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.debug_string().as_str())
    }
}
