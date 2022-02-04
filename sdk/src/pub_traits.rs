use crate::errors::YdbResult;
use std::fmt::{Debug, Formatter, Write};
use std::ops::Add;
use std::time::{Duration, Instant};

pub const DEFAULT_TOKEN_RENEW_INTERVAL: Duration = Duration::from_secs(3600); // 1 hour

#[derive(Debug, Clone)]
pub struct TokenInfo {
    pub(crate) token: String,
    pub(crate) next_renew: Instant,
}

impl TokenInfo {
    pub fn token(token: String) -> Self {
        return Self {
            token,
            next_renew: Instant::now().add(DEFAULT_TOKEN_RENEW_INTERVAL),
        };
    }

    pub fn with_renew(mut self, next_renew: Instant) -> Self {
        self.next_renew = next_renew;
        return self;
    }
}

pub trait Credentials: Send + Sync {
    // may not cache result and can block for some time (command execute, network request)
    // if always called from thread, available to block
    // successfully result will cache until TokenInfo.next_renew,
    // then create_token called in background.
    // cached token will use until successfully return again
    // and TokenInfo.next_renew reserve until token expire for renew it
    // and for retry errors
    fn create_token(&self) -> YdbResult<TokenInfo>;

    fn debug_string(&self) -> String {
        return "some credentials".to_string();
    }
}

impl Debug for dyn Credentials + 'static {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.debug_string().as_str())
    }
}
