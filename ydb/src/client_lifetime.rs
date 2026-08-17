use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::{YdbError, YdbResult};

/// Shared shutdown state for a driver and every service client derived from it.
#[derive(Clone)]
pub(crate) struct ClientLifetime {
    inner: Arc<ClientLifetimeInner>,
}

struct ClientLifetimeInner {
    closed: AtomicBool,
}

impl ClientLifetime {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(ClientLifetimeInner {
                closed: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn ensure_open(&self) -> YdbResult<()> {
        if self.inner.closed.load(Ordering::Relaxed) {
            Err(YdbError::custom("client shutdown has started"))
        } else {
            Ok(())
        }
    }

    pub(crate) fn close(&self) {
        self.inner.closed.store(true, Ordering::Relaxed);
    }
}
