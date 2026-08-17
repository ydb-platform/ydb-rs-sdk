use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::{YdbError, YdbResult};

const RESOURCE_COUNTER_CAPACITY: usize = Semaphore::MAX_PERMITS;

/// Shared shutdown state for a driver and every service client derived from it.
#[derive(Clone)]
pub(crate) struct ClientLifetime {
    inner: Arc<ClientLifetimeInner>,
}

/// A value that can only be accessed while its driver accepts new work.
#[derive(Clone)]
pub(crate) struct ShutdownGuarded<T> {
    lifetime: ClientLifetime,
    value: T,
}

struct ClientLifetimeInner {
    closed: AtomicBool,
    topic_readers: ResourceCounter,
    topic_writers: ResourceCounter,
    coordination_sessions: ResourceCounter,
}

/// Opaque ownership token held by one live client resource.
pub(crate) struct ClientResourceGuard {
    _permit: OwnedSemaphorePermit,
}

/// Final snapshot of resources that still depend on a shutting-down client.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct LiveClientResources {
    pub(crate) topic_readers: usize,
    pub(crate) topic_writers: usize,
    pub(crate) coordination_sessions: usize,
}

impl LiveClientResources {
    pub(crate) fn is_empty(&self) -> bool {
        self.topic_readers == 0 && self.topic_writers == 0 && self.coordination_sessions == 0
    }
}

impl Display for LiveClientResources {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "topic readers={topic_readers}, topic writers={topic_writers}, \
             coordination sessions={coordination_sessions}",
            topic_readers = self.topic_readers,
            topic_writers = self.topic_writers,
            coordination_sessions = self.coordination_sessions,
        )
    }
}

/// A closeable RAII counter. Permits represent live resources, not concurrency capacity.
struct ResourceCounter {
    name: &'static str,
    permits: Arc<Semaphore>,
}

impl ResourceCounter {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            permits: Arc::new(Semaphore::new(RESOURCE_COUNTER_CAPACITY)),
        }
    }

    fn register(&self) -> YdbResult<ClientResourceGuard> {
        match self.permits.clone().try_acquire_owned() {
            Ok(permit) => Ok(ClientResourceGuard { _permit: permit }),
            Err(TryAcquireError::Closed) => Err(YdbError::custom("client shutdown has started")),
            Err(TryAcquireError::NoPermits) => Err(YdbError::InternalError(format!(
                "{} resource counter exhausted",
                self.name
            ))),
        }
    }

    fn close(&self) {
        self.permits.close();
    }

    fn active(&self) -> usize {
        RESOURCE_COUNTER_CAPACITY - self.permits.available_permits()
    }
}

impl ClientLifetime {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(ClientLifetimeInner {
                closed: AtomicBool::new(false),
                topic_readers: ResourceCounter::new("topic reader"),
                topic_writers: ResourceCounter::new("topic writer"),
                coordination_sessions: ResourceCounter::new("coordination session"),
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

    pub(crate) fn guard<T>(&self, value: T) -> ShutdownGuarded<T> {
        ShutdownGuarded {
            lifetime: self.clone(),
            value,
        }
    }

    pub(crate) fn close(&self) {
        self.inner.closed.store(true, Ordering::Relaxed);
        self.inner.topic_readers.close();
        self.inner.topic_writers.close();
        self.inner.coordination_sessions.close();
    }

    pub(crate) fn register_topic_reader(&self) -> YdbResult<ClientResourceGuard> {
        self.inner.topic_readers.register()
    }

    pub(crate) fn register_topic_writer(&self) -> YdbResult<ClientResourceGuard> {
        self.inner.topic_writers.register()
    }

    pub(crate) fn register_coordination_session(&self) -> YdbResult<ClientResourceGuard> {
        self.inner.coordination_sessions.register()
    }

    pub(crate) fn live_resources(&self) -> LiveClientResources {
        LiveClientResources {
            topic_readers: self.inner.topic_readers.active(),
            topic_writers: self.inner.topic_writers.active(),
            coordination_sessions: self.inner.coordination_sessions.active(),
        }
    }
}

impl<T> ShutdownGuarded<T> {
    pub(crate) fn access(&self) -> YdbResult<&T> {
        self.lifetime.ensure_open()?;
        Ok(&self.value)
    }

    pub(crate) fn access_mut(&mut self) -> YdbResult<&mut T> {
        self.lifetime.ensure_open()?;
        Ok(&mut self.value)
    }

    pub(crate) fn register_topic_reader(&self) -> YdbResult<ClientResourceGuard> {
        self.lifetime.register_topic_reader()
    }

    pub(crate) fn register_topic_writer(&self) -> YdbResult<ClientResourceGuard> {
        self.lifetime.register_topic_writer()
    }

    pub(crate) fn register_coordination_session(&self) -> YdbResult<ClientResourceGuard> {
        self.lifetime.register_coordination_session()
    }
}
