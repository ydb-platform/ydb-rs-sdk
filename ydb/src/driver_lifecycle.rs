use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::{YdbError, YdbResult};

const RESOURCE_COUNTER_CAPACITY: usize = Semaphore::MAX_PERMITS;

/// Root driver lifecycle and the shared state observed by its derived clients.
pub(crate) struct DriverLifecycle {
    state: Arc<DriverLifecycleState>,
    shutdown_completed: bool,
}

/// A value that can only be accessed while its driver accepts new work.
#[derive(Clone)]
pub(crate) struct DriverGuarded<T> {
    state: Arc<DriverLifecycleState>,
    value: T,
}

struct DriverLifecycleState {
    shutdown_started: AtomicBool,
    topic_readers: ResourceCounter,
    topic_writers: ResourceCounter,
    coordination_sessions: ResourceCounter,
}

/// Opaque ownership token held by one live client resource.
pub(crate) struct DriverResourceGuard {
    _permit: OwnedSemaphorePermit,
}

/// Final snapshot of resources that still depend on a shutting-down driver.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct LiveDriverResources {
    pub(crate) topic_readers: usize,
    pub(crate) topic_writers: usize,
    pub(crate) coordination_sessions: usize,
}

impl LiveDriverResources {
    pub(crate) fn is_empty(&self) -> bool {
        self.topic_readers == 0 && self.topic_writers == 0 && self.coordination_sessions == 0
    }
}

impl Display for LiveDriverResources {
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

    fn register(&self) -> YdbResult<DriverResourceGuard> {
        match self.permits.clone().try_acquire_owned() {
            Ok(permit) => Ok(DriverResourceGuard { _permit: permit }),
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

impl DriverLifecycle {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(DriverLifecycleState {
                shutdown_started: AtomicBool::new(false),
                topic_readers: ResourceCounter::new("topic reader"),
                topic_writers: ResourceCounter::new("topic writer"),
                coordination_sessions: ResourceCounter::new("coordination session"),
            }),
            shutdown_completed: false,
        }
    }

    pub(crate) fn guard<T>(&self, value: T) -> DriverGuarded<T> {
        DriverGuarded {
            state: self.state.clone(),
            value,
        }
    }

    pub(crate) fn start_shutdown(&self) {
        self.state.start_shutdown();
    }

    pub(crate) fn complete_shutdown(&mut self) {
        self.shutdown_completed = true;
    }

    pub(crate) fn live_resources(&self) -> LiveDriverResources {
        self.state.live_resources()
    }
}

impl Drop for DriverLifecycle {
    fn drop(&mut self) {
        if !self.shutdown_completed {
        if !self.shutdown_completed {
            self.state.start_shutdown();
            tracing::warn!(
                "YDB driver dropped without completing graceful shutdown; call Client::shutdown().await before dropping it"
            );
        }
    }
}

impl DriverLifecycleState {
    fn ensure_open(&self) -> YdbResult<()> {
        if self.shutdown_started.load(Ordering::Relaxed) {
            Err(YdbError::custom("client shutdown has started"))
        } else {
            Ok(())
        }
    }

    fn start_shutdown(&self) {
        self.shutdown_started.store(true, Ordering::Relaxed);
        self.topic_readers.close();
        self.topic_writers.close();
        self.coordination_sessions.close();
    }

    fn live_resources(&self) -> LiveDriverResources {
        LiveDriverResources {
            topic_readers: self.topic_readers.active(),
            topic_writers: self.topic_writers.active(),
            coordination_sessions: self.coordination_sessions.active(),
        }
    }
}

impl<T> DriverGuarded<T> {
    pub(crate) fn access(&self) -> YdbResult<&T> {
        self.state.ensure_open()?;
        Ok(&self.value)
    }

    pub(crate) fn access_mut(&mut self) -> YdbResult<&mut T> {
        self.state.ensure_open()?;
        Ok(&mut self.value)
    }

    pub(crate) fn register_topic_reader(&self) -> YdbResult<DriverResourceGuard> {
        self.state.topic_readers.register()
    }

    pub(crate) fn register_topic_writer(&self) -> YdbResult<DriverResourceGuard> {
        self.state.topic_writers.register()
    }

    pub(crate) fn register_coordination_session(&self) -> YdbResult<DriverResourceGuard> {
        self.state.coordination_sessions.register()
    }
}
