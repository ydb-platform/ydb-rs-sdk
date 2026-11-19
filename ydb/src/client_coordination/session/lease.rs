use std::sync::Arc;
use tracing::{debug, trace};

use tokio_util::sync::CancellationToken;

use crate::{
    YdbResult,
    grpc_wrapper::raw_coordination_service::session::release_semaphore::{
        RawReleaseSemaphoreRequest, RawReleaseSemaphoreResult,
    },
};

use super::controller::RequestController;

/// Releases a semaphore whose acquire is still in flight, unless its reply
/// arrives first.
///
/// A caller that stops waiting for an acquire, its future dropped by a
/// timeout, never gets the [`Lease`] that would release the semaphore, while
/// the server may still grant it. Dropped while armed, the guard sends
/// `ReleaseSemaphore` at once, which removes the session from the semaphore's
/// waiters queue, or releases the semaphore if it was already granted. The
/// request is sent synchronously, so an acquire the caller retries on the same
/// session follows it on the stream instead of being released by it.
pub(crate) struct PendingAcquire {
    release_channel: Arc<RequestController<RawReleaseSemaphoreResult>>,
    semaphore_name: String,
    armed: bool,
}

impl PendingAcquire {
    pub(crate) fn new(
        release_channel: Arc<RequestController<RawReleaseSemaphoreResult>>,
        semaphore_name: String,
    ) -> Self {
        Self {
            release_channel,
            semaphore_name,
            armed: true,
        }
    }

    /// The semaphore was acquired: the lease releases it from here on.
    pub(crate) fn into_lease(mut self, cancellation_token: CancellationToken) -> Lease {
        self.armed = false;
        Lease::new(
            self.release_channel.clone(),
            std::mem::take(&mut self.semaphore_name),
            cancellation_token,
        )
    }

    /// The semaphore was not acquired, so there is nothing to release.
    pub(crate) fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for PendingAcquire {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        trace!(
            "releasing semaphore {} after its acquire was abandoned",
            self.semaphore_name
        );
        let release = RawReleaseSemaphoreRequest::new(self.semaphore_name.clone());
        if let Err(err) = self.release_channel.send_detached(release) {
            // The stream is gone, so nothing more can be sent on this session.
            // The server releases what the session held or waited for when
            // the session expires.
            debug!(
                "could not release semaphore {} after an abandoned acquire: {}",
                self.semaphore_name, err
            );
        }
    }
}

#[allow(dead_code)]
pub struct Lease {
    release_channel: Arc<RequestController<RawReleaseSemaphoreResult>>,
    semaphore_name: String,
    cancellation_token: CancellationToken,
}

#[allow(dead_code)]
impl Lease {
    pub(crate) fn new(
        release_channel: Arc<RequestController<RawReleaseSemaphoreResult>>,
        semaphore_name: String,
        cancellation_token: CancellationToken,
    ) -> Lease {
        Lease {
            release_channel,
            semaphore_name,
            cancellation_token,
        }
    }

    pub fn alive(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    pub fn release(self) {
        tokio::spawn(Lease::release_impl(
            self.semaphore_name.clone(),
            self.cancellation_token.clone(),
            self.release_channel.clone(),
        ));
    }

    async fn release_impl(
        semaphore_name: String,
        cancellation_token: CancellationToken,
        release_channel: Arc<RequestController<RawReleaseSemaphoreResult>>,
    ) -> YdbResult<()> {
        cancellation_token.cancel();
        trace!("releasing semaphore {}", semaphore_name);
        let mut rx = release_channel
            .send(RawReleaseSemaphoreRequest::new(semaphore_name.clone()))
            .await?;

        let result = rx.recv().await;
        if let Some(answer) = result
            && answer.released
        {
            trace!("semaphore {} released", semaphore_name);
        }
        Ok(())
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        tokio::spawn(Lease::release_impl(
            self.semaphore_name.clone(),
            self.cancellation_token.clone(),
            self.release_channel.clone(),
        ));
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use ydb_grpc::ydb_proto::coordination::{SessionRequest, session_request};

    use super::*;

    fn release_controller() -> (
        Arc<RequestController<RawReleaseSemaphoreResult>>,
        mpsc::UnboundedReceiver<SessionRequest>,
    ) {
        let (messages_tx, messages_rx) = mpsc::unbounded_channel();
        (Arc::new(RequestController::new(messages_tx)), messages_rx)
    }

    fn released_name(message: SessionRequest) -> String {
        match message.request {
            Some(session_request::Request::ReleaseSemaphore(release)) => release.name,
            other => panic!("expected a release, got {other:?}"),
        }
    }

    #[test]
    fn dropped_pending_acquire_releases_the_semaphore() {
        let (controller, mut messages_rx) = release_controller();

        drop(PendingAcquire::new(controller, "semaphore".to_string()));

        assert_eq!(released_name(messages_rx.try_recv().unwrap()), "semaphore");
        assert!(messages_rx.try_recv().is_err());
    }

    #[test]
    fn disarmed_pending_acquire_releases_nothing() {
        let (controller, mut messages_rx) = release_controller();

        PendingAcquire::new(controller, "semaphore".to_string()).disarm();

        assert!(messages_rx.try_recv().is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn acquired_semaphore_is_released_once_by_its_lease() {
        let (controller, mut messages_rx) = release_controller();

        let lease = PendingAcquire::new(controller, "semaphore".to_string())
            .into_lease(CancellationToken::new());
        assert!(messages_rx.try_recv().is_err());

        drop(lease);
        assert_eq!(
            released_name(messages_rx.recv().await.unwrap()),
            "semaphore"
        );
        assert!(messages_rx.try_recv().is_err());
    }
}
