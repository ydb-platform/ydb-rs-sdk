use std::sync::Arc;
use tracing::trace;

use tokio_util::sync::CancellationToken;

use crate::{
    grpc_wrapper::raw_coordination_service::session::release_semaphore::{
        RawReleaseSemaphoreRequest, RawReleaseSemaphoreResult,
    },
    YdbResult,
};

use super::controller::RequestController;

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
        trace!("releaseing semaphore {}", semaphore_name);
        let mut rx = release_channel
            .send(RawReleaseSemaphoreRequest::new(semaphore_name.clone()))
            .await?;

        let result = rx.recv().await;
        if let Some(answer) = result {
            if answer.released {
                trace!("semaphore {} released", semaphore_name);
            }
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
