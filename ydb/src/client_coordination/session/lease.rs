use tokio_util::sync::CancellationToken;

use crate::{
    grpc_wrapper::raw_coordination_service::session::release_semaphore::RawReleaseSemaphoreRequest,
    Session,
};

pub struct Lease<'a> {
    session: &'a Session,
    semaphore_name: String,
    cancellation_token: CancellationToken,
}

#[allow(dead_code)]
impl<'a> Lease<'a> {
    pub(crate) fn new(
        session: &'a Session,
        semaphore_name: String,
        cancellation_token: CancellationToken,
    ) -> Lease<'_> {
        Lease {
            session,
            semaphore_name,
            cancellation_token,
        }
    }

    pub fn alive(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    fn release_impl(mut self) {
        self.cancellation_token.cancel();
        drop(
            self.session
                .release_semaphore
                .send(RawReleaseSemaphoreRequest::new(self.semaphore_name)),
        );
    }
}

impl<'a> Drop for Lease<'a> {
    fn drop(&mut self) {
        self.release_impl();
    }
}
