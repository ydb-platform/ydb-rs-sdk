use crate::{SemaphoreDescription, YdbResult};

pub struct Lease;

#[allow(dead_code)]
impl Lease {
    pub(crate) async fn new() -> YdbResult<Self> {
        unimplemented!()
    }

    pub async fn release(mut self) -> YdbResult<()> {
        self.release_impl().await
    }

    async fn release_impl(&mut self) -> YdbResult<()> {
        unimplemented!()
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        // TODO: wait?
        self.release_impl();
    }
}
