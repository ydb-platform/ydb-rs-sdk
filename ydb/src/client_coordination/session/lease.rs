use tokio_util::sync::CancellationToken;

use crate::YdbResult;

pub struct Lease;

#[allow(dead_code)]
impl Lease {
    pub(crate) async fn new() -> YdbResult<Self> {
        unimplemented!()
    }

    pub fn alive(&self) -> CancellationToken {
        unimplemented!()
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
