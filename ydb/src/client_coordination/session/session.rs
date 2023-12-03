use tokio::sync::mpsc;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

use crate::{
    client_coordination::list_types::SemaphoreDescription, AcquireCount, AcquireOptions,
    CoordinationClient, DescribeOptions, YdbResult,
};

use super::{create_options::SemaphoreLimit, describe_options::WatchOptions, lease::Lease};

pub struct Session;

#[allow(dead_code)]
impl Session {
    pub(crate) async fn new() -> YdbResult<Self> {
        unimplemented!()
    }

    pub fn alive(&self) -> CancellationToken {
        unimplemented!()
    }

    pub async fn create_semaphore(
        &mut self,
        _name: String,
        _limit: SemaphoreLimit,
        _data: Option<Vec<u8>>,
    ) -> YdbResult<()> {
        unimplemented!()
    }

    pub async fn describe_semaphore(
        &mut self,
        _name: String,
        _options: DescribeOptions,
    ) -> YdbResult<SemaphoreDescription> {
        unimplemented!()
    }

    pub async fn watch_semaphore(
        &mut self,
        _name: String,
        _options: WatchOptions,
    ) -> YdbResult<mpsc::Receiver<SemaphoreDescription>> {
        unimplemented!()
    }

    pub async fn update_semaphore(
        &mut self,
        _name: String,
        _data: Option<Vec<u8>>,
    ) -> YdbResult<()> {
        unimplemented!()
    }

    pub async fn delete_semaphore(&mut self, _name: String) -> YdbResult<()> {
        unimplemented!()
    }

    pub async fn force_delete_semaphore(&mut self, _name: String) -> YdbResult<()> {
        unimplemented!()
    }

    pub async fn acquire_semaphore(
        &self,
        _name: String,
        _count: AcquireCount,
        _options: AcquireOptions,
    ) -> YdbResult<Lease> {
        unimplemented!()
    }

    pub fn client(&self) -> CoordinationClient {
        unimplemented!()
    }
}
