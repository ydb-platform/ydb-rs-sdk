use crate::{
    client_coordination::list_types::SemaphoreDescription, AcquireCount, AcquireOptions,
    CoordinationClient, DescribeOptions, YdbResult,
};

use super::{create_options::SemaphoreLimit, lease::Lease};

pub struct Session;

#[allow(dead_code)]
impl Session {
    pub(crate) async fn new() -> YdbResult<Self> {
        unimplemented!()
    }

    pub async fn close(self) -> YdbResult<()> {
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
        &mut self,
        _name: String,
        _count: AcquireCount,
        _options: AcquireOptions,
    ) -> YdbResult<Option<Lease>> {
        unimplemented!()
    }

    pub fn client(&self) -> CoordinationClient {
        unimplemented!()
    }
}
