use crate::YdbResult;

#[async_trait::async_trait]
pub(crate) trait Waiter {
    async fn wait(&self) -> YdbResult<()>;
}
