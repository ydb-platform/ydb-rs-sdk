use crate::YdbResult;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueryTxCommitStatus {
    /// The transaction definitely committed.
    Committed,

    /// The transaction did not commit, or we cannot prove it committed.
    Aborted,
}

#[async_trait::async_trait]
pub(crate) trait QueryTxHook: Send + Sync + 'static {
    async fn before_commit(&mut self) -> YdbResult<()> {
        Ok(())
    }

    fn after_commit(&mut self, status: QueryTxCommitStatus);
}
