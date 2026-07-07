#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueryTxCommitStatus {
    /// The transaction definitely committed.
    Committed,

    /// The transaction did not commit, or we cannot prove it committed.
    Aborted,
}

pub(crate) trait QueryTxHook: Send + Sync + 'static {
    fn after_commit(&mut self, status: QueryTxCommitStatus);
}
