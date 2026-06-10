pub(crate) struct PartitionSession {
    pub partition_session_id: i64,
    pub partition_id: i64,
    pub topic: String,

    // Each offset up to and including (committed_offset - 1) was fully processed.
    pub next_commit_offset_start: i64,
}
