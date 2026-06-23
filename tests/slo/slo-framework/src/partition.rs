use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;

pub type PartitionID = i64;

/// Workload-level bucket key. One bucket = the smallest scope inside which the
/// workload requires per-message ordering (single-consumer = one partition;
/// multi-consumer = one `(consumer, partition)` pair; producer-routed = one
/// `(producer_id, partition)` pair; etc.).
///
/// Construct via one of the named constructors below — the inner string format
/// is the contract between writer-side and reader-side bucket derivation, and
/// ad-hoc construction would silently desynchronise the workload's oracle.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct BucketID(String);

impl BucketID {
    /// Bucket keyed by `(consumer_index, partition_id)`. Used by single- and
    /// multi-consumer scenarios where each consumer's reader-set maintains its
    /// own per-partition ordering oracle.
    pub fn for_consumer_partition(consumer_idx: u32, partition_id: PartitionID) -> Self {
        Self(format!("c{consumer_idx}:p{partition_id}"))
    }
}

/// Per-worker writer handle returned by `Queue::open_writers`.
#[async_trait]
pub trait MessageWriter: Send + Sync {
    async fn write(&mut self, data: Vec<u8>) -> Result<(), String>;
}

/// Writer plus the set of workload buckets the workload must mirror each
/// payload to before/with the acknowledged `write`. The bucket list MUST cover
/// exactly the consumer-scoped buckets that will later observe this writer's
/// messages — otherwise the local-store oracle drifts even when transport is
/// correct.
pub struct WriterHandle {
    pub writer: Box<dyn MessageWriter>,
    pub buckets: Vec<BucketID>,
}

/// Per-worker reader handle returned by `Queue::open_readers`.
#[async_trait]
pub trait MessageReader: Send + Sync {
    async fn read_batch(&mut self) -> Result<TopicBatch, String>;
    async fn commit(&mut self, marker: CommitMarker) -> Result<(), String>;
}

/// Single message decoded from a topic batch. `bucket_id` identifies the
/// ordering bucket for this message.
#[derive(Debug)]
pub struct TestMessage {
    pub partition_id: PartitionID,
    pub bucket_id: BucketID,
    pub offset: i64,
    pub seq_no: i64,
    pub data: Vec<u8>,
    pub commit_marker: CommitMarker,
}

/// Batch returned by `MessageReader::read_batch`. The batch-level `marker`
/// drives the "commit whole batch" path; per-message markers on `messages`
/// drive the "commit each message" path.
#[derive(Debug)]
pub struct TopicBatch {
    pub messages: Vec<TestMessage>,
    pub marker: CommitMarker,
}

/// Opaque commit token. `MessageReader` impls validate the marker's concrete
/// type and return an error on mismatch — never panic.
#[derive(Clone, Debug)]
pub struct CommitMarker(pub Arc<dyn Any + Send + Sync>);

impl CommitMarker {
    pub fn new<T: Any + Send + Sync>(inner: T) -> Self {
        Self(Arc::new(inner))
    }
}
