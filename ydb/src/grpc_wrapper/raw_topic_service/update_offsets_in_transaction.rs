use crate::grpc_wrapper::raw_topic_service::common::partition::RawOffsetsRange;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use ydb_grpc::ydb_proto::operations::OperationParams;
use ydb_grpc::ydb_proto::topic::{
    TransactionIdentity,
    UpdateOffsetsInTransactionRequest,
    update_offsets_in_transaction_request::{TopicOffsets, topic_offsets::PartitionOffsets},
};

/// Raw wrapper for TransactionIdentity
#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct RawTransactionIdentity {
    /// Transaction identifier from TableService.
    pub id: String,
    /// Session identifier from TableService.
    pub session: String,
}

impl From<RawTransactionIdentity> for TransactionIdentity {
    fn from(value: RawTransactionIdentity) -> Self {
        Self {
            id: value.id,
            session: value.session,
        }
    }
}

impl From<TransactionIdentity> for RawTransactionIdentity {
    fn from(value: TransactionIdentity) -> Self {
        Self {
            id: value.id,
            session: value.session,
        }
    }
}

/// Raw wrapper for PartitionOffsets
#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct RawPartitionOffsets {
    /// Partition identifier.
    pub partition_id: i64,
    /// List of offset ranges.
    pub partition_offsets: Vec<RawOffsetsRange>,
}

impl From<RawPartitionOffsets> for PartitionOffsets {
    fn from(value: RawPartitionOffsets) -> Self {
        Self {
            partition_id: value.partition_id,
            partition_offsets: value.partition_offsets.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl From<PartitionOffsets> for RawPartitionOffsets {
    fn from(value: PartitionOffsets) -> Self {
        Self {
            partition_id: value.partition_id,
            partition_offsets: value.partition_offsets.into_iter().map(|x| x.into()).collect(),
        }
    }
}

/// Raw wrapper for TopicOffsets
#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct RawTopicOffsets {
    /// Topic path.
    pub path: String,
    /// Ranges of offsets by partitions.
    pub partitions: Vec<RawPartitionOffsets>,
}

impl From<RawTopicOffsets> for TopicOffsets {
    fn from(value: RawTopicOffsets) -> Self {
        Self {
            path: value.path,
            partitions: value.partitions.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl From<TopicOffsets> for RawTopicOffsets {
    fn from(value: TopicOffsets) -> Self {
        Self {
            path: value.path,
            partitions: value.partitions.into_iter().map(|x| x.into()).collect(),
        }
    }
}

/// Raw wrapper for UpdateOffsetsInTransactionRequest
#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct RawUpdateOffsetsInTransactionRequest {
    pub operation_params: RawOperationParams,
    pub tx: RawTransactionIdentity,
    /// Ranges of offsets by topics.
    pub topics: Vec<RawTopicOffsets>,
    pub consumer: String,
}

impl From<RawUpdateOffsetsInTransactionRequest> for UpdateOffsetsInTransactionRequest {
    fn from(value: RawUpdateOffsetsInTransactionRequest) -> Self {
        Self {
            operation_params: Some(OperationParams::from(value.operation_params)),
            tx: Some(TransactionIdentity::from(value.tx)),
            topics: value.topics.into_iter().map(|x| x.into()).collect(),
            consumer: value.consumer,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc_wrapper::raw_common_types::Duration;
    use std::time::Duration as StdDuration;

    #[test]
    fn test_raw_wrappers_conversion() {
        // Test that raw wrappers can convert to protobuf types correctly
        let raw_tx = RawTransactionIdentity {
            id: "test_tx_id".to_string(),
            session: "test_session_id".to_string(),
        };

        let raw_offsets_range = RawOffsetsRange {
            start: 0,
            end: 100,
        };

        let raw_partition_offsets = RawPartitionOffsets {
            partition_id: 1,
            partition_offsets: vec![raw_offsets_range.clone()],
        };

        let raw_topic_offsets = RawTopicOffsets {
            path: "test-topic".to_string(),
            partitions: vec![raw_partition_offsets],
        };

        let raw_operation_params = RawOperationParams::new_with_timeouts(
            StdDuration::from_secs(30),
            StdDuration::from_secs(60),
        );

        let raw_request = RawUpdateOffsetsInTransactionRequest {
            operation_params: raw_operation_params,
            tx: raw_tx,
            topics: vec![raw_topic_offsets],
            consumer: "test-consumer".to_string(),
        };

        // Convert to protobuf type
        let proto_request: UpdateOffsetsInTransactionRequest = raw_request.into();

        // Verify the conversion worked
        assert!(proto_request.operation_params.is_some());
        assert!(proto_request.tx.is_some());
        assert_eq!(proto_request.topics.len(), 1);
        assert_eq!(proto_request.consumer, "test-consumer");

        let tx = proto_request.tx.unwrap();
        assert_eq!(tx.id, "test_tx_id");
        assert_eq!(tx.session, "test_session_id");

        let topic = &proto_request.topics[0];
        assert_eq!(topic.path, "test-topic");
        assert_eq!(topic.partitions.len(), 1);

        let partition = &topic.partitions[0];
        assert_eq!(partition.partition_id, 1);
        assert_eq!(partition.partition_offsets.len(), 1);

        let offsets = &partition.partition_offsets[0];
        assert_eq!(offsets.start, 0);
        assert_eq!(offsets.end, 100);
    }

    #[test]
    fn test_bidirectional_conversion() {
        // Test that we can convert from protobuf to raw and back
        let original_tx = TransactionIdentity {
            id: "original_tx".to_string(),
            session: "original_session".to_string(),
        };

        // Convert to raw
        let raw_tx: RawTransactionIdentity = original_tx.clone().into();
        assert_eq!(raw_tx.id, "original_tx");
        assert_eq!(raw_tx.session, "original_session");

        // Convert back to protobuf  
        let converted_tx: TransactionIdentity = raw_tx.into();
        assert_eq!(converted_tx.id, original_tx.id);
        assert_eq!(converted_tx.session, original_tx.session);
    }
} 