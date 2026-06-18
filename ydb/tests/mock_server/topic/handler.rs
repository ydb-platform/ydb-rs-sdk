use ydb_grpc::ydb_proto::topic;
use ydb_grpc::ydb_proto::topic::{stream_read_message, stream_write_message};

#[derive(Clone, Debug)]
pub enum TopicIncoming {
    StreamRead {
        stream_id: u64,
        msg: stream_read_message::from_client::ClientMessage,
    },
    StreamWrite {
        stream_id: u64,
        msg: stream_write_message::from_client::ClientMessage,
    },
    CommitOffset(topic::CommitOffsetRequest),
    UpdateOffsetsInTransaction(topic::UpdateOffsetsInTransactionRequest),
    CreateTopic(topic::CreateTopicRequest),
    DescribeTopic(topic::DescribeTopicRequest),
    DescribeConsumer(topic::DescribeConsumerRequest),
    AlterTopic(topic::AlterTopicRequest),
    DropTopic(topic::DropTopicRequest),
}

#[derive(Debug)]
pub enum TopicReply {
    StreamRead {
        stream_id: u64,
        msg: stream_read_message::FromServer,
    },
    StreamWrite {
        stream_id: u64,
        msg: stream_write_message::FromServer,
    },
    CommitOffset(topic::CommitOffsetResponse),
    UpdateOffsetsInTransaction(topic::UpdateOffsetsInTransactionResponse),
    CreateTopic(topic::CreateTopicResponse),
    DescribeTopic(topic::DescribeTopicResponse),
    DescribeConsumer(topic::DescribeConsumerResponse),
    AlterTopic(topic::AlterTopicResponse),
    DropTopic(topic::DropTopicResponse),
}

pub type TopicTx = tokio::sync::mpsc::UnboundedSender<TopicReply>;
pub type TopicRx = tokio::sync::mpsc::UnboundedReceiver<TopicReply>;
