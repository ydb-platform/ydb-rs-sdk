#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SessionMetaValue {
    #[prost(map="string, string", tag="1")]
    pub value: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
/// *
/// Represents range [start_offset, end_offset).
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OffsetsRange {
    #[prost(int64, tag="1")]
    pub start_offset: i64,
    #[prost(int64, tag="2")]
    pub end_offset: i64,
}
/// *
/// Request for write session. Contains one of:
///       InitRequest - handshake request.
///       WriteRequest - portion of data to be written.
///       UpdateTokenRequest - user credentials if update is needed.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamingWriteClientMessage {
    #[prost(oneof="streaming_write_client_message::ClientMessage", tags="1, 2, 3")]
    pub client_message: ::core::option::Option<streaming_write_client_message::ClientMessage>,
}
/// Nested message and enum types in `StreamingWriteClientMessage`.
pub mod streaming_write_client_message {
    /// Handshake request that must be sent to server first.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct InitRequest {
        /// Path of topic to write to.
        #[prost(string, tag="1")]
        pub topic: ::prost::alloc::string::String,
        /// message group identifier of client data stream a.k.a. sourceId.
        #[prost(string, tag="2")]
        pub message_group_id: ::prost::alloc::string::String,
        /// Some user metadata attached to this write session.
        #[prost(map="string, string", tag="3")]
        pub session_meta: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
        /// Partition group to write to.
        /// Zero means any group.
        #[prost(int64, tag="4")]
        pub partition_group_id: i64,
        #[prost(int64, tag="5")]
        pub max_supported_block_format_version: i64,
        #[prost(string, tag="100")]
        pub session_id: ::prost::alloc::string::String,
        /// 0 for first init message and incremental value for connect retries. Used for server logging.
        #[prost(int64, tag="101")]
        pub connection_attempt: i64,
        /// Opaque blob. Take last one from previous connect.
        #[prost(bytes="vec", tag="102")]
        pub connection_meta: ::prost::alloc::vec::Vec<u8>,
        /// Optinal preferred cluster name. Sever will close session If preferred cluster is not server cluster and preferred cluster is enabled after delay TPQConfig::CloseClientSessionWithEnabledRemotePreferredClusterDelaySec
        #[prost(string, tag="103")]
        pub preferred_cluster: ::prost::alloc::string::String,
        /// Sanity check option. When no writing activity is done in idle_timeout_sec seconds, then session will be destroyed. Zero means infinity.
        #[prost(int64, tag="200")]
        pub idle_timeout_ms: i64,
    }
    /// Represents portion of client messages.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct WriteRequest {
        /// Sequence numbers of messages in order that client will provide to server.
        #[prost(int64, repeated, tag="2")]
        pub sequence_numbers: ::prost::alloc::vec::Vec<i64>,
        /// Message creation timestamps for client messages.
        /// Same size as sequence_numbers.
        #[prost(int64, repeated, tag="3")]
        pub created_at_ms: ::prost::alloc::vec::Vec<i64>,
        /// Message creation timestamps for client messages.
        /// Same size as sequence_numbers.
        #[prost(int64, repeated, tag="4")]
        pub sent_at_ms: ::prost::alloc::vec::Vec<i64>,
        /// Client message sizes.
        /// Same size as sequence_numbers.
        #[prost(int64, repeated, tag="5")]
        pub message_sizes: ::prost::alloc::vec::Vec<i64>,
        // Block must contain whole client message when it's size is not bigger that max_block_size.
        // If message is bigger than max_block_size - it will be transferred as SIZE/max_block_size blocks. All of
        // this blocks will be with block_count = 0 but not the last one - last one's block_count will be greater than 0;
        // Blocks can be reordered upto max_flush_window_size of uncompressed data.
        // Each block contains concatenated client messages, compressed by chosen codec.
        // If there is not full client message inside block, then all block contains only this part of message.
        // blocks:      A A A B B B BCDE
        // offset:      1 1 1 2 2 2 2
        // part_number: 0 1 2 0 1 2 3
        // count:       0 0 1 0 0 1 4

        #[prost(int64, repeated, tag="6")]
        pub blocks_offsets: ::prost::alloc::vec::Vec<i64>,
        #[prost(int64, repeated, tag="7")]
        pub blocks_part_numbers: ::prost::alloc::vec::Vec<i64>,
        /// How many complete messages and imcomplete messages end parts (one at most) this block contains
        #[prost(int64, repeated, tag="8")]
        pub blocks_message_counts: ::prost::alloc::vec::Vec<i64>,
        #[prost(int64, repeated, tag="9")]
        pub blocks_uncompressed_sizes: ::prost::alloc::vec::Vec<i64>,
        /// In block format version 0 each byte contains only block codec identifier
        #[prost(bytes="vec", repeated, tag="10")]
        pub blocks_headers: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
        #[prost(bytes="vec", repeated, tag="11")]
        pub blocks_data: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    }
    /// In-session reauthentication and reauthorization, lets user increase session lifetime. You should wait for 'update_token_response' before sending next 'update_token_request'.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct UpdateTokenRequest {
        #[prost(string, tag="1")]
        pub token: ::prost::alloc::string::String,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ClientMessage {
        #[prost(message, tag="1")]
        InitRequest(InitRequest),
        #[prost(message, tag="2")]
        WriteRequest(WriteRequest),
        #[prost(message, tag="3")]
        UpdateTokenRequest(UpdateTokenRequest),
    }
}
/// *
/// Response for write session. Contains one of:
///       InitResponse - correct handshake response.
///       BatchWriteResponse - acknowledgment of storing client messages.
///       UpdateTokenResponse - acknowledgment of reauthentication and reauthorization.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamingWriteServerMessage {
    /// Server status of response.
    #[prost(enumeration="super::super::status_ids::StatusCode", tag="1")]
    pub status: i32,
    /// Issues if any.
    #[prost(message, repeated, tag="2")]
    pub issues: ::prost::alloc::vec::Vec<super::super::issue::IssueMessage>,
    #[prost(oneof="streaming_write_server_message::ServerMessage", tags="3, 4, 5")]
    pub server_message: ::core::option::Option<streaming_write_server_message::ServerMessage>,
}
/// Nested message and enum types in `StreamingWriteServerMessage`.
pub mod streaming_write_server_message {
    /// Response for handshake.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct InitResponse {
        /// Last persisted message's sequence number for this message group.
        #[prost(int64, tag="1")]
        pub last_sequence_number: i64,
        /// Unique identifier of write session. Used for debug purposes.
        #[prost(string, tag="2")]
        pub session_id: ::prost::alloc::string::String,
        /// Path of topic that matched for this write session. Used for debug purposes, will be the same as in Init request from client.
        #[prost(string, tag="3")]
        pub topic: ::prost::alloc::string::String,
        /// Write session is established to this cluster. Client data will be in instance of topic in this cluster.
        #[prost(string, tag="4")]
        pub cluster: ::prost::alloc::string::String,
        /// Identifier of partition that is matched for this write session.
        #[prost(int64, tag="5")]
        pub partition_id: i64,
        /// Block (see StreamingWriteClientMessage.WriteRequest.blocks_data) format version supported by server or configured for a topic. Client must write data only with them.
        #[prost(int64, tag="6")]
        pub block_format_version: i64,
        /// Client can only use compression codecs from this set to write messages to topic, session will be closed with BAD_REQUEST otherwise.
        #[prost(enumeration="super::Codec", repeated, tag="10")]
        pub supported_codecs: ::prost::alloc::vec::Vec<i32>,
        /// Maximal flush window size choosed by server. Size of uncompressed data not sended to server must not be bigger than flush window size.
        /// In other words, this is maximal size of gap inside uncompressed data, which is not sended to server yet.
        /// will be 2048kb
        #[prost(int64, tag="7")]
        pub max_flush_window_size: i64,
        /// How big blocks per stream could be(in uncompressed size). When block contains more than max_block_size of uncompressed data - block must be truncated.
        /// will be 512kb
        #[prost(int64, tag="8")]
        pub max_block_size: i64,
        /// Opaque blob, used for fast reconnects.
        #[prost(bytes="vec", tag="9")]
        pub connection_meta: ::prost::alloc::vec::Vec<u8>,
    }
    /// Message that represents acknowledgment for sequence of client messages. This sequence is persisted together so write statistics is for messages batch.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct BatchWriteResponse {
        /// Sequence numbers of persisted client messages.
        #[prost(int64, repeated, tag="1")]
        pub sequence_numbers: ::prost::alloc::vec::Vec<i64>,
        /// Assigned partition offsets.
        /// Zero for skipped messages.
        #[prost(int64, repeated, tag="2")]
        pub offsets: ::prost::alloc::vec::Vec<i64>,
        /// Per message flag. False if message is written for the first time and True otherwise.
        #[prost(bool, repeated, tag="3")]
        pub already_written: ::prost::alloc::vec::Vec<bool>,
        /// Assigned partition for all client messages inside this batch.
        #[prost(int64, tag="4")]
        pub partition_id: i64,
        /// Write statistics for this sequence of client messages.
        #[prost(message, optional, tag="5")]
        pub write_statistics: ::core::option::Option<WriteStatistics>,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct UpdateTokenResponse {
    }
    /// Message with write statistics.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct WriteStatistics {
        /// Time spent in persisting of data.
        #[prost(int64, tag="1")]
        pub persist_duration_ms: i64,
        /// Time spent in queue before persisting.
        #[prost(int64, tag="2")]
        pub queued_in_partition_duration_ms: i64,
        /// Time spent awaiting for partition write quota.
        #[prost(int64, tag="3")]
        pub throttled_on_partition_duration_ms: i64,
        /// Time spent awaiting for topic write quota.
        #[prost(int64, tag="4")]
        pub throttled_on_topic_duration_ms: i64,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ServerMessage {
        #[prost(message, tag="3")]
        InitResponse(InitResponse),
        #[prost(message, tag="4")]
        BatchWriteResponse(BatchWriteResponse),
        #[prost(message, tag="5")]
        UpdateTokenResponse(UpdateTokenResponse),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Path {
    /// Path of object (topic/consumer).
    #[prost(string, tag="1")]
    pub path: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyValue {
    #[prost(string, tag="1")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub value: ::prost::alloc::string::String,
}
/// *
/// Single read parameters for server.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadParams {
    /// Max messages to give to client in one read request.
    #[prost(uint32, tag="1")]
    pub max_read_messages_count: u32,
    /// Max size in bytes to give to client in one read request.
    #[prost(uint32, tag="2")]
    pub max_read_size: u32,
}
/// *
/// Message that is used for addressing read for commiting.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitCookie {
    /// Assign identitifier of assigned partition from which read was done.
    #[prost(uint64, tag="1")]
    pub assign_id: u64,
    /// Incremental identifier of concrete partition read batch.
    #[prost(uint64, tag="2")]
    pub partition_cookie: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitOffsetRange {
    #[prost(uint64, tag="1")]
    pub assign_id: u64,
    #[prost(uint64, tag="2")]
    pub start_offset: u64,
    #[prost(uint64, tag="3")]
    pub end_offset: u64,
}
// TODO: replace with it actual protocol client message

/// *
/// Request for read session. Contains one of:
///       InitRequest - handshake request.
///       ReadRequest - request for data.
///       CommitRequest - request for commit of some read data.
///       CreatePartitionStreamResponse - signal for server that client is ready to get data from partition.
///       DestroyPartitionStreamResponse - signal for server that client finished working with partition. Must be sent only after corresponding Release request from server.
///       StopReadRequest - signal for server that client is not ready to get more data from this partition.
///       ResumeReadRequest - signal for server that client is ready to get more data from this partition.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamingReadClientMessageNew {
    /// User credentials if update is needed or empty string.
    #[prost(string, tag="20")]
    pub token: ::prost::alloc::string::String,
    #[prost(oneof="streaming_read_client_message_new::ClientMessage", tags="1, 2, 3, 4, 5, 6, 7, 8, 9, 10")]
    pub client_message: ::core::option::Option<streaming_read_client_message_new::ClientMessage>,
}
/// Nested message and enum types in `StreamingReadClientMessageNew`.
pub mod streaming_read_client_message_new {
    /// Handshake request.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct InitRequest {
        /// Message that describes topic to read.
        /// Topics that will be read by this session.
        #[prost(message, repeated, tag="1")]
        pub topics_read_settings: ::prost::alloc::vec::Vec<TopicReadSettings>,
        /// Flag that indicates reading only of original topics in cluster or all including mirrored.
        #[prost(bool, tag="2")]
        pub read_only_original: bool,
        /// Path of consumer that is used for reading by this session.
        #[prost(string, tag="3")]
        pub consumer: ::prost::alloc::string::String,
        /// Skip all messages that has write timestamp smaller than now - max_time_lag_ms.
        #[prost(int64, tag="4")]
        pub max_lag_duration_ms: i64,
        /// Read data only after this timestamp from all topics.
        #[prost(int64, tag="5")]
        pub start_from_written_at_ms: i64,
        /// Maximum block format version supported by the client. Server will asses this parameter and return actual data blocks version in
        /// StreamingReadServerMessage.InitResponse.block_format_version_by_topic (and StreamingReadServerMessage.AddTopicResponse.block_format_version)
        /// or error if client will not be able to read data.
        #[prost(int64, tag="6")]
        pub max_supported_block_format_version: i64,
        /// Maximal size of client cache for message_group_id, ip and meta, per partition.
        /// There is separate caches for each partition partition streams.
        /// There is separate caches for message group identifiers, ip and meta inside one partition partition stream.
        #[prost(int64, tag="10")]
        pub max_meta_cache_size: i64,
        /// Session identifier for retries. Must be the same as session_id from Inited server response. If this is first connect, not retry - do not use this field.
        #[prost(string, tag="100")]
        pub session_id: ::prost::alloc::string::String,
        /// 0 for first init message and incremental value for connect retries.
        #[prost(int64, tag="101")]
        pub connection_attempt: i64,
        /// Formed state for retries. If not retry - do not use this field.
        #[prost(message, optional, tag="102")]
        pub state: ::core::option::Option<init_request::State>,
        #[prost(int64, tag="200")]
        pub idle_timeout_ms: i64,
    }
    /// Nested message and enum types in `InitRequest`.
    pub mod init_request {
        /// State of client read session. Could be provided to server for retries.
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct State {
            #[prost(message, repeated, tag="1")]
            pub partition_streams_states: ::prost::alloc::vec::Vec<state::PartitionStreamState>,
        }
        /// Nested message and enum types in `State`.
        pub mod state {
            #[derive(serde::Serialize, serde::Deserialize)]
            #[derive(Clone, PartialEq, ::prost::Message)]
            pub struct PartitionStreamState {
                /// Partition partition stream.
                #[prost(message, optional, tag="1")]
                pub partition_stream: ::core::option::Option<super::super::super::PartitionStream>,
                /// Current read offset if has one. Actual for states DESTROYING, READING and STOPPED.
                #[prost(int64, tag="2")]
                pub read_offset: i64,
                /// Ranges of committed by client offsets.
                #[prost(message, repeated, tag="3")]
                pub offset_ranges: ::prost::alloc::vec::Vec<super::super::super::OffsetsRange>,
                /// Status of partition stream.
                #[prost(enumeration="partition_stream_state::Status", tag="4")]
                pub status: i32,
            }
            /// Nested message and enum types in `PartitionStreamState`.
            pub mod partition_stream_state {
                #[derive(serde::Serialize, serde::Deserialize)]
                #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
                #[repr(i32)]
                pub enum Status {
                    /// Not used state.
                    Unspecified = 0,
                    /// Client seen Create message but not yet responded to server with Created message.
                    Creating = 1,
                    /// Client seen Destroy message but not yet responded with Released message.
                    Destroying = 2,
                    /// Client sent Created or ResumeReadRequest message to server for this partition stream.
                    Reading = 3,
                    /// Client sent StopReadRequest for this partition stream.
                    Stopped = 4,
                }
                impl Status {
                    /// String value of the enum field names used in the ProtoBuf definition.
                    /// The values are not transformed in any way and thus are considered stable
                    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
                    pub fn as_str_name(&self) -> &'static str {
                        match self {
                            Status::Unspecified => "STATUS_UNSPECIFIED",
                            Status::Creating => "CREATING",
                            Status::Destroying => "DESTROYING",
                            Status::Reading => "READING",
                            Status::Stopped => "STOPPED",
                        }
                    }
                }
            }
        }
    }
    // TODO: add topics/groups and remove them from reading

    /// Message that represents client readiness for receiving more data.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ReadRequest {
        /// Client acquired this amount of free bytes more for buffer. Server can send more data at most of this uncompressed size.
        /// Subsequent messages with 5 and 10 request_uncompressed_size are treated by server that it can send messages for at most 15 bytes.
        #[prost(int64, tag="1")]
        pub request_uncompressed_size: i64,
    }
    /// Signal for server that cient is ready to recive data for partition.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct CreatePartitionStreamResponse {
        /// Partition stream identifier of partition to start read.
        #[prost(int64, tag="1")]
        pub partition_stream_id: i64,
        /// Start reading from partition offset that is not less than read_offset.
        /// Init.max_time_lag_ms and Init.read_timestamp_ms could lead to skip of more messages.
        /// The same with actual committed offset. Regardless of set read_offset server will return data from maximal offset from read_offset, actual committed offset
        /// and offsets calculated from Init.max_time_lag_ms and Init.read_timestamp_ms.
        #[prost(int64, tag="2")]
        pub read_offset: i64,
        /// All messages with offset less than commit_offset are processed by client. Server will commit this position if this is not done yet.
        #[prost(int64, tag="3")]
        pub commit_offset: i64,
        /// This option will enable sanity check on server for read_offset. Server will verify that read_offset is no less that actual committed offset.
        /// If verification will fail then server will kill this read session and client will find out error in reading logic.
        /// If client is not setting read_offset, sanity check will fail so do not set verify_read_offset if you not setting correct read_offset.
        #[prost(bool, tag="4")]
        pub verify_read_offset: bool,
    }
    /// Signal for server that client finished working with this partition. Must be sent only after corresponding Release request from server.
    /// Server will give this partition to other read session only after Released signal.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DestroyPartitionStreamResponse {
        /// Partition stream identifier of partition partition stream that is released by client.
        #[prost(int64, tag="1")]
        pub partition_stream_id: i64,
    }
    /// Signal for server that client is not ready to recieve more data from this partition.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StopReadRequest {
        #[prost(int64, repeated, tag="1")]
        pub partition_stream_ids: ::prost::alloc::vec::Vec<i64>,
    }
    /// Signal for server that client is ready to receive more data from this partition.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ResumeReadRequest {
        #[prost(int64, repeated, tag="1")]
        pub partition_stream_ids: ::prost::alloc::vec::Vec<i64>,
        /// Offset to start reading - may be smaller than known one in case of dropping of read-ahead in client lib.
        #[prost(int64, repeated, tag="2")]
        pub read_offsets: ::prost::alloc::vec::Vec<i64>,
        /// Cookie for matching data from PartitionStream after resuming. Must be greater than zero.
        #[prost(int64, repeated, tag="3")]
        pub resume_cookies: ::prost::alloc::vec::Vec<i64>,
    }
    /// Signal for server that client processed some read data.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct CommitRequest {
        /// Partition offsets that indicates processed data.
        #[prost(message, repeated, tag="1")]
        pub commits: ::prost::alloc::vec::Vec<PartitionCommit>,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct PartitionStreamStatusRequest {
        #[prost(int64, tag="1")]
        pub partition_stream_id: i64,
    }
    /// Add topic to current read session
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct AddTopicRequest {
        #[prost(message, optional, tag="1")]
        pub topic_read_settings: ::core::option::Option<TopicReadSettings>,
    }
    /// Remove topic from current read session
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct RemoveTopicRequest {
        #[prost(string, tag="1")]
        pub topic: ::prost::alloc::string::String,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct TopicReadSettings {
        /// Topic path.
        #[prost(string, tag="1")]
        pub topic: ::prost::alloc::string::String,
        /// Partition groups that will be read by this session.
        /// If list is empty - then session will read all partition groups.
        #[prost(int64, repeated, tag="2")]
        pub partition_group_ids: ::prost::alloc::vec::Vec<i64>,
        /// Read data only after this timestamp from this topic.
        #[prost(int64, tag="3")]
        pub start_from_written_at_ms: i64,
    }
    /// *
    /// Message that is used for describing commit.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct PartitionCommit {
        /// Identifier of partition stream with data to commit.
        #[prost(int64, tag="1")]
        pub partition_stream_id: i64,
        /// Processed ranges.
        #[prost(message, repeated, tag="2")]
        pub offsets: ::prost::alloc::vec::Vec<super::OffsetsRange>,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ClientMessage {
        #[prost(message, tag="1")]
        InitRequest(InitRequest),
        #[prost(message, tag="2")]
        ReadRequest(ReadRequest),
        #[prost(message, tag="3")]
        CreatePartitionStreamResponse(CreatePartitionStreamResponse),
        #[prost(message, tag="4")]
        CommitRequest(CommitRequest),
        #[prost(message, tag="5")]
        DestroyPartitionStreamResponse(DestroyPartitionStreamResponse),
        #[prost(message, tag="6")]
        StopReadRequest(StopReadRequest),
        #[prost(message, tag="7")]
        ResumeReadRequest(ResumeReadRequest),
        #[prost(message, tag="8")]
        PartitionStreamStatusRequest(PartitionStreamStatusRequest),
        #[prost(message, tag="9")]
        AddTopicRequest(AddTopicRequest),
        #[prost(message, tag="10")]
        RemoveTopicRequest(RemoveTopicRequest),
    }
}
// TODO: replace with it actual protocol server message

/// *
/// Response for read session. Contains one of :
///       InitResponse - handshake response from server.
///       BatchReadResponse - portion of data.
///       CommitResponse - acknowledgment for commit.
///       CreatePartitionStreamRequest - command from server to create a partition partition stream.
///       DestroyPartitionStreamRequest - command from server to destroy a partition partition stream.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamingReadServerMessageNew {
    #[prost(enumeration="super::super::status_ids::StatusCode", tag="1")]
    pub status: i32,
    #[prost(message, repeated, tag="2")]
    pub issues: ::prost::alloc::vec::Vec<super::super::issue::IssueMessage>,
    #[prost(oneof="streaming_read_server_message_new::ServerMessage", tags="3, 4, 5, 6, 7, 8, 9, 10, 11, 12")]
    pub server_message: ::core::option::Option<streaming_read_server_message_new::ServerMessage>,
}
/// Nested message and enum types in `StreamingReadServerMessageNew`.
pub mod streaming_read_server_message_new {
    /// Handshake response.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct InitResponse {
        /// Read session identifier for debug purposes.
        #[prost(string, tag="1")]
        pub session_id: ::prost::alloc::string::String,
        /// Block format version of data client will receive from topics.
        #[prost(map="string, int64", tag="2")]
        pub block_format_version_by_topic: ::std::collections::HashMap<::prost::alloc::string::String, i64>,
        /// Choosed maximan cache size by server.
        /// Client must use cache of this size. Could change on retries - reduce size of cache in this case.
        #[prost(int64, tag="10")]
        pub max_meta_cache_size: i64,
    }
    /// Command to create a partition partition stream.
    /// Client must react on this signal by sending StartRead when ready recieve data from this partition.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct CreatePartitionStreamRequest {
        /// Partition partition stream description.
        #[prost(message, optional, tag="1")]
        pub partition_stream: ::core::option::Option<super::PartitionStream>,
        /// Actual committed offset.
        #[prost(int64, tag="2")]
        pub committed_offset: i64,
        /// Offset of first not existing message in partition till now.
        #[prost(int64, tag="3")]
        pub end_offset: i64,
    }
    /// Command to destroy concrete partition stream.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DestroyPartitionStreamRequest {
        /// Identifier of partition partition stream that is ready to be closed by server.
        #[prost(int64, tag="1")]
        pub partition_stream_id: i64,
        /// Flag of gracefull or not destroy.
        /// If True then server is waiting for Destroyed signal from client before giving of this partition for other read session.
        /// Server will not send more data from this partition.
        /// Client can process all received data and wait for commit and only after send Destroyed signal.
        /// If False then server gives partition for other session right now.
        /// All futher commits for this PartitionStream has no effect. Server is not waiting for Destroyed signal.
        #[prost(bool, tag="2")]
        pub graceful: bool,
        /// Last known committed offset.
        #[prost(int64, tag="3")]
        pub committed_offset: i64,
    }
    /// Acknowledgement for commits.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct CommitResponse {
        /// Partitions with progress.
        #[prost(message, repeated, tag="1")]
        pub partitions_committed_offsets: ::prost::alloc::vec::Vec<commit_response::PartitionCommittedOffset>,
    }
    /// Nested message and enum types in `CommitResponse`.
    pub mod commit_response {
        /// Per-partition commit representation.
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct PartitionCommittedOffset {
            /// Partition partition stream identifier.
            #[prost(int64, tag="1")]
            pub partition_stream_id: i64,
            /// Last committed offset.
            #[prost(int64, tag="2")]
            pub committed_offset: i64,
        }
    }
    /// Readed data.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct BatchReadResponse {
        #[prost(message, repeated, tag="1")]
        pub skip_range: ::prost::alloc::vec::Vec<batch_read_response::SkipRange>,
        /// Per-partition data.
        #[prost(message, repeated, tag="2")]
        pub partitions: ::prost::alloc::vec::Vec<batch_read_response::PartitionData>,
    }
    /// Nested message and enum types in `BatchReadResponse`.
    pub mod batch_read_response {
        /// One client message representation.
        /// Client lib must send commit right now for all skipped data (from it's read offset till first offset in range).
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct PartitionData {
            /// Data inside this message is from partition stream with this identifier.
            #[prost(int64, tag="1")]
            pub partition_stream_id: i64,
            /// Offsets in partition that assigned for messages.
            /// Unique value for clientside deduplication - (topic, cluster, partition_id, offset).
            #[prost(int64, repeated, tag="2")]
            pub offsets: ::prost::alloc::vec::Vec<i64>,
            /// Sequence numbers that provided with messages on write from client.
            /// Same size as offsets.
            /// Unique value for clientside deduplication - (topic, cluster, message_group_id, sequence_number).
            #[prost(int64, repeated, tag="3")]
            pub sequence_numbers: ::prost::alloc::vec::Vec<i64>,
            /// Timestamps of creation of messages provided on write from client.
            /// Same size as offsets.
            #[prost(int64, repeated, tag="4")]
            pub created_at_ms: ::prost::alloc::vec::Vec<i64>,
            /// Timestamps of writing in partition for client messages.
            /// Same size as offsets.
            #[prost(int64, repeated, tag="5")]
            pub written_at_ms: ::prost::alloc::vec::Vec<i64>,
            /// New messageGroupIds for updating cache.
            /// Size of vector is the same as number of negative values in message_group_id_indexes.
            #[prost(string, repeated, tag="6")]
            pub message_group_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
            /// Indexes of messageGroupIds.
            /// same size as offsets.
            /// Negative values (-X) means - put first not used messageGroupId from message_group_ids on index X in cache and use it for this client message.
            /// Positive values (X) means -use element by index X from cache for this client message. Do not change state of cache.
            /// Assumptions:
            ///       - Server will use positive values only for proposed before indexes.
            ///       - Each value is from 1 to max_meta_cache_size by abs.
            ///       - Do not make assumptions about choosing algorihm.
            ///       - There is separate caches of size max_meta_cache_size for different partition and different metadata fileds - message_group_id, ip and session_meta_data.
            ///       - Number of negative values in message_group_id_indexes vector is the same as length of message_group_ids vector.
            /// Example:
            ///                 max_meta_cache_size :  2
            ///                       Cache indexes :  1  2
            ///       Cache state before processing :  s0,? // ? means not set yet.
            ///                   message_group_ids :  s1 s2 s3 s1
            ///            message_group_id_indexes :  -1    -2    1     2     1     1     -1    2     -2
            ///                         cache state :  s1,?  s1,s2 s1,s2 s1,s2 s1,s2 s1,s2 s3,s2 s3,s2 s3,s1
            ///              real message group ids :  s1    s2    s1    s2    s1    s1    s3    s2    s1
            ///                       Cache indexes :  1  2
            ///        Cache state after processing :  s3,s1
            #[prost(sint64, repeated, tag="7")]
            pub message_group_id_indexes: ::prost::alloc::vec::Vec<i64>,
            /// New ips for updating ip cache.
            #[prost(string, repeated, tag="8")]
            pub ips: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
            /// Same as message_group_id_indexes but for ips.
            #[prost(sint64, repeated, tag="9")]
            pub ip_indexes: ::prost::alloc::vec::Vec<i64>,
            /// New session meta datas for updating cache.
            #[prost(message, repeated, tag="10")]
            pub message_session_meta: ::prost::alloc::vec::Vec<super::super::SessionMetaValue>,
            /// Same as message_group_id_indexes but for session meta data.
            #[prost(sint64, repeated, tag="11")]
            pub message_session_meta_indexes: ::prost::alloc::vec::Vec<i64>,
            /// Client messages sizes.
            /// Same size as offsets.
            #[prost(int64, repeated, tag="12")]
            pub message_sizes: ::prost::alloc::vec::Vec<i64>,
            /// Block must contain whole client message when it's size is not bigger that max_block_size.
            /// If message is bigger than max_block_size - it will be transferred as SIZE/max_block_size blocks. All of this blocks will be with block_count = 0 but not the last one - last one's block_count will be 0;
            /// Blocks can be reordered upto provided by client uncompressed free buffer size.
            /// blocks:      A A A B B B CDE
            /// offset:      1 1 1 4 4 4 6
            /// part_number: 0 1 2 0 1 2 0
            /// count:       0 0 1 0 0 1 3
            /// Offset will be the same as in Offsets.
            #[prost(int64, repeated, tag="13")]
            pub blocks_offsets: ::prost::alloc::vec::Vec<i64>,
            #[prost(int64, repeated, tag="14")]
            pub blocks_part_numbers: ::prost::alloc::vec::Vec<i64>,
            /// How many complete messages and imcomplete messages end parts (one at most) this block contains
            #[prost(int64, repeated, tag="15")]
            pub blocks_message_counts: ::prost::alloc::vec::Vec<i64>,
            #[prost(int64, repeated, tag="16")]
            pub blocks_uncompressed_sizes: ::prost::alloc::vec::Vec<i64>,
            /// In block format version 0 each byte contains only block codec identifier
            #[prost(bytes="vec", repeated, tag="17")]
            pub blocks_headers: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
            #[prost(bytes="vec", repeated, tag="18")]
            pub blocks_data: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
            /// Zero if this is not first portion of data after resume or provided by client cookie otherwise.
            #[prost(int64, tag="50")]
            pub resume_cookie: i64,
            #[prost(message, optional, tag="100")]
            pub read_statistics: ::core::option::Option<partition_data::ReadStatistics>,
        }
        /// Nested message and enum types in `PartitionData`.
        pub mod partition_data {
            #[derive(serde::Serialize, serde::Deserialize)]
            #[derive(Clone, PartialEq, ::prost::Message)]
            pub struct ReadStatistics {
                #[prost(int64, tag="1")]
                pub blobs_from_cache: i64,
                #[prost(int64, tag="2")]
                pub blobs_from_disk: i64,
                #[prost(int64, tag="3")]
                pub bytes_from_head: i64,
                #[prost(int64, tag="4")]
                pub bytes_from_cache: i64,
                #[prost(int64, tag="5")]
                pub bytes_from_disk: i64,
                #[prost(int64, tag="6")]
                pub repack_duration_ms: i64,
            }
        }
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct SkipRange {
            /// Partition Stream identifier.
            #[prost(int64, tag="1")]
            pub partition_stream_id: i64,
            /// When some data is skipped by client parameters (read_timestamp_ms for example) then range of skipped offsets is sended to client.
            /// Client lib must commit this range and change read_offset to end of this range.
            #[prost(message, optional, tag="2")]
            pub skip_range: ::core::option::Option<super::super::OffsetsRange>,
        }
    }
    /// Response for status requst.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct PartitionStreamStatusResponse {
        /// Identifier of partition partition stream that is ready to be closed by server.
        #[prost(int64, tag="1")]
        pub partition_stream_id: i64,
        #[prost(int64, tag="2")]
        pub committed_offset: i64,
        #[prost(int64, tag="3")]
        pub end_offset: i64,
        /// WriteTimestamp of next message (and end_offset) will be not less that WriteWatermarkMs.
        #[prost(int64, tag="4")]
        pub written_at_watermark_ms: i64,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StopReadResponse {
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ResumeReadResponse {
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct AddTopicResponse {
        /// Block format version of data client will receive from the topic.
        #[prost(int64, tag="1")]
        pub block_format_version: i64,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct RemoveTopicResponse {
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ServerMessage {
        #[prost(message, tag="3")]
        InitResponse(InitResponse),
        #[prost(message, tag="4")]
        BatchReadResponse(BatchReadResponse),
        #[prost(message, tag="5")]
        CreatePartitionStreamRequest(CreatePartitionStreamRequest),
        #[prost(message, tag="6")]
        DestroyPartitionStreamRequest(DestroyPartitionStreamRequest),
        #[prost(message, tag="7")]
        CommitResponse(CommitResponse),
        #[prost(message, tag="8")]
        PartitionStreamStatusResponse(PartitionStreamStatusResponse),
        #[prost(message, tag="9")]
        StopReadResponse(StopReadResponse),
        #[prost(message, tag="10")]
        ResumeReadResponse(ResumeReadResponse),
        #[prost(message, tag="11")]
        AddTopicResponse(AddTopicResponse),
        #[prost(message, tag="12")]
        RemoveTopicResponse(RemoveTopicResponse),
    }
}
/// *
/// Message that represens concrete partition partition stream.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionStream {
    /// Topic path of partition.
    #[prost(string, tag="1")]
    pub topic: ::prost::alloc::string::String,
    /// Cluster of topic instance.
    #[prost(string, tag="2")]
    pub cluster: ::prost::alloc::string::String,
    /// Partition identifier. Explicit only for debug purposes.
    #[prost(int64, tag="3")]
    pub partition_id: i64,
    /// Partition group identifier. Explicit only for debug purposes.
    #[prost(int64, tag="4")]
    pub partition_group_id: i64,
    /// Identitifier of partition stream. Unique inside one RPC call.
    #[prost(int64, tag="6")]
    pub partition_stream_id: i64,
    /// Opaque blob. Provide it with partition stream in state for session reconnects.
    #[prost(bytes="vec", tag="7")]
    pub connection_meta: ::prost::alloc::vec::Vec<u8>,
}
// *
// Request for read session. Contains one of :
//       Init - handshake request.
//       Read - request for data.
//       Commit - request for commit of some read data.
//       Start_read - signal for server that client is ready to get data from partition.
//       Released - signal for server that client finished working with partition. Must be sent only after corresponding Release request from server.

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MigrationStreamingReadClientMessage {
    /// User credentials if update is needed or empty string.
    #[prost(bytes="vec", tag="20")]
    pub token: ::prost::alloc::vec::Vec<u8>,
    #[prost(oneof="migration_streaming_read_client_message::Request", tags="1, 2, 3, 4, 5, 6")]
    pub request: ::core::option::Option<migration_streaming_read_client_message::Request>,
}
/// Nested message and enum types in `MigrationStreamingReadClientMessage`.
pub mod migration_streaming_read_client_message {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct TopicReadSettings {
        /// Topic path.
        #[prost(string, tag="1")]
        pub topic: ::prost::alloc::string::String,
        /// Partition groups that will be read by this session.
        /// If list is empty - then session will read all partition groups.
        #[prost(int64, repeated, tag="2")]
        pub partition_group_ids: ::prost::alloc::vec::Vec<i64>,
        /// Read data only after this timestamp from this topic.
        #[prost(int64, tag="3")]
        pub start_from_written_at_ms: i64,
    }
    /// Handshake request.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct InitRequest {
        /// Message that describes topic to read.
        /// Topics that will be read by this session.
        #[prost(message, repeated, tag="1")]
        pub topics_read_settings: ::prost::alloc::vec::Vec<TopicReadSettings>,
        /// Flag that indicates reading only of original topics in cluster or all including mirrored.
        #[prost(bool, tag="2")]
        pub read_only_original: bool,
        /// Path of consumer that is used for reading by this session.
        #[prost(string, tag="3")]
        pub consumer: ::prost::alloc::string::String,
        /// Skip all messages that has write timestamp smaller than now - max_time_lag_ms.
        #[prost(int64, tag="4")]
        pub max_lag_duration_ms: i64,
        /// Read data only after this timestamp from all topics.
        #[prost(int64, tag="5")]
        pub start_from_written_at_ms: i64,
        /// Maximum block format version supported by the client. Server will asses this parameter and return actual data blocks version in
        /// StreamingReadServerMessage.InitResponse.block_format_version_by_topic (and StreamingReadServerMessage.AddTopicResponse.block_format_version)
        /// or error if client will not be able to read data.
        #[prost(int64, tag="6")]
        pub max_supported_block_format_version: i64,
        /// Maximal size of client cache for message_group_id, ip and meta, per partition.
        /// There is separate caches for each partition partition streams.
        /// There is separate caches for message group identifiers, ip and meta inside one partition partition stream.
        #[prost(int64, tag="10")]
        pub max_meta_cache_size: i64,
        /// Session identifier for retries. Must be the same as session_id from Inited server response. If this is first connect, not retry - do not use this field.
        #[prost(string, tag="100")]
        pub session_id: ::prost::alloc::string::String,
        /// 0 for first init message and incremental value for connect retries.
        #[prost(int64, tag="101")]
        pub connection_attempt: i64,
        /// Formed state for retries. If not retry - do not use this field.
        #[prost(message, optional, tag="102")]
        pub state: ::core::option::Option<init_request::State>,
        #[prost(int64, tag="200")]
        pub idle_timeout_ms: i64,
        /// //////////////////////////////////////////////////////////////////////////////////////////////////////////
        /// TODO: remove after refactoring
        /// Single read request params.
        #[prost(message, optional, tag="42")]
        pub read_params: ::core::option::Option<super::ReadParams>,
        /// //////////////////////////////////////////////////////////////////////////////////////////////////////////
        #[prost(bool, tag="442")]
        pub ranges_mode: bool,
    }
    /// Nested message and enum types in `InitRequest`.
    pub mod init_request {
        /// State of client read session. Could be provided to server for retries.
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct State {
            #[prost(message, repeated, tag="1")]
            pub partition_streams_states: ::prost::alloc::vec::Vec<state::PartitionStreamState>,
        }
        /// Nested message and enum types in `State`.
        pub mod state {
            #[derive(serde::Serialize, serde::Deserialize)]
            #[derive(Clone, PartialEq, ::prost::Message)]
            pub struct PartitionStreamState {
                /// Partition partition stream.
                #[prost(message, optional, tag="1")]
                pub partition_stream: ::core::option::Option<super::super::super::PartitionStream>,
                /// Current read offset if has one. Actual for states DESTROYING, READING and STOPPED.
                #[prost(int64, tag="2")]
                pub read_offset: i64,
                /// Ranges of committed by client offsets.
                #[prost(message, repeated, tag="3")]
                pub offset_ranges: ::prost::alloc::vec::Vec<super::super::super::OffsetsRange>,
                /// Status of partition stream.
                #[prost(enumeration="partition_stream_state::Status", tag="4")]
                pub status: i32,
            }
            /// Nested message and enum types in `PartitionStreamState`.
            pub mod partition_stream_state {
                #[derive(serde::Serialize, serde::Deserialize)]
                #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
                #[repr(i32)]
                pub enum Status {
                    /// Not used state.
                    Unspecified = 0,
                    /// Client seen Create message but not yet responded to server with Created message.
                    Creating = 1,
                    /// Client seen Destroy message but not yet responded with Released message.
                    Destroying = 2,
                    /// Client sent Created or ResumeReadRequest message to server for this partition stream.
                    Reading = 3,
                    /// Client sent StopReadRequest for this partition stream.
                    Stopped = 4,
                }
                impl Status {
                    /// String value of the enum field names used in the ProtoBuf definition.
                    /// The values are not transformed in any way and thus are considered stable
                    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
                    pub fn as_str_name(&self) -> &'static str {
                        match self {
                            Status::Unspecified => "STATUS_UNSPECIFIED",
                            Status::Creating => "CREATING",
                            Status::Destroying => "DESTROYING",
                            Status::Reading => "READING",
                            Status::Stopped => "STOPPED",
                        }
                    }
                }
            }
        }
    }
    /// Request of single read.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Read {
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StartRead {
        /// Topic path of partition.
        #[prost(message, optional, tag="1")]
        pub topic: ::core::option::Option<super::Path>,
        /// Cluster of topic instance.
        #[prost(string, tag="2")]
        pub cluster: ::prost::alloc::string::String,
        /// Partition identifier. Explicit only for debug purposes.
        #[prost(uint64, tag="3")]
        pub partition: u64,
        /// Assign identifier of Assign request from server. Used for mathing Assign requests from server with StartRead responses from client.
        #[prost(uint64, tag="5")]
        pub assign_id: u64,
        /// Start reading from partition offset that is not less than read_offset.
        /// ReadParams.max_time_lag_ms and ReadParams.read_timestamp_ms could lead to skip of more messages.
        /// The same with actual committed offset. Regardless of set read_offset server will return data from maximal offset from read_offset, actual committed offset
        /// and offsets calculated from ReadParams.max_time_lag_ms and ReadParams.read_timestamp_ms.
        #[prost(uint64, tag="6")]
        pub read_offset: u64,
        /// All messages with offset less than commit_offset are processed by client. Server will commit this position if this is not done yet.
        #[prost(uint64, tag="7")]
        pub commit_offset: u64,
        /// This option will enable sanity check on server for read_offset. Server will verify that read_offset is no less that actual committed offset.
        /// If verification will fail then server will kill this read session and client will find out error in reading logic.
        /// If client is not setting read_offset, sanity check will fail so do not set verify_read_offset if you not setting correct read_offset.
        /// if true then check that committed position is <= ReadOffset; otherwise it means error in client logic
        #[prost(bool, tag="8")]
        pub verify_read_offset: bool,
    }
    /// Signal for server that client finished working with this partition. Must be sent only after corresponding Release request from server.
    /// Server will give this partition to other read session only after Released signal.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Released {
        /// Topic path of partition.
        #[prost(message, optional, tag="1")]
        pub topic: ::core::option::Option<super::Path>,
        /// Cluster of topic instance.
        #[prost(string, tag="2")]
        pub cluster: ::prost::alloc::string::String,
        /// Partition identifier. Explicit only for debug purposes.
        #[prost(uint64, tag="3")]
        pub partition: u64,
        /// Assign identifier of Assign request from server. Used for mathing Assign requests from server with Released responses from client.
        #[prost(uint64, tag="5")]
        pub assign_id: u64,
    }
    /// Signal for server that client processed some read data.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Commit {
        /// Partition read cookies that indicates processed data.
        #[prost(message, repeated, tag="1")]
        pub cookies: ::prost::alloc::vec::Vec<super::CommitCookie>,
        #[prost(message, repeated, tag="2")]
        pub offset_ranges: ::prost::alloc::vec::Vec<super::CommitOffsetRange>,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Status {
        /// Topic path of partition.
        #[prost(message, optional, tag="1")]
        pub topic: ::core::option::Option<super::Path>,
        /// Cluster of topic instance.
        #[prost(string, tag="2")]
        pub cluster: ::prost::alloc::string::String,
        /// Partition identifier. Explicit only for debug purposes.
        #[prost(uint64, tag="3")]
        pub partition: u64,
        /// Assign identifier of Assign request from server. Used for mathing Assign requests from server with Released responses from client.
        #[prost(uint64, tag="5")]
        pub assign_id: u64,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Request {
        #[prost(message, tag="1")]
        InitRequest(InitRequest),
        #[prost(message, tag="2")]
        Read(Read),
        #[prost(message, tag="3")]
        StartRead(StartRead),
        #[prost(message, tag="4")]
        Commit(Commit),
        #[prost(message, tag="5")]
        Released(Released),
        #[prost(message, tag="6")]
        Status(Status),
    }
}
// *
// Response for read session. Contains one of :
//       Inited - handshake response from server.
//       Batched_data - result of single read.
//       Committed - acknowledgment for commit.
//       Assigned - signal from server for assigning of partition.
//       Release - signal from server for releasing of partition.

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MigrationStreamingReadServerMessage {
    #[prost(enumeration="super::super::status_ids::StatusCode", tag="1")]
    pub status: i32,
    #[prost(message, repeated, tag="2")]
    pub issues: ::prost::alloc::vec::Vec<super::super::issue::IssueMessage>,
    #[prost(oneof="migration_streaming_read_server_message::Response", tags="3, 4, 5, 6, 7, 8")]
    pub response: ::core::option::Option<migration_streaming_read_server_message::Response>,
}
/// Nested message and enum types in `MigrationStreamingReadServerMessage`.
pub mod migration_streaming_read_server_message {
    /// Handshake response.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct InitResponse {
        /// Read session identifier for debug purposes.
        #[prost(string, tag="1")]
        pub session_id: ::prost::alloc::string::String,
        /// Block format version of data client will receive from topics.
        #[prost(map="string, int64", tag="2")]
        pub block_format_version_by_topic: ::std::collections::HashMap<::prost::alloc::string::String, i64>,
        /// Choosed maximan cache size by server.
        /// Client must use cache of this size. Could change on retries - reduce size of cache in this case.
        #[prost(int64, tag="10")]
        pub max_meta_cache_size: i64,
    }
    /// Signal that partition is assigned to this read session. Client must react on this signal by sending StartRead when ready.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Assigned {
        /// Partition's topic path.
        #[prost(message, optional, tag="1")]
        pub topic: ::core::option::Option<super::Path>,
        /// Topic's instance cluster name.
        #[prost(string, tag="2")]
        pub cluster: ::prost::alloc::string::String,
        /// Partition identifier. topic:cluster:partition is unique addressing of partition.
        #[prost(uint64, tag="3")]
        pub partition: u64,
        /// Assign idenfier. Is not unique between diffrent partitions. Used for matching Assigned request from server and StartRead response from client.
        #[prost(uint64, tag="5")]
        pub assign_id: u64,
        /// Actual read offset. Equeal to last committed offset.
        #[prost(uint64, tag="6")]
        pub read_offset: u64,
        /// Offset of first not existing message in partition at this time.
        #[prost(uint64, tag="7")]
        pub end_offset: u64,
    }
    /// Partition release request from server.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Release {
        /// Partition's topic path.
        #[prost(message, optional, tag="1")]
        pub topic: ::core::option::Option<super::Path>,
        /// Topic's instance cluster name.
        #[prost(string, tag="2")]
        pub cluster: ::prost::alloc::string::String,
        /// Partition identifier. topic:cluster:partition is unique addressing of partition.
        #[prost(uint64, tag="3")]
        pub partition: u64,
        /// Assign idenfier. Used for matching Assigned and Release requests from server.
        #[prost(uint64, tag="5")]
        pub assign_id: u64,
        /// If False then server is waiting for Released signal from client before giving of this partition for other read session.
        /// If True then server gives partition for other session right now. All futher commits for this partition has no effect. Server is not waiting for Released signal.
        #[prost(bool, tag="6")]
        pub forceful_release: bool,
        /// Last known committed offset.
        #[prost(uint64, tag="7")]
        pub commit_offset: u64,
    }
    /// Acknowledgement for commits.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Committed {
        /// List of cookies that correspond to commit of processing read data.
        #[prost(message, repeated, tag="1")]
        pub cookies: ::prost::alloc::vec::Vec<super::CommitCookie>,
        #[prost(message, repeated, tag="2")]
        pub offset_ranges: ::prost::alloc::vec::Vec<super::CommitOffsetRange>,
    }
    /// Readed data.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DataBatch {
        /// Client messages, divided by partitions.
        #[prost(message, repeated, tag="1")]
        pub partition_data: ::prost::alloc::vec::Vec<data_batch::PartitionData>,
    }
    /// Nested message and enum types in `DataBatch`.
    pub mod data_batch {
        /// One client message representation.
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct MessageData {
            /// Partition offset in partition that assigned for message.
            /// unique value for clientside deduplication - Topic:Cluster:Partition:Offset
            #[prost(uint64, tag="1")]
            pub offset: u64,
            /// Sequence number that provided with message on write from client.
            #[prost(uint64, tag="2")]
            pub seq_no: u64,
            /// Timestamp of creation of message provided on write from client.
            #[prost(uint64, tag="3")]
            pub create_timestamp_ms: u64,
            /// Codec that is used for data compressing.
            #[prost(enumeration="super::super::Codec", tag="4")]
            pub codec: i32,
            /// Compressed client message body.
            #[prost(bytes="vec", tag="5")]
            pub data: ::prost::alloc::vec::Vec<u8>,
            /// Uncompressed size of client message body.
            #[prost(uint64, tag="6")]
            pub uncompressed_size: u64,
            /// kinesis data
            #[prost(string, tag="7")]
            pub partition_key: ::prost::alloc::string::String,
            #[prost(bytes="vec", tag="8")]
            pub explicit_hash: ::prost::alloc::vec::Vec<u8>,
        }
        /// Representation of sequence of client messages from one write session.
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Batch {
            /// Source identifier provided by client for this batch of client messages.
            #[prost(bytes="vec", tag="2")]
            pub source_id: ::prost::alloc::vec::Vec<u8>,
            /// Client metadata attached to write session, the same for all messages in batch.
            #[prost(message, repeated, tag="3")]
            pub extra_fields: ::prost::alloc::vec::Vec<super::super::KeyValue>,
            /// Persist timestamp on server for batch.
            #[prost(uint64, tag="4")]
            pub write_timestamp_ms: u64,
            /// Peer address of node that created write session.
            #[prost(string, tag="5")]
            pub ip: ::prost::alloc::string::String,
            /// List of client messages.
            #[prost(message, repeated, tag="1")]
            pub message_data: ::prost::alloc::vec::Vec<MessageData>,
        }
        /// Representation of sequence of messages from one partition.
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct PartitionData {
            /// Partition's topic path.
            #[prost(message, optional, tag="1")]
            pub topic: ::core::option::Option<super::super::Path>,
            /// Topic's instance cluster name.
            #[prost(string, tag="2")]
            pub cluster: ::prost::alloc::string::String,
            /// Partition identifier. topic:cluster:partition is unique addressing for partition.
            #[prost(uint64, tag="3")]
            pub partition: u64,
            /// Client messages, divided by write sessions.
            #[prost(message, repeated, tag="4")]
            pub batches: ::prost::alloc::vec::Vec<Batch>,
            /// Cookie for addressing this partition messages batch for committing.
            #[prost(message, optional, tag="5")]
            pub cookie: ::core::option::Option<super::super::CommitCookie>,
            /// Old formatted topic name with cluster inside.
            #[prost(string, tag="10")]
            pub deprecated_topic: ::prost::alloc::string::String,
        }
    }
    /// Response for status requst.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct PartitionStatus {
        /// Partition's topic path.
        #[prost(message, optional, tag="1")]
        pub topic: ::core::option::Option<super::Path>,
        /// Topic's instance cluster name.
        #[prost(string, tag="2")]
        pub cluster: ::prost::alloc::string::String,
        /// Partition identifier. topic:cluster:partition is unique addressing of partition.
        #[prost(uint64, tag="3")]
        pub partition: u64,
        /// Assign idenfier. Used for matching Assigned and Release requests from server.
        #[prost(uint64, tag="5")]
        pub assign_id: u64,
        #[prost(uint64, tag="6")]
        pub committed_offset: u64,
        #[prost(uint64, tag="7")]
        pub end_offset: u64,
        #[prost(uint64, tag="8")]
        pub write_watermark_ms: u64,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Response {
        #[prost(message, tag="3")]
        InitResponse(InitResponse),
        #[prost(message, tag="4")]
        DataBatch(DataBatch),
        #[prost(message, tag="5")]
        Assigned(Assigned),
        #[prost(message, tag="6")]
        Release(Release),
        #[prost(message, tag="7")]
        Committed(Committed),
        #[prost(message, tag="8")]
        PartitionStatus(PartitionStatus),
    }
}
// *
// Reading information request sent from client to server.

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadInfoRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::super::operations::OperationParams>,
    /// List of topics that are beeing read.
    #[prost(message, repeated, tag="2")]
    pub topics: ::prost::alloc::vec::Vec<Path>,
    /// If get_only_original == false then return info about mirrored topics too.
    #[prost(bool, tag="3")]
    pub get_only_original: bool,
    /// Consumer path that is reading specified topics.
    #[prost(message, optional, tag="4")]
    pub consumer: ::core::option::Option<Path>,
}
// *
// Reading information response sent from server to client.

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadInfoResponse {
    /// Result of request will be inside operation.
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::super::operations::Operation>,
}
// *
// Reading information message that will be inside ReadInfoResponse.operation.

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadInfoResult {
    /// List of topics info.
    #[prost(message, repeated, tag="1")]
    pub topics: ::prost::alloc::vec::Vec<read_info_result::TopicInfo>,
}
/// Nested message and enum types in `ReadInfoResult`.
pub mod read_info_result {
    /// Message containing information about concrete topic reading.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct TopicInfo {
        /// Topic path.
        #[prost(message, optional, tag="1")]
        pub topic: ::core::option::Option<super::Path>,
        /// Topic original cluster.
        #[prost(string, tag="2")]
        pub cluster: ::prost::alloc::string::String,
        /// Status of whole topic.
        #[prost(enumeration="super::super::super::status_ids::StatusCode", tag="3")]
        pub status: i32,
        /// Issues if any.
        #[prost(message, repeated, tag="4")]
        pub issues: ::prost::alloc::vec::Vec<super::super::super::issue::IssueMessage>,
        /// Reading info for partitions of this topic.
        #[prost(message, repeated, tag="5")]
        pub partitions: ::prost::alloc::vec::Vec<topic_info::PartitionInfo>,
    }
    /// Nested message and enum types in `TopicInfo`.
    pub mod topic_info {
        /// Message containing information about concrete topic's partition reading.
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct PartitionInfo {
            /// Patition identifier inside topic.
            #[prost(uint64, tag="1")]
            pub partition: u64,
            /// Request status of partition.
            #[prost(enumeration="super::super::super::super::status_ids::StatusCode", tag="2")]
            pub status: i32,
            /// Issues if any.
            #[prost(message, repeated, tag="3")]
            pub issues: ::prost::alloc::vec::Vec<super::super::super::super::issue::IssueMessage>,
            /// Offset of first message in partition.
            #[prost(uint64, tag="4")]
            pub start_offset: u64,
            /// Offset of next not yet existing message in partition.
            #[prost(uint64, tag="5")]
            pub end_offset: u64,
            /// Offset of consumer committed message a.k.a. first not processed message.
            /// If commit_offset == end_offset then all messages from partition are processed.
            #[prost(uint64, tag="6")]
            pub commit_offset: u64,
            /// Consumer lag in time between committed and last messages in partition.
            #[prost(uint64, tag="7")]
            pub commit_time_lag_ms: u64,
            /// Offset of first not read message by consumer from this partition.
            /// read_offset can be bigger that committed_offset - consumer could read some messages but not yet commit them.
            #[prost(uint64, tag="8")]
            pub read_offset: u64,
            /// Consumer lag in time between read and last messages in partition.
            #[prost(uint64, tag="9")]
            pub read_time_lag_ms: u64,
            /// Session identifier that locked and reading this partition right now.
            #[prost(string, tag="10")]
            pub session_id: ::prost::alloc::string::String,
            /// Ip if node that created reading this session.
            #[prost(string, tag="11")]
            pub client_node: ::prost::alloc::string::String,
            /// Host name of proxy node that processing this reading session.
            #[prost(string, tag="12")]
            pub proxy_node: ::prost::alloc::string::String,
            /// Host name of node where partition leader is running.
            #[prost(string, tag="13")]
            pub tablet_node: ::prost::alloc::string::String,
            /// Assign identifier of actual partition assignment.
            #[prost(uint64, tag="14")]
            pub assign_id: u64,
            /// Timestamp of assignment.
            #[prost(uint64, tag="15")]
            pub assign_timestamp_ms: u64,
            /// Cookie of last performed read in session.
            #[prost(uint64, tag="16")]
            pub last_read_cookie: u64,
            /// Cookie upto whitch commits done.
            #[prost(uint64, tag="17")]
            pub committed_read_cookie: u64,
            /// Cookie that client wants to commit, but server is waiting for committed_read_cookie + 1.
            #[prost(uint64, repeated, tag="18")]
            pub out_of_order_read_cookies_to_commit: ::prost::alloc::vec::Vec<u64>,
        }
    }
}
// *
// Drop topic request sent from client to server.

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropTopicRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::super::operations::OperationParams>,
    /// Topic path.
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
}
// *
// Drop topic response sent from server to client. If topic is not existed then response status will be "SCHEME_ERROR".

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropTopicResponse {
    /// Result of request will be inside operation.
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::super::operations::Operation>,
}
// *
// Drop topic result message that will be inside DropTopicResponse.operation.

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropTopicResult {
}
// *
// Credentials settings

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Credentials {
    #[prost(oneof="credentials::Credentials", tags="1, 2, 3")]
    pub credentials: ::core::option::Option<credentials::Credentials>,
}
/// Nested message and enum types in `Credentials`.
pub mod credentials {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Iam {
        #[prost(string, tag="1")]
        pub endpoint: ::prost::alloc::string::String,
        #[prost(string, tag="2")]
        pub service_account_key: ::prost::alloc::string::String,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Credentials {
        #[prost(string, tag="1")]
        OauthToken(::prost::alloc::string::String),
        #[prost(string, tag="2")]
        JwtParams(::prost::alloc::string::String),
        #[prost(message, tag="3")]
        Iam(Iam),
    }
}
// *
// Message for describing topic internals.

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopicSettings {
    /// How many partitions in topic. Must less than database limit. Default limit - 10.
    #[prost(int32, tag="1")]
    pub partitions_count: i32,
    /// How long data in partition should be stored. Must be greater than 0 and less than limit for this database.  Default limit - 36 hours.
    #[prost(int64, tag="2")]
    pub retention_period_ms: i64,
    /// How long last written seqno for message group should be stored. Must be greater then retention_period_ms and less then limit for this database.  Default limit - 16 days.
    #[prost(int64, tag="12")]
    pub message_group_seqno_retention_period_ms: i64,
    /// How many last written seqno for various message groups should be stored per partition. Must be less than limit for this database.  Default limit - 6*10^6 values.
    #[prost(int64, tag="13")]
    pub max_partition_message_groups_seqno_stored: i64,
    /// Max format version that is allowed for writers. Must be value from enum FormatVersion.
    /// Writes with greater format version are forbiden.
    #[prost(enumeration="topic_settings::Format", tag="3")]
    pub supported_format: i32,
    /// List of allowed codecs for writers.
    /// Writes with codec not from this list are forbiden.
    #[prost(enumeration="Codec", repeated, packed="false", tag="4")]
    pub supported_codecs: ::prost::alloc::vec::Vec<i32>,
    /// Max storage usage for each topic's partition. Must be less than database limit. Default limit - 130 GB.
    #[prost(int64, tag="5")]
    pub max_partition_storage_size: i64,
    /// Partition write speed in bytes per second. Must be less than database limit. Default limit - 1 MB/s.
    #[prost(int64, tag="6")]
    pub max_partition_write_speed: i64,
    /// Burst size for write in partition, in bytes. Must be less than database limit. Default limit - 1 MB.
    #[prost(int64, tag="7")]
    pub max_partition_write_burst: i64,
    /// Disallows client writes. Used for mirrored topics in federation.
    #[prost(bool, tag="8")]
    pub client_write_disabled: bool,
    /// List of consumer read rules for this topic.
    #[prost(message, repeated, tag="9")]
    pub read_rules: ::prost::alloc::vec::Vec<topic_settings::ReadRule>,
    /// User and server attributes of topic. Server attributes starts from "_" and will be validated by server.
    #[prost(map="string, string", tag="10")]
    pub attributes: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    /// remote mirror rule for this topic.
    #[prost(message, optional, tag="11")]
    pub remote_mirror_rule: ::core::option::Option<topic_settings::RemoteMirrorRule>,
}
/// Nested message and enum types in `TopicSettings`.
pub mod topic_settings {
    /// Message for read rules description.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ReadRule {
        /// For what consumer this read rule is. Must be valid not empty consumer name.
        /// Is key for read rules. There could be only one read rule with corresponding consumer name.
        #[prost(string, tag="1")]
        pub consumer_name: ::prost::alloc::string::String,
        /// Flag that this consumer is important.
        #[prost(bool, tag="2")]
        pub important: bool,
        /// All messages with smaller timestamp of write will be skipped.
        #[prost(int64, tag="3")]
        pub starting_message_timestamp_ms: i64,
        /// Max format version that is supported by this consumer.
        /// supported_format on topic must not be greater.
        #[prost(enumeration="Format", tag="4")]
        pub supported_format: i32,
        /// List of supported codecs by this consumer.
        /// supported_codecs on topic must be contained inside this list.
        #[prost(enumeration="super::Codec", repeated, packed="false", tag="5")]
        pub supported_codecs: ::prost::alloc::vec::Vec<i32>,
        /// Read rule version. Any non-negative integer.
        #[prost(int64, tag="6")]
        pub version: i64,
        /// Client service type.
        #[prost(string, tag="7")]
        pub service_type: ::prost::alloc::string::String,
    }
    /// Message for remote mirror rule description.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct RemoteMirrorRule {
        /// Source cluster endpoint in format server:port.
        #[prost(string, tag="1")]
        pub endpoint: ::prost::alloc::string::String,
        /// Source topic that we want to mirror.
        #[prost(string, tag="2")]
        pub topic_path: ::prost::alloc::string::String,
        /// Source consumer for reading source topic.
        #[prost(string, tag="3")]
        pub consumer_name: ::prost::alloc::string::String,
        /// Credentials for reading source topic by source consumer.
        #[prost(message, optional, tag="4")]
        pub credentials: ::core::option::Option<super::Credentials>,
        /// All messages with smaller timestamp of write will be skipped.
        #[prost(int64, tag="5")]
        pub starting_message_timestamp_ms: i64,
        /// Database
        #[prost(string, tag="6")]
        pub database: ::prost::alloc::string::String,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Format {
        Unspecified = 0,
        Base = 1,
    }
    impl Format {
        /// String value of the enum field names used in the ProtoBuf definition.
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Format::Unspecified => "FORMAT_UNSPECIFIED",
                Format::Base => "FORMAT_BASE",
            }
        }
    }
}
// *
// Create topic request sent from client to server.

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateTopicRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::super::operations::OperationParams>,
    /// Topic path.
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
    /// Topic settings.
    #[prost(message, optional, tag="4")]
    pub settings: ::core::option::Option<TopicSettings>,
}
// *
// Create topic response sent from server to client. If topic is already exists then response status will be "ALREADY_EXISTS".

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateTopicResponse {
    /// Result of request will be inside operation.
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::super::operations::Operation>,
}
// *
// Create topic result message that will be inside CreateTopicResponse.operation.

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateTopicResult {
}
// *
// Update existing topic request sent from client to server.

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterTopicRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::super::operations::OperationParams>,
    /// Topic path.
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
    /// New topic settings to be set. All options inside should be set despite same value.
    #[prost(message, optional, tag="4")]
    pub settings: ::core::option::Option<TopicSettings>,
}
// *
// Update topic response sent from server to client.

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterTopicResponse {
    /// Result of request will be inside operation.
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::super::operations::Operation>,
}
/// *
/// Update topic result message that will be inside UpdateTopicResponse.operation.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterTopicResult {
}
/// *
/// Add read rules for existing topic request.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddReadRuleRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::super::operations::OperationParams>,
    /// Topic path.
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
    /// read rules to add
    #[prost(message, optional, tag="3")]
    pub read_rule: ::core::option::Option<topic_settings::ReadRule>,
}
/// *
/// Add read rules for existing topic response.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddReadRuleResponse {
    /// Result of request will be inside operation.
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::super::operations::Operation>,
}
/// *
/// Add read rules result message that will be inside AddReadRuleReponse.operation.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddReadRuleResult {
}
/// *
/// Remove read rules request for existing topic.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveReadRuleRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::super::operations::OperationParams>,
    /// Topic path.
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
    /// read rules to remove. Only consumer names
    #[prost(string, tag="3")]
    pub consumer_name: ::prost::alloc::string::String,
}
/// *
/// Remove read rules response for existing topic.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveReadRuleResponse {
    /// Result of request will be inside operation.
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::super::operations::Operation>,
}
/// *
/// Remove read rules result message that will be inside RemoveReadRuleReponse.operation.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoveReadRuleResult {
}
// *
// Describe topic request sent from client to server.

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeTopicRequest {
    #[prost(message, optional, tag="1")]
    pub operation_params: ::core::option::Option<super::super::operations::OperationParams>,
    /// Topic path.
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
}
// *
// Describe topic response sent from server to client. If topic is not existed then response status will be "SCHEME_ERROR".

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeTopicResponse {
    /// Result of request will be inside operation.
    #[prost(message, optional, tag="1")]
    pub operation: ::core::option::Option<super::super::operations::Operation>,
}
// *
// Describe topic result message that will be inside DescribeTopicResponse.operation.

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeTopicResult {
    /// Topic path.
    #[prost(message, optional, tag="1")]
    pub self_: ::core::option::Option<super::super::scheme::Entry>,
    /// Settings of topic.
    #[prost(message, optional, tag="2")]
    pub settings: ::core::option::Option<TopicSettings>,
}
// NOTE:
// * We use 'ms' suffix instead of google.protobuf.Timestamp and google.protobuf.Duration in order to utilize
// packed encoding ('message' types can't be packed encoded). In non-repeated fields we use 'ms' for consistency.
// * Any message with non-empty 'issues' property leads to streaming RPC termination.

#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Codec {
    Unspecified = 0,
    Raw = 1,
    Gzip = 2,
    Lzop = 3,
    Zstd = 4,
}
impl Codec {
    /// String value of the enum field names used in the ProtoBuf definition.
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Codec::Unspecified => "CODEC_UNSPECIFIED",
            Codec::Raw => "CODEC_RAW",
            Codec::Gzip => "CODEC_GZIP",
            Codec::Lzop => "CODEC_LZOP",
            Codec::Zstd => "CODEC_ZSTD",
        }
    }
}
/// Generated client implementations.
pub mod pers_queue_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct PersQueueServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl PersQueueServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> PersQueueServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> PersQueueServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            PersQueueServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        pub async fn streaming_write(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::StreamingWriteClientMessage,
            >,
        ) -> Result<
                tonic::Response<
                    tonic::codec::Streaming<super::StreamingWriteServerMessage>,
                >,
                tonic::Status,
            > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.PersQueue.V1.PersQueueService/StreamingWrite",
            );
            self.inner.streaming(request.into_streaming_request(), path, codec).await
        }
        ///*
        /// Creates Read Session
        /// Pipeline:
        /// client                  server
        ///         Init(Topics, ClientId, ...)
        ///        ---------------->
        ///         Init(SessionId)
        ///        <----------------
        ///         read1
        ///        ---------------->
        ///         read2
        ///        ---------------->
        ///         assign(Topic1, Cluster, Partition1, ...) - assigns and releases are optional
        ///        <----------------
        ///         assign(Topic2, Clutster, Partition2, ...)
        ///        <----------------
        ///         start_read(Topic1, Partition1, ...) - client must respond to assign request with this message. Only after this client will start recieving messages from this partition
        ///        ---------------->
        ///         release(Topic1, Partition1, ...)
        ///        <----------------
        ///         released(Topic1, Partition1, ...) - only after released server will give this parittion to other session.
        ///        ---------------->
        ///         start_read(Topic2, Partition2, ...) - client must respond to assign request with this message. Only after this client will start recieving messages from this partition
        ///        ---------------->
        ///         read data(data, ...)
        ///        <----------------
        ///         commit(cookie1)
        ///        ---------------->
        ///         committed(cookie1)
        ///        <----------------
        ///         issue(description, ...)
        ///        <----------------
        pub async fn migration_streaming_read(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::MigrationStreamingReadClientMessage,
            >,
        ) -> Result<
                tonic::Response<
                    tonic::codec::Streaming<super::MigrationStreamingReadServerMessage>,
                >,
                tonic::Status,
            > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.PersQueue.V1.PersQueueService/MigrationStreamingRead",
            );
            self.inner.streaming(request.into_streaming_request(), path, codec).await
        }
        /// Get information about reading
        pub async fn get_read_sessions_info(
            &mut self,
            request: impl tonic::IntoRequest<super::ReadInfoRequest>,
        ) -> Result<tonic::Response<super::ReadInfoResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.PersQueue.V1.PersQueueService/GetReadSessionsInfo",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Describe topic command.
        pub async fn describe_topic(
            &mut self,
            request: impl tonic::IntoRequest<super::DescribeTopicRequest>,
        ) -> Result<tonic::Response<super::DescribeTopicResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.PersQueue.V1.PersQueueService/DescribeTopic",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Drop topic command.
        pub async fn drop_topic(
            &mut self,
            request: impl tonic::IntoRequest<super::DropTopicRequest>,
        ) -> Result<tonic::Response<super::DropTopicResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.PersQueue.V1.PersQueueService/DropTopic",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Create topic command.
        pub async fn create_topic(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateTopicRequest>,
        ) -> Result<tonic::Response<super::CreateTopicResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.PersQueue.V1.PersQueueService/CreateTopic",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Alter topic command.
        pub async fn alter_topic(
            &mut self,
            request: impl tonic::IntoRequest<super::AlterTopicRequest>,
        ) -> Result<tonic::Response<super::AlterTopicResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.PersQueue.V1.PersQueueService/AlterTopic",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Add read rule command.
        pub async fn add_read_rule(
            &mut self,
            request: impl tonic::IntoRequest<super::AddReadRuleRequest>,
        ) -> Result<tonic::Response<super::AddReadRuleResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.PersQueue.V1.PersQueueService/AddReadRule",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Remove read rule command.
        pub async fn remove_read_rule(
            &mut self,
            request: impl tonic::IntoRequest<super::RemoveReadRuleRequest>,
        ) -> Result<tonic::Response<super::RemoveReadRuleResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.PersQueue.V1.PersQueueService/RemoveReadRule",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated client implementations.
pub mod cluster_discovery_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct ClusterDiscoveryServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ClusterDiscoveryServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ClusterDiscoveryServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ClusterDiscoveryServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            ClusterDiscoveryServiceClient::new(
                InterceptedService::new(inner, interceptor),
            )
        }
        /// Compress requests with the given encoding.
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Get PQ clusters which are eligible for the specified Write or Read Sessions
        pub async fn discover_clusters(
            &mut self,
            request: impl tonic::IntoRequest<
                super::super::cluster_discovery::DiscoverClustersRequest,
            >,
        ) -> Result<
                tonic::Response<
                    super::super::cluster_discovery::DiscoverClustersResponse,
                >,
                tonic::Status,
            > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Ydb.PersQueue.V1.ClusterDiscoveryService/DiscoverClusters",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}