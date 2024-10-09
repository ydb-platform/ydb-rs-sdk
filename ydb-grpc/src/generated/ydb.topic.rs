/// Description of supported codecs.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SupportedCodecs {
    /// List of supported codecs.
    /// See enum Codec above for values.
    #[prost(int32, repeated, packed = "false", tag = "1")]
    pub codecs: ::prost::alloc::vec::Vec<i32>,
}
/// Represents range [start, end).
/// I.e. (end - 1) is the greatest of offsets, included in non-empty range.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OffsetsRange {
    #[prost(int64, tag = "1")]
    pub start: i64,
    #[prost(int64, tag = "2")]
    pub end: i64,
}
/// In-session reauthentication and reauthorization, lets user increase session lifetime.
/// Client should wait for UpdateTokenResponse before sending next UpdateTokenRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateTokenRequest {
    #[prost(string, tag = "1")]
    pub token: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateTokenResponse {}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionWithGeneration {
    /// Partition identifier.
    #[prost(int64, tag = "1")]
    pub partition_id: i64,
    /// Partition generation.
    #[prost(int64, tag = "2")]
    pub generation: i64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MetadataItem {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "2")]
    pub value: ::prost::alloc::vec::Vec<u8>,
}
/// Messages for bidirectional streaming rpc StreamWrite
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamWriteMessage {}
/// Nested message and enum types in `StreamWriteMessage`.
pub mod stream_write_message {
    /// Client-server message for write session. Contains one of:
    ///      InitRequest - handshake request.
    ///      WriteRequest - portion of data to be written.
    ///      UpdateTokenRequest - user credentials if update is needed.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct FromClient {
        #[prost(oneof = "from_client::ClientMessage", tags = "1, 2, 3")]
        pub client_message: ::core::option::Option<from_client::ClientMessage>,
    }
    /// Nested message and enum types in `FromClient`.
    pub mod from_client {
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum ClientMessage {
            #[prost(message, tag = "1")]
            InitRequest(super::InitRequest),
            #[prost(message, tag = "2")]
            WriteRequest(super::WriteRequest),
            #[prost(message, tag = "3")]
            UpdateTokenRequest(super::super::UpdateTokenRequest),
        }
    }
    /// Server-client message for write session. Contains either non-success status, or one of:
    ///      InitResponse - correct handshake response.
    ///      WriteResponse - acknowledgment of storing client messages.
    ///      UpdateTokenResponse - acknowledgment of reauthentication and reauthorization.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct FromServer {
        /// Server status of response.
        #[prost(enumeration = "super::super::status_ids::StatusCode", tag = "1")]
        pub status: i32,
        /// Issues if any.
        #[prost(message, repeated, tag = "2")]
        pub issues: ::prost::alloc::vec::Vec<super::super::issue::IssueMessage>,
        #[prost(oneof = "from_server::ServerMessage", tags = "3, 4, 5")]
        pub server_message: ::core::option::Option<from_server::ServerMessage>,
    }
    /// Nested message and enum types in `FromServer`.
    pub mod from_server {
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum ServerMessage {
            #[prost(message, tag = "3")]
            InitResponse(super::InitResponse),
            #[prost(message, tag = "4")]
            WriteResponse(super::WriteResponse),
            #[prost(message, tag = "5")]
            UpdateTokenResponse(super::super::UpdateTokenResponse),
        }
    }
    /// Handshake request that must be sent to server first.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct InitRequest {
        /// Full path of topic to write to.
        #[prost(string, tag = "1")]
        pub path: ::prost::alloc::string::String,
        /// Producer identifier of client data stream.
        /// Used for message deduplication by sequence numbers.
        #[prost(string, tag = "2")]
        pub producer_id: ::prost::alloc::string::String,
        /// User metadata attached to this write session.
        /// Reader will get this session meta data with each message read.
        #[prost(map = "string, string", tag = "3")]
        pub write_session_meta: ::std::collections::HashMap<
            ::prost::alloc::string::String,
            ::prost::alloc::string::String,
        >,
        /// Explicitly request for last sequential number
        /// It may be expensive, if producer wrote to many partitions before.
        #[prost(bool, tag = "6")]
        pub get_last_seq_no: bool,
        /// Option for setting order on messages.
        /// If neither is set, no guarantees on ordering or partitions to write to.
        #[prost(oneof = "init_request::Partitioning", tags = "4, 5, 7")]
        pub partitioning: ::core::option::Option<init_request::Partitioning>,
    }
    /// Nested message and enum types in `InitRequest`.
    pub mod init_request {
        /// Option for setting order on messages.
        /// If neither is set, no guarantees on ordering or partitions to write to.
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Partitioning {
            /// All messages with given pair (producer_id, message_group_id) go to single partition in order of writes.
            #[prost(string, tag = "4")]
            MessageGroupId(::prost::alloc::string::String),
            /// Explicit partition id to write to.
            #[prost(int64, tag = "5")]
            PartitionId(i64),
            /// Explicit partition location to write to.
            #[prost(message, tag = "7")]
            PartitionWithGeneration(super::super::PartitionWithGeneration),
        }
    }
    /// Response to the handshake.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct InitResponse {
        /// Last persisted message's sequence number for this producer.
        /// Zero for new producer.
        #[prost(int64, tag = "1")]
        pub last_seq_no: i64,
        /// Unique identifier of write session. Used for debug purposes.
        #[prost(string, tag = "2")]
        pub session_id: ::prost::alloc::string::String,
        /// Identifier of partition that is matched for this write session.
        #[prost(int64, tag = "3")]
        pub partition_id: i64,
        /// Client can only use compression codecs from this set to write messages to topic.
        /// Otherwise session will be closed with BAD_REQUEST.
        #[prost(message, optional, tag = "4")]
        pub supported_codecs: ::core::option::Option<super::SupportedCodecs>,
    }
    /// Represents portion of client messages.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct WriteRequest {
        #[prost(message, repeated, tag = "1")]
        pub messages: ::prost::alloc::vec::Vec<write_request::MessageData>,
        /// Codec that is used for data compression.
        /// See enum Codec above for values.
        #[prost(int32, tag = "2")]
        pub codec: i32,
        #[prost(message, optional, tag = "3")]
        pub tx: ::core::option::Option<super::TransactionIdentity>,
    }
    /// Nested message and enum types in `WriteRequest`.
    pub mod write_request {
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct MessageData {
            /// Message sequence number, provided by client for deduplication.
            /// Starts at 1
            #[prost(int64, tag = "1")]
            pub seq_no: i64,
            /// Creation timestamp
            #[prost(message, optional, tag = "2")]
            pub created_at: ::core::option::Option<
                super::super::super::super::google::protobuf::Timestamp,
            >,
            /// Compressed client message body.
            #[prost(bytes = "vec", tag = "3")]
            pub data: ::prost::alloc::vec::Vec<u8>,
            /// Uncompressed size of client message body.
            #[prost(int64, tag = "4")]
            pub uncompressed_size: i64,
            /// Message metadata. Overall size is limited to 4096 symbols (all keys and values combined).
            #[prost(message, repeated, tag = "7")]
            pub metadata_items: ::prost::alloc::vec::Vec<super::super::MetadataItem>,
            /// Per-message override for respective write session settings.
            #[prost(oneof = "message_data::Partitioning", tags = "5, 6, 8")]
            pub partitioning: ::core::option::Option<message_data::Partitioning>,
        }
        /// Nested message and enum types in `MessageData`.
        pub mod message_data {
            /// Per-message override for respective write session settings.
            #[derive(serde::Serialize, serde::Deserialize)]
            #[derive(Clone, PartialEq, ::prost::Oneof)]
            pub enum Partitioning {
                /// All messages with given pair (producer_id, message_group_id) go to single partition in order of writes.
                #[prost(string, tag = "5")]
                MessageGroupId(::prost::alloc::string::String),
                /// Explicit partition id to write to.
                #[prost(int64, tag = "6")]
                PartitionId(i64),
                /// Explicit partition location to write to.
                #[prost(message, tag = "8")]
                PartitionWithGeneration(super::super::super::PartitionWithGeneration),
            }
        }
    }
    /// Message that represents acknowledgment for sequence of client messages.
    /// This sequence is persisted together so write statistics is for messages batch.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct WriteResponse {
        /// Number of acks is equal to number of messages in the corresponding WriteRequests.
        #[prost(message, repeated, tag = "1")]
        pub acks: ::prost::alloc::vec::Vec<write_response::WriteAck>,
        /// Assigned partition for all client messages inside this batch.
        /// This actual partition may differ from that returned in InitResponse
        /// or other WriteResponses in this write session.
        #[prost(int64, tag = "2")]
        pub partition_id: i64,
        /// Write statistics for this sequence of client messages.
        #[prost(message, optional, tag = "3")]
        pub write_statistics: ::core::option::Option<write_response::WriteStatistics>,
    }
    /// Nested message and enum types in `WriteResponse`.
    pub mod write_response {
        /// Acknowledgment for one persistently written message.
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct WriteAck {
            /// Sequence number as in WriteRequest.
            #[prost(int64, tag = "1")]
            pub seq_no: i64,
            /// Either message is written for the first time or duplicate.
            #[prost(oneof = "write_ack::MessageWriteStatus", tags = "2, 3, 4")]
            pub message_write_status: ::core::option::Option<
                write_ack::MessageWriteStatus,
            >,
        }
        /// Nested message and enum types in `WriteAck`.
        pub mod write_ack {
            #[derive(serde::Serialize, serde::Deserialize)]
            #[derive(Clone, PartialEq, ::prost::Message)]
            pub struct Written {
                /// Assigned partition offset.
                #[prost(int64, tag = "1")]
                pub offset: i64,
            }
            #[derive(serde::Serialize, serde::Deserialize)]
            #[derive(Clone, PartialEq, ::prost::Message)]
            pub struct Skipped {
                #[prost(enumeration = "skipped::Reason", tag = "1")]
                pub reason: i32,
            }
            /// Nested message and enum types in `Skipped`.
            pub mod skipped {
                #[derive(serde::Serialize, serde::Deserialize)]
                #[derive(
                    Clone,
                    Copy,
                    Debug,
                    PartialEq,
                    Eq,
                    Hash,
                    PartialOrd,
                    Ord,
                    ::prost::Enumeration
                )]
                #[repr(i32)]
                pub enum Reason {
                    Unspecified = 0,
                    AlreadyWritten = 1,
                }
                impl Reason {
                    /// String value of the enum field names used in the ProtoBuf definition.
                    /// The values are not transformed in any way and thus are considered stable
                    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
                    pub fn as_str_name(&self) -> &'static str {
                        match self {
                            Reason::Unspecified => "REASON_UNSPECIFIED",
                            Reason::AlreadyWritten => "REASON_ALREADY_WRITTEN",
                        }
                    }
                }
            }
            #[derive(serde::Serialize, serde::Deserialize)]
            #[derive(Clone, PartialEq, ::prost::Message)]
            pub struct WrittenInTx {}
            /// Either message is written for the first time or duplicate.
            #[derive(serde::Serialize, serde::Deserialize)]
            #[derive(Clone, PartialEq, ::prost::Oneof)]
            pub enum MessageWriteStatus {
                #[prost(message, tag = "2")]
                Written(Written),
                #[prost(message, tag = "3")]
                Skipped(Skipped),
                #[prost(message, tag = "4")]
                WrittenInTx(WrittenInTx),
            }
        }
        /// Message with write statistics.
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct WriteStatistics {
            /// Time spent in persisting of data. Same for each message in response.
            #[prost(message, optional, tag = "1")]
            pub persisting_time: ::core::option::Option<
                super::super::super::super::google::protobuf::Duration,
            >,
            /// Time spent in queue before persisting, minimal of all messages in response.
            #[prost(message, optional, tag = "2")]
            pub min_queue_wait_time: ::core::option::Option<
                super::super::super::super::google::protobuf::Duration,
            >,
            /// Time spent in queue before persisting, maximal of all messages in response.
            #[prost(message, optional, tag = "3")]
            pub max_queue_wait_time: ::core::option::Option<
                super::super::super::super::google::protobuf::Duration,
            >,
            /// Time spent awaiting for partition write quota. Same for each message in response.
            #[prost(message, optional, tag = "4")]
            pub partition_quota_wait_time: ::core::option::Option<
                super::super::super::super::google::protobuf::Duration,
            >,
            /// Time spent awaiting for topic write quota. Same for each message in response.
            #[prost(message, optional, tag = "5")]
            pub topic_quota_wait_time: ::core::option::Option<
                super::super::super::super::google::protobuf::Duration,
            >,
        }
    }
}
/// Messages for bidirectional streaming rpc StreamRead
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamReadMessage {}
/// Nested message and enum types in `StreamReadMessage`.
pub mod stream_read_message {
    /// Within a StreamRead session delivered messages are separated by partition.
    /// Reads from a single partition are represented by a partition session.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct PartitionSession {
        /// Identifier of partition session. Unique inside one RPC call.
        #[prost(int64, tag = "1")]
        pub partition_session_id: i64,
        /// Topic path of partition.
        #[prost(string, tag = "2")]
        pub path: ::prost::alloc::string::String,
        /// Partition identifier.
        #[prost(int64, tag = "3")]
        pub partition_id: i64,
    }
    /// Client-server message for read session. Contains one of:
    ///      InitRequest - handshake request.
    ///      ReadRequest - request for data.
    ///      CommitOffsetRequest - request for commit of some read data.
    ///      PartitionSessionStatusRequest - request for session status
    ///      UpdateTokenRequest - request to update auth token
    ///      DirectReadAck - client signals it has finished direct reading from the partition node.
    ///      StartPartitionSessionResponse - Response to StreamReadServerMessage.StartPartitionSessionRequest.
    ///          Client signals it is ready to get data from partition.
    ///      StopPartitionSessionResponse - Response to StreamReadServerMessage.StopPartitionSessionRequest.
    ///          Client signals it has finished working with partition. Mandatory for graceful stop, optional otherwise.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct FromClient {
        #[prost(oneof = "from_client::ClientMessage", tags = "1, 2, 3, 4, 5, 8, 6, 7")]
        pub client_message: ::core::option::Option<from_client::ClientMessage>,
    }
    /// Nested message and enum types in `FromClient`.
    pub mod from_client {
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum ClientMessage {
            /// Client requests.
            #[prost(message, tag = "1")]
            InitRequest(super::InitRequest),
            #[prost(message, tag = "2")]
            ReadRequest(super::ReadRequest),
            #[prost(message, tag = "3")]
            CommitOffsetRequest(super::CommitOffsetRequest),
            #[prost(message, tag = "4")]
            PartitionSessionStatusRequest(super::PartitionSessionStatusRequest),
            #[prost(message, tag = "5")]
            UpdateTokenRequest(super::super::UpdateTokenRequest),
            #[prost(message, tag = "8")]
            DirectReadAck(super::DirectReadAck),
            /// Responses to respective server commands.
            #[prost(message, tag = "6")]
            StartPartitionSessionResponse(super::StartPartitionSessionResponse),
            #[prost(message, tag = "7")]
            StopPartitionSessionResponse(super::StopPartitionSessionResponse),
        }
    }
    /// Server-client message for read session. Contains one of:
    ///      InitResponse - handshake response from server.
    ///      ReadResponse - portion of data.
    ///      CommitOffsetResponse - acknowledgment for commit.
    ///      PartitionSessionStatusResponse - server response with partition session status.
    ///      UpdateTokenResponse - acknowledgment of token update.
    ///      StartPartitionSessionRequest - command from server to create a partition session.
    ///      StopPartitionSessionRequest - command from server to destroy a partition session.
    ///      UpdatePartitionSession - command from server to update a partition session.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct FromServer {
        /// Server status of response.
        #[prost(enumeration = "super::super::status_ids::StatusCode", tag = "1")]
        pub status: i32,
        /// Issues if any.
        #[prost(message, repeated, tag = "2")]
        pub issues: ::prost::alloc::vec::Vec<super::super::issue::IssueMessage>,
        #[prost(oneof = "from_server::ServerMessage", tags = "3, 4, 5, 6, 7, 8, 9, 10")]
        pub server_message: ::core::option::Option<from_server::ServerMessage>,
    }
    /// Nested message and enum types in `FromServer`.
    pub mod from_server {
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum ServerMessage {
            /// Responses to respective client requests.
            #[prost(message, tag = "3")]
            InitResponse(super::InitResponse),
            #[prost(message, tag = "4")]
            ReadResponse(super::ReadResponse),
            #[prost(message, tag = "5")]
            CommitOffsetResponse(super::CommitOffsetResponse),
            #[prost(message, tag = "6")]
            PartitionSessionStatusResponse(super::PartitionSessionStatusResponse),
            #[prost(message, tag = "7")]
            UpdateTokenResponse(super::super::UpdateTokenResponse),
            /// Server commands.
            #[prost(message, tag = "8")]
            StartPartitionSessionRequest(super::StartPartitionSessionRequest),
            #[prost(message, tag = "9")]
            StopPartitionSessionRequest(super::StopPartitionSessionRequest),
            #[prost(message, tag = "10")]
            UpdatePartitionSession(super::UpdatePartitionSession),
        }
    }
    /// Handshake request.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct InitRequest {
        /// Message that describes topic to read.
        /// Topics that will be read by this session.
        #[prost(message, repeated, tag = "1")]
        pub topics_read_settings: ::prost::alloc::vec::Vec<
            init_request::TopicReadSettings,
        >,
        /// Path of consumer that is used for reading by this session.
        #[prost(string, tag = "2")]
        pub consumer: ::prost::alloc::string::String,
        /// Optional name. Will be shown in debug stat.
        #[prost(string, tag = "3")]
        pub reader_name: ::prost::alloc::string::String,
        /// Direct reading from a partition node.
        #[prost(bool, tag = "4")]
        pub direct_read: bool,
    }
    /// Nested message and enum types in `InitRequest`.
    pub mod init_request {
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct TopicReadSettings {
            /// Topic path.
            #[prost(string, tag = "1")]
            pub path: ::prost::alloc::string::String,
            /// Partitions that will be read by this session.
            /// If list is empty - then session will read all partitions.
            #[prost(int64, repeated, tag = "2")]
            pub partition_ids: ::prost::alloc::vec::Vec<i64>,
            /// Skip all messages that has write timestamp smaller than now - max_lag.
            /// Zero means infinite lag.
            #[prost(message, optional, tag = "3")]
            pub max_lag: ::core::option::Option<
                super::super::super::super::google::protobuf::Duration,
            >,
            /// Read data only after this timestamp from this topic.
            /// Read only messages with 'written_at' value greater or equal than this timestamp.
            #[prost(message, optional, tag = "4")]
            pub read_from: ::core::option::Option<
                super::super::super::super::google::protobuf::Timestamp,
            >,
        }
    }
    /// Handshake response.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct InitResponse {
        /// Read session identifier.
        #[prost(string, tag = "1")]
        pub session_id: ::prost::alloc::string::String,
    }
    /// Message that represents client readiness for receiving more data.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ReadRequest {
        /// Server and client each keep track of total bytes size of all ReadResponses.
        /// When client is ready to receive N more bytes in responses (to increment possible total by N),
        /// it sends a ReadRequest with bytes_size = N.
        /// bytes_size value must be positive.
        /// So in expression 'A = (sum of bytes_size in all ReadRequests) - (sum of bytes_size in all ReadResponses)'
        ///    server will keep A (available size for responses) non-negative.
        /// But there is an exception. If server receives ReadRequest, and the first message in response exceeds A -
        /// then it will still be delivered, and A will become negative until enough additional ReadRequests.
        /// Example:
        /// 1) Let client have 200 bytes buffer. It sends ReadRequest with bytes_size = 200;
        /// 2) Server may return one ReadResponse with bytes_size = 70 and than another 80 bytes response;
        ///     now client buffer has 50 free bytes, server is free to send up to 50 bytes in responses.
        /// 3) Client processes 100 bytes from buffer, now buffer free space is 150 bytes,
        ///     so client sends ReadRequest with bytes_size = 100;
        /// 4) Server is free to send up to 50 + 100 = 150 bytes. But the next read message is too big,
        ///     and it sends 160 bytes ReadResponse.
        /// 5) Let's assume client somehow processes it, and its 200 bytes buffer is free again.
        ///     It should account for excess 10 bytes and send ReadRequest with bytes_size = 210.
        #[prost(int64, tag = "1")]
        pub bytes_size: i64,
    }
    /// Data read.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ReadResponse {
        /// Client messages, divided by partitions.
        #[prost(message, repeated, tag = "1")]
        pub partition_data: ::prost::alloc::vec::Vec<read_response::PartitionData>,
        /// Total size in bytes of this response as calculated by server.
        /// See ReadRequest comment above.
        #[prost(int64, tag = "2")]
        pub bytes_size: i64,
    }
    /// Nested message and enum types in `ReadResponse`.
    pub mod read_response {
        /// One client message representation.
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct MessageData {
            /// Partition offset in partition that assigned for message.
            /// unique value for client side deduplication - Topic:Partition:Offset
            #[prost(int64, tag = "1")]
            pub offset: i64,
            /// Sequence number that provided with message on write from client.
            #[prost(int64, tag = "2")]
            pub seq_no: i64,
            /// Timestamp of creation of message provided on write from client.
            #[prost(message, optional, tag = "3")]
            pub created_at: ::core::option::Option<
                super::super::super::super::google::protobuf::Timestamp,
            >,
            /// Compressed client message body.
            #[prost(bytes = "vec", tag = "5")]
            pub data: ::prost::alloc::vec::Vec<u8>,
            /// Uncompressed size of client message body.
            /// sent as is from WriteRequest, without check on server side. May be empty (for writes from old client) or wrong (if bug in writer).
            /// Use it for optimization purposes only, don't trust it.
            #[prost(int64, tag = "6")]
            pub uncompressed_size: i64,
            /// Filled if message_group_id was set on message write.
            #[prost(string, tag = "7")]
            pub message_group_id: ::prost::alloc::string::String,
            #[prost(message, repeated, tag = "8")]
            pub metadata_items: ::prost::alloc::vec::Vec<super::super::MetadataItem>,
        }
        /// Representation of sequence of client messages from one write session.
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Batch {
            /// List of client messages.
            #[prost(message, repeated, tag = "1")]
            pub message_data: ::prost::alloc::vec::Vec<MessageData>,
            /// Producer identifier provided by client for this batch of client messages.
            #[prost(string, tag = "2")]
            pub producer_id: ::prost::alloc::string::String,
            /// Client metadata attached to write session, the same for all messages in batch.
            #[prost(map = "string, string", tag = "3")]
            pub write_session_meta: ::std::collections::HashMap<
                ::prost::alloc::string::String,
                ::prost::alloc::string::String,
            >,
            /// Codec that is used for data compression.
            /// See enum Codec above for values.
            #[prost(int32, tag = "4")]
            pub codec: i32,
            /// Persist timestamp on server for batch.
            #[prost(message, optional, tag = "5")]
            pub written_at: ::core::option::Option<
                super::super::super::super::google::protobuf::Timestamp,
            >,
        }
        /// Representation of sequence of messages from one partition.
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct PartitionData {
            #[prost(int64, tag = "1")]
            pub partition_session_id: i64,
            /// Client messages, divided by write sessions.
            #[prost(message, repeated, tag = "2")]
            pub batches: ::prost::alloc::vec::Vec<Batch>,
        }
    }
    /// Signal for server that client processed some read data.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct CommitOffsetRequest {
        /// Partition offsets that indicates processed data.
        #[prost(message, repeated, tag = "1")]
        pub commit_offsets: ::prost::alloc::vec::Vec<
            commit_offset_request::PartitionCommitOffset,
        >,
    }
    /// Nested message and enum types in `CommitOffsetRequest`.
    pub mod commit_offset_request {
        /// Message that is used for describing commit.
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct PartitionCommitOffset {
            /// Identifier of partition session with data to commit.
            #[prost(int64, tag = "1")]
            pub partition_session_id: i64,
            /// Processed offsets ranges, repeated in case of disjoint ranges.
            #[prost(message, repeated, tag = "2")]
            pub offsets: ::prost::alloc::vec::Vec<super::super::OffsetsRange>,
        }
    }
    /// Acknowledgement for commits.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct CommitOffsetResponse {
        /// Partitions with progress.
        #[prost(message, repeated, tag = "1")]
        pub partitions_committed_offsets: ::prost::alloc::vec::Vec<
            commit_offset_response::PartitionCommittedOffset,
        >,
    }
    /// Nested message and enum types in `CommitOffsetResponse`.
    pub mod commit_offset_response {
        /// Per-partition commit representation.
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct PartitionCommittedOffset {
            /// Partition session identifier.
            #[prost(int64, tag = "1")]
            pub partition_session_id: i64,
            /// Upper bound for committed offsets.
            #[prost(int64, tag = "2")]
            pub committed_offset: i64,
        }
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct PartitionSessionStatusRequest {
        #[prost(int64, tag = "1")]
        pub partition_session_id: i64,
    }
    /// Response to status request.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct PartitionSessionStatusResponse {
        /// Identifier of partition session whose status was requested.
        #[prost(int64, tag = "1")]
        pub partition_session_id: i64,
        /// Partition contains messages with offsets in range [start, end).
        #[prost(message, optional, tag = "2")]
        pub partition_offsets: ::core::option::Option<super::OffsetsRange>,
        /// Each offset up to and including (committed_offset - 1) was fully processed.
        #[prost(int64, tag = "3")]
        pub committed_offset: i64,
        /// Write timestamp of next message written to this partition will be no less than write_time_high_watermark.
        #[prost(message, optional, tag = "4")]
        pub write_time_high_watermark: ::core::option::Option<
            super::super::super::google::protobuf::Timestamp,
        >,
    }
    /// Command from server to create and start a partition session.
    /// Client must respond with StartPartitionSessionResponse when ready to receive data from this partition.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StartPartitionSessionRequest {
        /// Partition session description.
        #[prost(message, optional, tag = "1")]
        pub partition_session: ::core::option::Option<PartitionSession>,
        /// Each offset up to and including (committed_offset - 1) was fully processed.
        #[prost(int64, tag = "2")]
        pub committed_offset: i64,
        /// Partition contains messages with offsets in range [start, end).
        #[prost(message, optional, tag = "3")]
        pub partition_offsets: ::core::option::Option<super::OffsetsRange>,
        /// Partition location, filled only when InitRequest.direct_read is true.
        #[prost(message, optional, tag = "4")]
        pub partition_location: ::core::option::Option<super::PartitionLocation>,
    }
    /// Signal for server that cient is ready to recive data for partition.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StartPartitionSessionResponse {
        /// Partition session identifier of partition to start read.
        #[prost(int64, tag = "1")]
        pub partition_session_id: i64,
        /// Reads in this partition session will start from offset no less than read_offset.
        /// If read_offset is set, server will check that read_offset is no less that actual committed offset.
        /// If check fails then server will send an error message (status != SUCCESS) and close stream.
        /// If read_offset is not set, no check will be made.
        /// InitRequest.max_lag and InitRequest.read_from could lead to skip of more messages.
        /// Server will return data starting from offset that is maximum of actual committed offset, read_offset (if set)
        /// and offsets calculated from InitRequest.max_lag and InitRequest.read_from.
        #[prost(int64, optional, tag = "2")]
        pub read_offset: ::core::option::Option<i64>,
        /// All messages with offset less than commit_offset are processed by client.
        /// Server will commit this position if this is not done yet.
        #[prost(int64, optional, tag = "3")]
        pub commit_offset: ::core::option::Option<i64>,
    }
    /// Command from server to stop and destroy concrete partition session.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StopPartitionSessionRequest {
        /// Identifier of partition session that is ready to be closed by server.
        #[prost(int64, tag = "1")]
        pub partition_session_id: i64,
        /// Flag of graceful stop.
        /// If set, server will wait for response from client before giving this partition to other read session.
        /// Server will not send more data from this partition.
        /// Client can process all received data and wait for commit and only after send response.
        /// If False then server gives partition for other session right now.
        /// All further commits for this partition session has no effect. Server is not waiting for response.
        #[prost(bool, tag = "2")]
        pub graceful: bool,
        /// Upper bound for committed offsets.
        #[prost(int64, tag = "3")]
        pub committed_offset: i64,
        /// Upper bound for read request identifiers, filled only when InitRequest.direct_read is true and graceful is true.
        #[prost(int64, tag = "4")]
        pub last_direct_read_id: i64,
    }
    /// Signal for server that client finished working with this partition.
    /// Must be sent only after corresponding StopPartitionSessionRequest from server.
    /// Server will give this partition to other read session only after StopPartitionSessionResponse signal.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StopPartitionSessionResponse {
        /// Partition session identifier of partition session that is released by client.
        #[prost(int64, tag = "1")]
        pub partition_session_id: i64,
        /// Flag of graceful stop, used only when InitRequest.direct_read is true
        /// Client must pass this value unchanged from the StopPartitionSessionRequest.
        /// Server can sent two StopPartitionSessionRequests, the first with graceful=true, the second with graceful=false. The client must answer both of them.
        #[prost(bool, tag = "2")]
        pub graceful: bool,
    }
    /// Command from server to notify about a partition session update.
    /// Client should not send a response to the command.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct UpdatePartitionSession {
        /// Partition session identifier.
        #[prost(int64, tag = "1")]
        pub partition_session_id: i64,
        /// Partition location, filled only when InitRequest.direct_read is true.
        #[prost(message, optional, tag = "2")]
        pub partition_location: ::core::option::Option<super::PartitionLocation>,
    }
    /// Signal for server that client has finished direct reading.
    /// Server should not send a response to the command.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DirectReadAck {
        /// Partition session identifier.
        #[prost(int64, tag = "1")]
        pub partition_session_id: i64,
        /// Identifier of the successfully completed read request.
        #[prost(int64, tag = "2")]
        pub direct_read_id: i64,
    }
}
/// Messages for bidirectional streaming rpc StreamDirectRead
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamDirectReadMessage {}
/// Nested message and enum types in `StreamDirectReadMessage`.
pub mod stream_direct_read_message {
    /// Client-server message for direct read session.
    ///      InitDirectRead - command from client to create and start a direct read session.
    ///      StartDirectReadPartitionSession - command from client to create and start a direct read partition session.
    ///      UpdateTokenRequest - request to update auth token
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct FromClient {
        #[prost(oneof = "from_client::ClientMessage", tags = "1, 2, 3")]
        pub client_message: ::core::option::Option<from_client::ClientMessage>,
    }
    /// Nested message and enum types in `FromClient`.
    pub mod from_client {
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum ClientMessage {
            #[prost(message, tag = "1")]
            InitDirectRead(super::InitDirectRead),
            #[prost(message, tag = "2")]
            StartDirectReadPartitionSession(super::StartDirectReadPartitionSession),
            #[prost(message, tag = "3")]
            UpdateTokenRequest(super::super::UpdateTokenRequest),
        }
    }
    /// Server-client message for direct read session.
    ///      DirectReadResponse - portion of message data.
    ///      StopDirectReadPartitionSession - command from server to stop a direct read partition session.
    ///      UpdateTokenResponse - acknowledgment of token update.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct FromServer {
        /// Server status of response.
        #[prost(enumeration = "super::super::status_ids::StatusCode", tag = "1")]
        pub status: i32,
        /// Issues if any.
        #[prost(message, repeated, tag = "2")]
        pub issues: ::prost::alloc::vec::Vec<super::super::issue::IssueMessage>,
        #[prost(oneof = "from_server::ServerMessage", tags = "3, 4, 5")]
        pub server_message: ::core::option::Option<from_server::ServerMessage>,
    }
    /// Nested message and enum types in `FromServer`.
    pub mod from_server {
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum ServerMessage {
            #[prost(message, tag = "3")]
            StopDirectReadPartitionSession(super::StopDirectReadPartitionSession),
            #[prost(message, tag = "4")]
            DirectReadResponse(super::DirectReadResponse),
            #[prost(message, tag = "5")]
            UpdateTokenResponse(super::super::UpdateTokenResponse),
        }
    }
    /// Command from client to create and start a direct read session.
    /// Server should not send a response to the command.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct InitDirectRead {
        /// Read session identifier.
        #[prost(string, tag = "1")]
        pub session_id: ::prost::alloc::string::String,
        /// Topics that will be read by this session.
        #[prost(message, repeated, tag = "2")]
        pub topics_read_settings: ::prost::alloc::vec::Vec<
            init_direct_read::TopicReadSettings,
        >,
        /// Path of consumer that is used for reading by this session.
        #[prost(string, tag = "3")]
        pub consumer: ::prost::alloc::string::String,
    }
    /// Nested message and enum types in `InitDirectRead`.
    pub mod init_direct_read {
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct TopicReadSettings {
            /// Topic path.
            #[prost(string, tag = "1")]
            pub path: ::prost::alloc::string::String,
        }
    }
    /// Command from client to create and start a direct read partition session.
    /// Server should not send a response to the command.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StartDirectReadPartitionSession {
        /// Partition session identifier.
        #[prost(int64, tag = "1")]
        pub partition_session_id: i64,
        /// Upper bound for read request identifiers.
        #[prost(int64, tag = "2")]
        pub last_direct_read_id: i64,
        /// Partition generation.
        #[prost(int64, tag = "3")]
        pub generation: i64,
    }
    /// Command from server to stop a direct read partition session.
    /// Client should not send a response to the command.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StopDirectReadPartitionSession {
        /// The reason for the stop.
        #[prost(enumeration = "super::super::status_ids::StatusCode", tag = "1")]
        pub status: i32,
        /// Issues if any.
        #[prost(message, repeated, tag = "2")]
        pub issues: ::prost::alloc::vec::Vec<super::super::issue::IssueMessage>,
        /// Partition session identifier.
        #[prost(int64, tag = "3")]
        pub partition_session_id: i64,
    }
    /// Messages that have been read directly from the partition node.
    /// It's a response to StreamRead.ReadRequest
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DirectReadResponse {
        /// Partition session identifier.
        #[prost(int64, tag = "1")]
        pub partition_session_id: i64,
        /// Read request identifier.
        #[prost(int64, tag = "2")]
        pub direct_read_id: i64,
        /// Messages data
        #[prost(message, optional, tag = "3")]
        pub partition_data: ::core::option::Option<
            super::stream_read_message::read_response::PartitionData,
        >,
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionIdentity {
    /// Transaction identifier from TableService.
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    /// Session identifier from TableService.
    #[prost(string, tag = "2")]
    pub session: ::prost::alloc::string::String,
}
/// Add offsets to transaction request sent from client to server.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateOffsetsInTransactionRequest {
    #[prost(message, optional, tag = "1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    #[prost(message, optional, tag = "2")]
    pub tx: ::core::option::Option<TransactionIdentity>,
    /// Ranges of offsets by topics.
    #[prost(message, repeated, tag = "3")]
    pub topics: ::prost::alloc::vec::Vec<
        update_offsets_in_transaction_request::TopicOffsets,
    >,
    #[prost(string, tag = "4")]
    pub consumer: ::prost::alloc::string::String,
}
/// Nested message and enum types in `UpdateOffsetsInTransactionRequest`.
pub mod update_offsets_in_transaction_request {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct TopicOffsets {
        /// Topic path.
        #[prost(string, tag = "1")]
        pub path: ::prost::alloc::string::String,
        /// Ranges of offsets by partitions.
        #[prost(message, repeated, tag = "2")]
        pub partitions: ::prost::alloc::vec::Vec<topic_offsets::PartitionOffsets>,
    }
    /// Nested message and enum types in `TopicOffsets`.
    pub mod topic_offsets {
        #[derive(serde::Serialize, serde::Deserialize)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct PartitionOffsets {
            /// Partition identifier.
            #[prost(int64, tag = "1")]
            pub partition_id: i64,
            /// List of offset ranges.
            #[prost(message, repeated, tag = "2")]
            pub partition_offsets: ::prost::alloc::vec::Vec<super::super::OffsetsRange>,
        }
    }
}
/// Add offsets to transaction response sent from server to client.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateOffsetsInTransactionResponse {
    /// Result of request will be inside operation.
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// Add offsets to transaction result message that will be inside UpdateOffsetsInTransactionResponse.operation.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateOffsetsInTransactionResult {}
/// Commit offset request sent from client to server.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitOffsetRequest {
    #[prost(message, optional, tag = "1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Topic path of partition.
    #[prost(string, tag = "2")]
    pub path: ::prost::alloc::string::String,
    /// Partition identifier.
    #[prost(int64, tag = "3")]
    pub partition_id: i64,
    /// Path of consumer.
    #[prost(string, tag = "4")]
    pub consumer: ::prost::alloc::string::String,
    /// Processed offset.
    #[prost(int64, tag = "5")]
    pub offset: i64,
}
/// Commit offset response sent from server to client.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitOffsetResponse {
    /// Result of request will be inside operation.
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// Commit offset result message inside CommitOffsetResponse.operation.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitOffsetResult {}
/// message representing statistics by several windows
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MultipleWindowsStat {
    #[prost(int64, tag = "1")]
    pub per_minute: i64,
    #[prost(int64, tag = "2")]
    pub per_hour: i64,
    #[prost(int64, tag = "3")]
    pub per_day: i64,
}
/// Consumer description.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Consumer {
    /// Must have valid not empty name as a key.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    /// Consumer may be marked as 'important'. It means messages for this consumer will never expire due to retention.
    /// User should take care that such consumer never stalls, to prevent running out of disk space.
    /// Flag that this consumer is important.
    #[prost(bool, tag = "2")]
    pub important: bool,
    /// All messages with smaller server written_at timestamp will be skipped.
    #[prost(message, optional, tag = "3")]
    pub read_from: ::core::option::Option<super::super::google::protobuf::Timestamp>,
    /// List of supported codecs by this consumer.
    /// supported_codecs on topic must be contained inside this list.
    /// If empty, codec compatibility check for the consumer is disabled.
    #[prost(message, optional, tag = "5")]
    pub supported_codecs: ::core::option::Option<SupportedCodecs>,
    /// Attributes of consumer
    #[prost(map = "string, string", tag = "6")]
    pub attributes: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// Filled only when requested statistics in Describe*Request.
    #[prost(message, optional, tag = "7")]
    pub consumer_stats: ::core::option::Option<consumer::ConsumerStats>,
}
/// Nested message and enum types in `Consumer`.
pub mod consumer {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ConsumerStats {
        /// Minimal timestamp of last read from partitions.
        #[prost(message, optional, tag = "1")]
        pub min_partitions_last_read_time: ::core::option::Option<
            super::super::super::google::protobuf::Timestamp,
        >,
        /// Maximum of differences between timestamp of read and write timestamp for all messages, read during last minute.
        #[prost(message, optional, tag = "2")]
        pub max_read_time_lag: ::core::option::Option<
            super::super::super::google::protobuf::Duration,
        >,
        /// Maximum of differences between write timestamp and create timestamp for all messages, read during last minute.
        #[prost(message, optional, tag = "3")]
        pub max_write_time_lag: ::core::option::Option<
            super::super::super::google::protobuf::Duration,
        >,
        /// Bytes read statistics.
        #[prost(message, optional, tag = "4")]
        pub bytes_read: ::core::option::Option<super::MultipleWindowsStat>,
    }
}
/// Consumer alter description.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterConsumer {
    /// Must have valid not empty name as a key.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    /// Consumer may be marked as 'important'. It means messages for this consumer will never expire due to retention.
    /// User should take care that such consumer never stalls, to prevent running out of disk space.
    /// Flag that this consumer is important.
    #[prost(bool, optional, tag = "2")]
    pub set_important: ::core::option::Option<bool>,
    /// All messages with smaller server written_at timestamp will be skipped.
    #[prost(message, optional, tag = "3")]
    pub set_read_from: ::core::option::Option<super::super::google::protobuf::Timestamp>,
    /// List of supported codecs by this consumer.
    /// supported_codecs on topic must be contained inside this list.
    /// If empty, codec compatibility check for the consumer is disabled.
    #[prost(message, optional, tag = "5")]
    pub set_supported_codecs: ::core::option::Option<SupportedCodecs>,
    /// User and server attributes of consumer. Server attributes starts from "_" and will be validated by server.
    /// Leave the value blank to drop an attribute.
    #[prost(map = "string, string", tag = "6")]
    pub alter_attributes: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
}
/// Partitioning settings for topic.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitioningSettings {
    /// Minimum partition count auto merge would stop working at.
    /// Zero value means default - 1.
    #[prost(int64, tag = "1")]
    pub min_active_partitions: i64,
    /// Limit for total partition count, including active (open for write) and read-only partitions.
    /// Zero value means default - 100.
    #[prost(int64, tag = "2")]
    pub partition_count_limit: i64,
}
/// Partitioning settings for topic.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterPartitioningSettings {
    /// Minimum partition count auto merge would stop working at.
    /// Zero value means default - 1.
    #[prost(int64, optional, tag = "1")]
    pub set_min_active_partitions: ::core::option::Option<i64>,
    /// Limit for total partition count, including active (open for write) and read-only partitions.
    /// Zero value means default - 100.
    #[prost(int64, optional, tag = "2")]
    pub set_partition_count_limit: ::core::option::Option<i64>,
}
/// Create topic request sent from client to server.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateTopicRequest {
    #[prost(message, optional, tag = "1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Topic path.
    #[prost(string, tag = "2")]
    pub path: ::prost::alloc::string::String,
    /// Settings for partitioning
    #[prost(message, optional, tag = "3")]
    pub partitioning_settings: ::core::option::Option<PartitioningSettings>,
    /// Retention settings.
    /// Currently, only one limit may be set, so other should not be set.
    /// How long data in partition should be stored. Must be greater than 0 and less than limit for this database.
    /// Default limit - 36 hours.
    #[prost(message, optional, tag = "4")]
    pub retention_period: ::core::option::Option<
        super::super::google::protobuf::Duration,
    >,
    /// How much data in partition should be stored. Must be greater than 0 and less than limit for this database.
    /// Zero value means infinite limit.
    #[prost(int64, tag = "5")]
    pub retention_storage_mb: i64,
    /// List of allowed codecs for writers.
    /// Writes with codec not from this list are forbidden.
    /// If empty, codec compatibility check for the topic is disabled.
    #[prost(message, optional, tag = "7")]
    pub supported_codecs: ::core::option::Option<SupportedCodecs>,
    /// Partition write speed in bytes per second. Must be less than database limit.
    /// Zero value means default limit: 1 MB per second.
    #[prost(int64, tag = "8")]
    pub partition_write_speed_bytes_per_second: i64,
    /// Burst size for write in partition, in bytes. Must be less than database limit.
    /// Zero value means default limit: 1 MB.
    #[prost(int64, tag = "9")]
    pub partition_write_burst_bytes: i64,
    /// User and server attributes of topic. Server attributes starts from "_" and will be validated by server.
    #[prost(map = "string, string", tag = "10")]
    pub attributes: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// List of consumers for this topic.
    #[prost(message, repeated, tag = "11")]
    pub consumers: ::prost::alloc::vec::Vec<Consumer>,
    /// Metering mode for the topic in a serverless database.
    #[prost(enumeration = "MeteringMode", tag = "12")]
    pub metering_mode: i32,
}
/// Create topic response sent from server to client.
/// If topic is already exists then response status will be "ALREADY_EXISTS".
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateTopicResponse {
    /// Result of request will be inside operation.
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// Create topic result message that will be inside CreateTopicResponse.operation.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateTopicResult {}
/// Topic partition location
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionLocation {
    /// Node identificator.
    #[prost(int32, tag = "1")]
    pub node_id: i32,
    /// Partition generation.
    #[prost(int64, tag = "2")]
    pub generation: i64,
}
/// Describe topic request sent from client to server.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeTopicRequest {
    #[prost(message, optional, tag = "1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Topic path.
    #[prost(string, tag = "2")]
    pub path: ::prost::alloc::string::String,
    /// Include topic statistics.
    #[prost(bool, tag = "3")]
    pub include_stats: bool,
    /// Include partition location.
    #[prost(bool, tag = "4")]
    pub include_location: bool,
}
/// Describe topic response sent from server to client.
/// If topic is not existed then response status will be "SCHEME_ERROR".
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeTopicResponse {
    /// Result of request will be inside operation.
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// Describe topic result message that will be inside DescribeTopicResponse.operation.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeTopicResult {
    /// Description of scheme object.
    #[prost(message, optional, tag = "1")]
    pub self_: ::core::option::Option<super::scheme::Entry>,
    /// Settings for partitioning
    #[prost(message, optional, tag = "2")]
    pub partitioning_settings: ::core::option::Option<PartitioningSettings>,
    /// Partitions description.
    #[prost(message, repeated, tag = "3")]
    pub partitions: ::prost::alloc::vec::Vec<describe_topic_result::PartitionInfo>,
    /// Retention settings.
    /// Currently, only one limit may be set, so other should not be set.
    /// How long data in partition should be stored.
    #[prost(message, optional, tag = "4")]
    pub retention_period: ::core::option::Option<
        super::super::google::protobuf::Duration,
    >,
    /// How much data in partition should be stored.
    /// Zero value means infinite limit.
    #[prost(int64, tag = "5")]
    pub retention_storage_mb: i64,
    /// List of allowed codecs for writers.
    /// Writes with codec not from this list are forbidden.
    /// If empty, codec compatibility check for the topic is disabled.
    #[prost(message, optional, tag = "7")]
    pub supported_codecs: ::core::option::Option<SupportedCodecs>,
    /// Partition write speed in bytes per second.
    /// Zero value means default limit: 1 MB per second.
    #[prost(int64, tag = "8")]
    pub partition_write_speed_bytes_per_second: i64,
    #[prost(int64, tag = "14")]
    pub partition_total_read_speed_bytes_per_second: i64,
    #[prost(int64, tag = "15")]
    pub partition_consumer_read_speed_bytes_per_second: i64,
    /// Burst size for write in partition, in bytes.
    /// Zero value means default limit: 1 MB.
    #[prost(int64, tag = "9")]
    pub partition_write_burst_bytes: i64,
    /// User and server attributes of topic. Server attributes starts from "_" and will be validated by server.
    #[prost(map = "string, string", tag = "10")]
    pub attributes: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// List of consumers for this topic.
    #[prost(message, repeated, tag = "11")]
    pub consumers: ::prost::alloc::vec::Vec<Consumer>,
    /// Metering settings.
    #[prost(enumeration = "MeteringMode", tag = "12")]
    pub metering_mode: i32,
    /// Statistics of topic.
    #[prost(message, optional, tag = "13")]
    pub topic_stats: ::core::option::Option<describe_topic_result::TopicStats>,
}
/// Nested message and enum types in `DescribeTopicResult`.
pub mod describe_topic_result {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct PartitionInfo {
        /// Partition identifier.
        #[prost(int64, tag = "1")]
        pub partition_id: i64,
        /// Is partition open for write.
        #[prost(bool, tag = "2")]
        pub active: bool,
        /// Ids of partitions which was formed when this partition was split or merged.
        #[prost(int64, repeated, tag = "3")]
        pub child_partition_ids: ::prost::alloc::vec::Vec<i64>,
        /// Ids of partitions from which this partition was formed by split or merge.
        #[prost(int64, repeated, tag = "4")]
        pub parent_partition_ids: ::prost::alloc::vec::Vec<i64>,
        /// Stats for partition, filled only when include_stats in request is true.
        #[prost(message, optional, tag = "5")]
        pub partition_stats: ::core::option::Option<super::PartitionStats>,
        /// Partition location, filled only when include_location in request is true.
        #[prost(message, optional, tag = "6")]
        pub partition_location: ::core::option::Option<super::PartitionLocation>,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct TopicStats {
        /// Approximate size of topic.
        #[prost(int64, tag = "1")]
        pub store_size_bytes: i64,
        /// Minimum of timestamps of last write among all partitions.
        #[prost(message, optional, tag = "2")]
        pub min_last_write_time: ::core::option::Option<
            super::super::super::google::protobuf::Timestamp,
        >,
        /// Maximum of differences between write timestamp and create timestamp for all messages, written during last minute.
        #[prost(message, optional, tag = "3")]
        pub max_write_time_lag: ::core::option::Option<
            super::super::super::google::protobuf::Duration,
        >,
        /// How much bytes were written statistics.
        #[prost(message, optional, tag = "4")]
        pub bytes_written: ::core::option::Option<super::MultipleWindowsStat>,
    }
}
/// Describe partition request sent from client to server.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribePartitionRequest {
    #[prost(message, optional, tag = "1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Topic path.
    #[prost(string, tag = "2")]
    pub path: ::prost::alloc::string::String,
    /// Partition identifier.
    #[prost(int64, tag = "3")]
    pub partition_id: i64,
    /// Include partition statistics.
    #[prost(bool, tag = "4")]
    pub include_stats: bool,
    /// Include partition location.
    #[prost(bool, tag = "5")]
    pub include_location: bool,
}
/// Describe partition response sent from server to client.
/// If topic is not existed then response status will be "SCHEME_ERROR".
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribePartitionResponse {
    /// Result of request will be inside operation.
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// Describe partition result message that will be inside DescribeTopicResponse.operation.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribePartitionResult {
    /// Partitions description.
    #[prost(message, optional, tag = "1")]
    pub partition: ::core::option::Option<describe_topic_result::PartitionInfo>,
}
/// Describe topic's consumer request sent from client to server.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeConsumerRequest {
    #[prost(message, optional, tag = "1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Topic path.
    #[prost(string, tag = "2")]
    pub path: ::prost::alloc::string::String,
    /// Consumer name;
    #[prost(string, tag = "3")]
    pub consumer: ::prost::alloc::string::String,
    /// Include consumer statistics.
    #[prost(bool, tag = "4")]
    pub include_stats: bool,
    /// Include partition location.
    #[prost(bool, tag = "5")]
    pub include_location: bool,
}
/// Describe topic's consumer response sent from server to client.
/// If topic is not existed then response status will be "SCHEME_ERROR".
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeConsumerResponse {
    /// Result of request will be inside operation.
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// Describe topic's consumer result message that will be inside DescribeConsumerResponse.operation.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DescribeConsumerResult {
    /// Description of scheme object.
    #[prost(message, optional, tag = "1")]
    pub self_: ::core::option::Option<super::scheme::Entry>,
    #[prost(message, optional, tag = "2")]
    pub consumer: ::core::option::Option<Consumer>,
    #[prost(message, repeated, tag = "3")]
    pub partitions: ::prost::alloc::vec::Vec<describe_consumer_result::PartitionInfo>,
}
/// Nested message and enum types in `DescribeConsumerResult`.
pub mod describe_consumer_result {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct PartitionInfo {
        /// Partition identifier.
        #[prost(int64, tag = "1")]
        pub partition_id: i64,
        /// Is partition open for write.
        #[prost(bool, tag = "2")]
        pub active: bool,
        /// Ids of partitions which was formed when this partition was split or merged.
        #[prost(int64, repeated, tag = "3")]
        pub child_partition_ids: ::prost::alloc::vec::Vec<i64>,
        /// Ids of partitions from which this partition was formed by split or merge.
        #[prost(int64, repeated, tag = "4")]
        pub parent_partition_ids: ::prost::alloc::vec::Vec<i64>,
        /// Stats for partition, filled only when include_stats in request is true.
        #[prost(message, optional, tag = "5")]
        pub partition_stats: ::core::option::Option<super::PartitionStats>,
        /// Stats for consumer of this partition, filled only when include_stats in request is true.
        #[prost(message, optional, tag = "6")]
        pub partition_consumer_stats: ::core::option::Option<PartitionConsumerStats>,
        /// Partition location, filled only when include_location in request is true.
        #[prost(message, optional, tag = "7")]
        pub partition_location: ::core::option::Option<super::PartitionLocation>,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct PartitionConsumerStats {
        /// Last read offset from this partition.
        #[prost(int64, tag = "1")]
        pub last_read_offset: i64,
        /// Committed offset for this partition.
        #[prost(int64, tag = "2")]
        pub committed_offset: i64,
        /// Reading this partition read session identifier.
        #[prost(string, tag = "3")]
        pub read_session_id: ::prost::alloc::string::String,
        /// Timestamp of providing this partition to this session by server.
        #[prost(message, optional, tag = "4")]
        pub partition_read_session_create_time: ::core::option::Option<
            super::super::super::google::protobuf::Timestamp,
        >,
        /// Timestamp of last read from this partition.
        #[prost(message, optional, tag = "5")]
        pub last_read_time: ::core::option::Option<
            super::super::super::google::protobuf::Timestamp,
        >,
        /// Maximum of differences between timestamp of read and write timestamp for all messages, read during last minute.
        #[prost(message, optional, tag = "6")]
        pub max_read_time_lag: ::core::option::Option<
            super::super::super::google::protobuf::Duration,
        >,
        /// Maximum of differences between write timestamp and create timestamp for all messages, read during last minute.
        #[prost(message, optional, tag = "7")]
        pub max_write_time_lag: ::core::option::Option<
            super::super::super::google::protobuf::Duration,
        >,
        /// How much bytes were read during several windows statistics from this partition.
        #[prost(message, optional, tag = "8")]
        pub bytes_read: ::core::option::Option<super::MultipleWindowsStat>,
        /// Read session name, provided by client.
        #[prost(string, tag = "11")]
        pub reader_name: ::prost::alloc::string::String,
        /// Host where read session connected.
        #[prost(int32, tag = "12")]
        pub connection_node_id: i32,
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionStats {
    /// Partition contains messages with offsets in range [start, end).
    #[prost(message, optional, tag = "1")]
    pub partition_offsets: ::core::option::Option<OffsetsRange>,
    /// Approximate size of partition.
    #[prost(int64, tag = "2")]
    pub store_size_bytes: i64,
    /// Timestamp of last write.
    #[prost(message, optional, tag = "3")]
    pub last_write_time: ::core::option::Option<
        super::super::google::protobuf::Timestamp,
    >,
    /// Maximum of differences between write timestamp and create timestamp for all messages, written during last minute.
    #[prost(message, optional, tag = "4")]
    pub max_write_time_lag: ::core::option::Option<
        super::super::google::protobuf::Duration,
    >,
    /// How much bytes were written during several windows in this partition.
    #[prost(message, optional, tag = "5")]
    pub bytes_written: ::core::option::Option<MultipleWindowsStat>,
    /// Partition host. Useful for debugging purposes.
    /// Use PartitionLocation
    #[deprecated]
    #[prost(int32, tag = "8")]
    pub partition_node_id: i32,
}
/// Update existing topic request sent from client to server.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterTopicRequest {
    #[prost(message, optional, tag = "1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Topic path.
    #[prost(string, tag = "2")]
    pub path: ::prost::alloc::string::String,
    /// partitioning_settings
    #[prost(message, optional, tag = "3")]
    pub alter_partitioning_settings: ::core::option::Option<AlterPartitioningSettings>,
    /// Retention settings.
    /// Currently, only one limit may be set, so other should not be set.
    /// How long data in partition should be stored. Must be greater than 0 and less than limit for this database.
    /// Default limit - 36 hours.
    #[prost(message, optional, tag = "4")]
    pub set_retention_period: ::core::option::Option<
        super::super::google::protobuf::Duration,
    >,
    /// How much data in partition should be stored. Must be greater than 0 and less than limit for this database.
    #[prost(int64, optional, tag = "5")]
    pub set_retention_storage_mb: ::core::option::Option<i64>,
    /// List of allowed codecs for writers.
    /// Writes with codec not from this list are forbidden.
    /// If empty, codec compatibility check for the topic is disabled.
    #[prost(message, optional, tag = "7")]
    pub set_supported_codecs: ::core::option::Option<SupportedCodecs>,
    /// Partition write speed in bytes per second. Must be less than database limit. Default limit - 1 MB/s.
    #[prost(int64, optional, tag = "8")]
    pub set_partition_write_speed_bytes_per_second: ::core::option::Option<i64>,
    /// Burst size for write in partition, in bytes. Must be less than database limit. Default limit - 1 MB.
    #[prost(int64, optional, tag = "9")]
    pub set_partition_write_burst_bytes: ::core::option::Option<i64>,
    /// User and server attributes of topic. Server attributes starts from "_" and will be validated by server.
    /// Leave the value blank to drop an attribute.
    #[prost(map = "string, string", tag = "10")]
    pub alter_attributes: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// Add consumers.
    #[prost(message, repeated, tag = "11")]
    pub add_consumers: ::prost::alloc::vec::Vec<Consumer>,
    /// Remove consumers (by their names)
    #[prost(string, repeated, tag = "12")]
    pub drop_consumers: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Alter consumers
    #[prost(message, repeated, tag = "13")]
    pub alter_consumers: ::prost::alloc::vec::Vec<AlterConsumer>,
    /// Set metering mode for topic in serverless database.
    #[prost(enumeration = "MeteringMode", tag = "14")]
    pub set_metering_mode: i32,
}
/// Update topic response sent from server to client.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterTopicResponse {
    /// Result of request will be inside operation.
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// Update topic result message that will be inside UpdateTopicResponse.operation.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterTopicResult {}
/// Drop topic request sent from client to server.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropTopicRequest {
    #[prost(message, optional, tag = "1")]
    pub operation_params: ::core::option::Option<super::operations::OperationParams>,
    /// Topic path.
    #[prost(string, tag = "2")]
    pub path: ::prost::alloc::string::String,
}
/// Drop topic response sent from server to client.
/// If topic not exists then response status will be "SCHEME_ERROR".
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropTopicResponse {
    /// Result of request will be inside operation.
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<super::operations::Operation>,
}
/// Drop topic result message that will be inside DropTopicResponse.operation.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropTopicResult {}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Codec {
    Unspecified = 0,
    Raw = 1,
    Gzip = 2,
    Lzop = 3,
    Zstd = 4,
    /// User-defined codecs from 10000 to 19999
    Custom = 10000,
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
            Codec::Custom => "CODEC_CUSTOM",
        }
    }
}
/// Metering mode specifies the method used to determine consumption of resources by the topic.
/// This settings will have an effect only in a serverless database.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum MeteringMode {
    /// Use default
    Unspecified = 0,
    /// Metering based on resource reservation
    ReservedCapacity = 1,
    /// Metering based on actual consumption. Default.
    RequestUnits = 2,
}
impl MeteringMode {
    /// String value of the enum field names used in the ProtoBuf definition.
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            MeteringMode::Unspecified => "METERING_MODE_UNSPECIFIED",
            MeteringMode::ReservedCapacity => "METERING_MODE_RESERVED_CAPACITY",
            MeteringMode::RequestUnits => "METERING_MODE_REQUEST_UNITS",
        }
    }
}