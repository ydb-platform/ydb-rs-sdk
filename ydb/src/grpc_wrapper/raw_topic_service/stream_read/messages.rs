use crate::grpc_wrapper::grpc::proto_issues_to_ydb_issues;
use crate::grpc_wrapper::raw_common_types::{Duration, Timestamp};
use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_topic_service::common::codecs::RawCodec;
use crate::grpc_wrapper::raw_topic_service::common::partition::RawOffsetsRange;
use crate::grpc_wrapper::raw_topic_service::common::update_token::{
    RawUpdateTokenRequest, RawUpdateTokenResponse,
};
use crate::YdbStatusError;
use std::collections::{HashMap, VecDeque};
use std::time::UNIX_EPOCH;
use tracing::warn;
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::topic::stream_read_message;
use ydb_grpc::ydb_proto::topic::stream_read_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::stream_read_message::FromServer;

pub(crate) enum RawFromClientOneOf {
    InitRequest(RawInitRequest),
    ReadRequest(RawReadRequest),
    CommitOffsetRequest(RawCommitOffsetRequest),
    StartPartitionSessionResponse(RawStartPartitionSessionResponse),
    StopPartitionSessionResponse(RawStopPartitionSessionResponse),
    UpdateTokenRequest(RawUpdateTokenRequest),
}

impl From<RawFromClientOneOf> for stream_read_message::FromClient {
    fn from(value: RawFromClientOneOf) -> Self {
        Self {
            client_message: Some(value.into()),
        }
    }
}

impl From<RawFromClientOneOf> for stream_read_message::from_client::ClientMessage {
    fn from(value: RawFromClientOneOf) -> Self {
        match value {
            RawFromClientOneOf::InitRequest(init_request) => {
                ClientMessage::InitRequest(init_request.into())
            }
            RawFromClientOneOf::ReadRequest(read_request) => {
                ClientMessage::ReadRequest(read_request.into())
            }
            RawFromClientOneOf::CommitOffsetRequest(commit_offset_request) => {
                ClientMessage::CommitOffsetRequest(commit_offset_request.into())
            }
            RawFromClientOneOf::StartPartitionSessionResponse(start_partition_session_response) => {
                ClientMessage::StartPartitionSessionResponse(
                    start_partition_session_response.into(),
                )
            }
            RawFromClientOneOf::StopPartitionSessionResponse(stop_partition_session_response) => {
                ClientMessage::StopPartitionSessionResponse(stop_partition_session_response.into())
            }
            RawFromClientOneOf::UpdateTokenRequest(update_token_request) => {
                ClientMessage::UpdateTokenRequest(update_token_request.into())
            }
        }
    }
}

pub(crate) enum RawFromServer {
    InitResponse(RawInitResponse),
    ReadResponse(RawReadResponse),
    StartPartitionSessionRequest(RawStartPartitionSessionRequest),
    StopPartitionSessionRequest(RawStopPartitionSessionRequest),
    UpdateTokenResponse(RawUpdateTokenResponse),
    UnsupportedMessage(String),
}

impl TryFrom<stream_read_message::FromServer> for RawFromServer {
    type Error = RawError;

    fn try_from(value: FromServer) -> Result<Self, Self::Error> {
        if value.status != StatusCode::Success as i32 {
            return Err(RawError::YdbStatus(YdbStatusError {
                message: "".to_string(),
                operation_status: value.status,
                issues: proto_issues_to_ydb_issues(value.issues),
            }));
        }

        let message = value.server_message.ok_or(RawError::Custom(
            "Server message is absent in streaming response body for topic reader stream"
                .to_string(),
        ))?;

        let mess = match message {
            stream_read_message::from_server::ServerMessage::InitResponse(init_response) => {
                RawFromServer::InitResponse(init_response.into())
            }
            stream_read_message::from_server::ServerMessage::ReadResponse(read_response) => {
                RawFromServer::ReadResponse(read_response.into())
            }
            stream_read_message::from_server::ServerMessage::StartPartitionSessionRequest(
                start_partition_session_request,
            ) => {
                RawFromServer::StartPartitionSessionRequest(start_partition_session_request.into())
            }
            stream_read_message::from_server::ServerMessage::StopPartitionSessionRequest(
                stop_partition_session_request,
            ) => RawFromServer::StopPartitionSessionRequest(stop_partition_session_request.into()),
            stream_read_message::from_server::ServerMessage::UpdateTokenResponse(
                update_token_response,
            ) => RawFromServer::UpdateTokenResponse(update_token_response.into()),
            other => {
                RawFromServer::UnsupportedMessage(serde_json::to_string(&other).map_err(|err| {
                    RawError::Custom(format!(
                        "failed json serialize while marshal unknown message in topic reader: {}",
                        err
                    ))
                })?)
            }
        };

        Ok(mess)
    }
}

pub(crate) struct RawInitRequest {
    pub topics_read_settings: Vec<RawTopicReadSettings>,
    pub consumer: String,
    pub reader_name: String,
}

impl From<RawInitRequest> for stream_read_message::InitRequest {
    fn from(value: RawInitRequest) -> Self {
        stream_read_message::InitRequest {
            topics_read_settings: value
                .topics_read_settings
                .into_iter()
                .map(|x| x.into())
                .collect(),
            consumer: value.consumer,
            reader_name: value.reader_name,
            direct_read: false,
        }
    }
}

pub(crate) struct RawTopicReadSettings {
    pub path: String,
    pub partition_ids: Vec<i64>,
    pub max_lag: Option<Duration>,
    pub read_from: Option<Timestamp>,
}

impl From<RawTopicReadSettings> for stream_read_message::init_request::TopicReadSettings {
    fn from(value: RawTopicReadSettings) -> Self {
        stream_read_message::init_request::TopicReadSettings {
            path: value.path,
            partition_ids: value.partition_ids,
            max_lag: value.max_lag.map(|val| val.into()),
            read_from: value.read_from.map(|val| val.into()),
        }
    }
}

#[derive(Debug)]
pub(crate) struct RawInitResponse {
    session_id: String,
}

impl From<stream_read_message::InitResponse> for RawInitResponse {
    fn from(value: stream_read_message::InitResponse) -> Self {
        RawInitResponse {
            session_id: value.session_id,
        }
    }
}

pub(crate) struct RawReadRequest {
    pub bytes_size: i64,
}

impl From<RawReadRequest> for stream_read_message::ReadRequest {
    fn from(value: RawReadRequest) -> Self {
        stream_read_message::ReadRequest {
            bytes_size: value.bytes_size,
        }
    }
}

pub(crate) struct RawReadResponse {
    pub bytes_size: i64,

    pub partition_data: Vec<RawPartitionData>,
}

impl From<stream_read_message::ReadResponse> for RawReadResponse {
    fn from(value: stream_read_message::ReadResponse) -> Self {
        let mut res = RawReadResponse {
            bytes_size: value.bytes_size,
            partition_data: value.partition_data.into_iter().map(|x| x.into()).collect(),
        };

        let set_size = if let Some(last_partition_data) = res.partition_data.last_mut() {
            if let Some(last_batch) = last_partition_data.batches.iter_mut().last() {
                if let Some(last_message_data) = last_batch.message_data.last_mut() {
                    last_message_data.read_session_size_bytes = res.bytes_size;
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        if !set_size {
            warn!(
                "topic reader received empty read response with size bytes: {}",
                res.bytes_size
            )
        }

        res
    }
}

#[derive(Debug)]
pub(crate) struct RawPartitionData {
    pub partition_session_id: i64,
    pub batches: VecDeque<RawBatch>,
}

impl From<stream_read_message::read_response::PartitionData> for RawPartitionData {
    fn from(value: stream_read_message::read_response::PartitionData) -> Self {
        RawPartitionData {
            partition_session_id: value.partition_session_id,
            batches: value.batches.into_iter().map(|x| x.into()).collect(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct RawBatch {
    pub producer_id: String,
    pub write_session_meta: HashMap<String, String>,
    pub codec: RawCodec,
    pub written_at: Timestamp,

    pub message_data: Vec<RawMessageData>,
}

impl RawBatch {
    pub fn get_read_session_size(&self) -> i64 {
        self.message_data
            .iter()
            .map(|x| x.read_session_size_bytes)
            .sum()
    }
}

impl From<stream_read_message::read_response::Batch> for RawBatch {
    fn from(value: stream_read_message::read_response::Batch) -> Self {
        RawBatch {
            producer_id: value.producer_id,
            write_session_meta: value.write_session_meta.into_iter().collect(),
            codec: RawCodec { code: value.codec },
            written_at: value
                .written_at
                .map_or(Timestamp::from(UNIX_EPOCH), |x| x.into()),
            message_data: value.message_data.into_iter().map(|x| x.into()).collect(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct RawMessageData {
    pub offset: i64,
    pub seq_no: i64,
    pub created_at: Option<Timestamp>,
    pub uncompressed_size: i64,
    pub data: Vec<u8>,

    pub read_session_size_bytes: i64,
}

impl From<stream_read_message::read_response::MessageData> for RawMessageData {
    fn from(value: stream_read_message::read_response::MessageData) -> Self {
        RawMessageData {
            offset: value.offset,
            seq_no: value.seq_no,
            created_at: value.created_at.map(|x| x.into()),
            uncompressed_size: value.uncompressed_size,
            data: value.data.into_iter().collect(),
            read_session_size_bytes: 0,
        }
    }
}

pub(crate) struct RawCommitOffsetRequest {
    pub commit_offsets: Vec<PartitionCommitOffset>,
}

impl From<RawCommitOffsetRequest> for stream_read_message::CommitOffsetRequest {
    fn from(value: RawCommitOffsetRequest) -> Self {
        stream_read_message::CommitOffsetRequest {
            commit_offsets: value.commit_offsets.into_iter().map(|x| x.into()).collect(),
        }
    }
}

pub(crate) struct PartitionCommitOffset {
    pub partition_session_id: i64,
    pub offsets: Vec<RawOffsetsRange>,
}

impl From<PartitionCommitOffset>
    for stream_read_message::commit_offset_request::PartitionCommitOffset
{
    fn from(value: PartitionCommitOffset) -> Self {
        stream_read_message::commit_offset_request::PartitionCommitOffset {
            partition_session_id: value.partition_session_id,
            offsets: value.offsets.into_iter().map(|x| x.into()).collect(),
        }
    }
}

pub(crate) struct RawStartPartitionSessionRequest {
    pub partition_session: RawPartitionSession,
    pub committed_offset: i64,
}

impl From<stream_read_message::StartPartitionSessionRequest> for RawStartPartitionSessionRequest {
    fn from(value: stream_read_message::StartPartitionSessionRequest) -> Self {
        RawStartPartitionSessionRequest {
            partition_session: value.partition_session.map_or(
                RawPartitionSession {
                    partition_session_id: 0,
                    path: "".to_string(),
                    partition_id: 0,
                },
                |x| x.into(),
            ),
            committed_offset: value.committed_offset,
        }
    }
}

pub(crate) struct RawPartitionSession {
    pub partition_session_id: i64,
    pub path: String,
    pub partition_id: i64,
}

impl From<stream_read_message::PartitionSession> for RawPartitionSession {
    fn from(value: stream_read_message::PartitionSession) -> Self {
        RawPartitionSession {
            partition_session_id: value.partition_session_id,
            path: value.path,
            partition_id: value.partition_id,
        }
    }
}

pub(crate) struct RawStartPartitionSessionResponse {
    pub partition_session_id: i64,
}

impl From<RawStartPartitionSessionResponse> for stream_read_message::StartPartitionSessionResponse {
    fn from(value: RawStartPartitionSessionResponse) -> Self {
        stream_read_message::StartPartitionSessionResponse {
            partition_session_id: value.partition_session_id,
            read_offset: None,
            commit_offset: None,
        }
    }
}

pub(crate) struct RawStopPartitionSessionRequest {
    pub partition_session_id: i64,
    pub graceful: bool,
}

impl From<stream_read_message::StopPartitionSessionRequest> for RawStopPartitionSessionRequest {
    fn from(value: stream_read_message::StopPartitionSessionRequest) -> Self {
        RawStopPartitionSessionRequest {
            partition_session_id: value.partition_session_id,
            graceful: value.graceful,
        }
    }
}

pub(crate) struct RawStopPartitionSessionResponse {
    pub partition_session_id: i64,
}

impl From<RawStopPartitionSessionResponse> for stream_read_message::StopPartitionSessionResponse {
    fn from(value: RawStopPartitionSessionResponse) -> Self {
        stream_read_message::StopPartitionSessionResponse {
            partition_session_id: value.partition_session_id,
            graceful: false,
        }
    }
}
