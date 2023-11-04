use ydb_grpc::ydb_proto::topic::stream_write_message::from_server::ServerMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::FromServer;
use ydb_grpc::ydb_proto::topic::UpdateTokenResponse;
use ydb_grpc::ydb_proto::{status_ids::StatusCode, topic::stream_write_message::WriteResponse};

use crate::grpc_wrapper::{
    grpc::proto_issues_to_ydb_issues,
    raw_errors::{RawError, RawResult},
};

use crate::grpc_wrapper::raw_topic_service::stream_write::init::RawInitResponse;

pub(crate) mod init;

#[derive(serde::Serialize)]
pub(crate) enum RawServerMessage {
    Init(RawInitResponse),
    Write(WriteResponse),
    UpdateToken(UpdateTokenResponse),
}

pub(crate) fn create_server_status_error(message: FromServer) -> RawError {
    RawError::YdbStatus(crate::errors::YdbStatusError {
        message: "".to_string(), // TODO: what message?
        operation_status: message.status,
        issues: proto_issues_to_ydb_issues(message.issues),
    })
}

impl TryFrom<FromServer> for RawServerMessage {
    type Error = RawError;

    fn try_from(value: FromServer) -> RawResult<Self> {
        if value.status != StatusCode::Success as i32 {
            return Err(create_server_status_error(value));
        }

        let message = value.server_message.ok_or(RawError::Custom(
            "Server message is absent in streaming response body".to_string(),
        ))?;

        let raw_message = match message {
            ServerMessage::InitResponse(response) => RawServerMessage::Init(response.try_into()?),
            ServerMessage::WriteResponse(response) => RawServerMessage::Write(response),
            ServerMessage::UpdateTokenResponse(response) => RawServerMessage::UpdateToken(response),
        };

        Ok(raw_message)
    }
}
