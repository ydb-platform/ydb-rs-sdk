use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::topic::stream_write_message::{from_server, FromServer};

use crate::grpc_wrapper::{
    grpc::proto_issues_to_ydb_issues,
    raw_errors::{RawError, RawResult},
};

pub(crate) fn create_server_status_error(message: FromServer) -> RawError {
    RawError::YdbStatus(crate::errors::YdbStatusError {
        message: "".to_string(), // TODO: what message?
        operation_status: message.status,
        issues: proto_issues_to_ydb_issues(message.issues),
    })
}

pub(crate) fn from_grpc_to_server_message(
    value: FromServer,
) -> RawResult<from_server::ServerMessage> {
    if value.status != StatusCode::Success as i32 {
        return Err(create_server_status_error(value));
    }

    value.server_message.ok_or(RawError::Custom(
        "Server message is absent in streaming response body".to_string(),
    ))
}
