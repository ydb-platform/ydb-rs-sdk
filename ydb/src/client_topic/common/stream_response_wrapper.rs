use crate::grpc::proto_issues_to_ydb_issues;

use crate::grpc_wrapper::raw_errors::{RawError, RawResult};

use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::topic::stream_write_message;
use ydb_grpc::ydb_proto::topic::stream_write_message::{from_server, FromServer};

pub(crate) trait StreamingResponseTrait<ServerMessageT> {
    fn extract_response_body(self) -> RawResult<ServerMessageT>;
}

pub(crate) fn create_server_status_error(message: FromServer) -> RawError {
    RawError::YdbStatus(crate::errors::YdbStatusError {
        message: "".to_string(), // TODO: what message?
        operation_status: message.status,
        issues: proto_issues_to_ydb_issues(message.issues),
    })
}

impl StreamingResponseTrait<from_server::ServerMessage> for stream_write_message::FromServer {
    fn extract_response_body(self) -> RawResult<from_server::ServerMessage> {
        if self.status != StatusCode::Success as i32 {
            return Err(create_server_status_error(self));
        }
        let unpacked_server_message = self.server_message.ok_or(RawError::Custom(
            "Server message is absent in streaming response body".to_string(),
        ))?;

        Ok(unpacked_server_message)
    }
}
