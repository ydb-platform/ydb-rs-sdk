use crate::{YdbError, YdbStatusError};

impl YdbError {
    pub(crate) fn status_code(&self) -> &'static str {
        match self {
            YdbError::YdbStatusError(status) => ydb_status_code(status),
            YdbError::TransportGRPCStatus(status) => grpc_status_code(status.code()),
            YdbError::DeadlineExceeded => "DEADLINE_EXCEEDED",
            _ => "UNKNOWN",
        }
    }
}

fn ydb_status_code(error: &YdbStatusError) -> &'static str {
    error
        .operation_status()
        .map(|status| status.as_str_name())
        .unwrap_or("UNKNOWN")
}

fn grpc_status_code(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "OK",
        tonic::Code::Cancelled => "CANCELLED",
        tonic::Code::Unknown => "UNKNOWN",
        tonic::Code::InvalidArgument => "INVALID_ARGUMENT",
        tonic::Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        tonic::Code::NotFound => "NOT_FOUND",
        tonic::Code::AlreadyExists => "ALREADY_EXISTS",
        tonic::Code::PermissionDenied => "PERMISSION_DENIED",
        tonic::Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        tonic::Code::FailedPrecondition => "FAILED_PRECONDITION",
        tonic::Code::Aborted => "ABORTED",
        tonic::Code::OutOfRange => "OUT_OF_RANGE",
        tonic::Code::Unimplemented => "UNIMPLEMENTED",
        tonic::Code::Internal => "INTERNAL",
        tonic::Code::Unavailable => "UNAVAILABLE",
        tonic::Code::DataLoss => "DATA_LOSS",
        tonic::Code::Unauthenticated => "UNAUTHENTICATED",
    }
}
