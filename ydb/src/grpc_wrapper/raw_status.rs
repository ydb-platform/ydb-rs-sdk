pub(crate) enum RawStatusCode {
    Unspecified,
    Success,
    BadRequest,
    Unauthorized,
    InternalError,
    ExternalError,
    Aborted,
    Unavailable,
    Overloaded,
    SchemeError,
    GenericError,
    Timeout,
    BadSession,
    PreconditionFailed,
    AlreadyExists,
    NotFound,
    SessionExpired,
    Cancelled,
    Undetermined,
    Unsupported,
    SessionBusy,
    Unknown(i32),
}

impl From<i32> for RawStatusCode {
    fn from(value: i32) -> Self {
        if let Some(status) = ydb_grpc::ydb_proto::status_ids::StatusCode::from_i32(value) {
            match status {
                ydb_grpc::ydb_proto::status_ids::StatusCode::Unspecified => Self::Unspecified,
                ydb_grpc::ydb_proto::status_ids::StatusCode::Success => Self::Success,
                ydb_grpc::ydb_proto::status_ids::StatusCode::BadRequest => Self::BadRequest,
                ydb_grpc::ydb_proto::status_ids::StatusCode::Unauthorized => Self::Unauthorized,
                ydb_grpc::ydb_proto::status_ids::StatusCode::InternalError => Self::InternalError,
                ydb_grpc::ydb_proto::status_ids::StatusCode::Aborted => Self::Aborted,
                ydb_grpc::ydb_proto::status_ids::StatusCode::Unavailable => Self::Unavailable,
                ydb_grpc::ydb_proto::status_ids::StatusCode::Overloaded => Self::Overloaded,
                ydb_grpc::ydb_proto::status_ids::StatusCode::SchemeError => Self::SchemeError,
                ydb_grpc::ydb_proto::status_ids::StatusCode::GenericError => Self::GenericError,
                ydb_grpc::ydb_proto::status_ids::StatusCode::Timeout => Self::Timeout,
                ydb_grpc::ydb_proto::status_ids::StatusCode::BadSession => Self::BadSession,
                ydb_grpc::ydb_proto::status_ids::StatusCode::PreconditionFailed => {
                    Self::PreconditionFailed
                }
                ydb_grpc::ydb_proto::status_ids::StatusCode::AlreadyExists => Self::AlreadyExists,
                ydb_grpc::ydb_proto::status_ids::StatusCode::NotFound => Self::NotFound,
                ydb_grpc::ydb_proto::status_ids::StatusCode::SessionExpired => Self::SessionExpired,
                ydb_grpc::ydb_proto::status_ids::StatusCode::Cancelled => Self::Cancelled,
                ydb_grpc::ydb_proto::status_ids::StatusCode::Undetermined => Self::Undetermined,
                ydb_grpc::ydb_proto::status_ids::StatusCode::Unsupported => Self::Unsupported,
                ydb_grpc::ydb_proto::status_ids::StatusCode::SessionBusy => Self::SessionBusy,
                ydb_grpc::ydb_proto::status_ids::StatusCode::ExternalError => Self::ExternalError,
            }
        } else {
            Self::Unknown(value)
        }
    }
}
