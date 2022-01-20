use crate::errors::NeedRetry::IdempotentOnly;
use std::fmt::{Debug, Display, Formatter};
use std::string::FromUtf8Error;
use std::sync::Arc;
use tokio::sync::AcquireError;
use url::ParseError;

pub type Result<T> = std::result::Result<T, Error>;
pub type ResultWithCustomerErr<T> = std::result::Result<T, YdbOrCustomerError>;

#[derive(Clone)]
pub enum YdbOrCustomerError {
    YDB(Error),
    Customer(Arc<Box<dyn std::error::Error>>),
}

impl YdbOrCustomerError {
    #[allow(dead_code)]
    pub fn from_mess<T: Into<String>>(s: T) -> Self {
        return Self::Customer(Arc::new(Box::new(Error::Custom(s.into()))));
    }

    #[allow(dead_code)]
    pub fn from_err<T: std::error::Error + 'static>(err: T) -> Self {
        return Self::Customer(Arc::new(Box::new(err)));
    }
}

impl Debug for YdbOrCustomerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        return match self {
            Self::YDB(err) => Debug::fmt(err, f),
            Self::Customer(err) => Debug::fmt(err, f),
        };
    }
}

impl Display for YdbOrCustomerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        return match self {
            Self::YDB(err) => Display::fmt(err, f),
            Self::Customer(err) => Display::fmt(err, f),
        };
    }
}

impl From<Error> for YdbOrCustomerError {
    fn from(e: Error) -> Self {
        return Self::YDB(e);
    }
}

pub(crate) enum NeedRetry {
    True,           // operation guarantee to not completed, error is temporary, need retry
    IdempotentOnly, // operation in unknown state - it may be completed or not, error temporary. Operation may be auto retry for idempotent operations only.
    False, // operation is completed or error is stable (for example yql syntaxt errror) and no need retry
}

#[derive(Clone, Debug)]
pub enum Error {
    Custom(String),
    InternalError(String),
    TransportDial(Arc<tonic::transport::Error>),
    Transport(String),
    TransportGRPCStatus(Arc<tonic::Status>),
    YdbOperation(YdbOperationError),
}

#[derive(Clone, Debug)]
pub struct YdbOperationError {
    #[allow(dead_code)]
    pub(crate) message: String,
    pub(crate) operation_status: i32,
}

impl Error {
    #[allow(dead_code)]
    pub fn from_str(s: &str) -> Error {
        return Error::Custom(s.to_string());
    }
    pub(crate) fn need_retry(&self) -> NeedRetry {
        match self {
            Self::Custom(_) => NeedRetry::False,
            Self::InternalError(_) => NeedRetry::False,
            Self::TransportDial(_) => NeedRetry::True,
            Self::Transport(_) => IdempotentOnly, // TODO: check when transport error created
            Self::TransportGRPCStatus(status) => {
                use tonic::Code;
                match status.code() {
                    Code::Aborted | Code::ResourceExhausted => NeedRetry::True,
                    Code::Internal | Code::Cancelled | Code::Unavailable => {
                        NeedRetry::IdempotentOnly
                    }
                    _ => NeedRetry::False,
                }
            }
            Self::YdbOperation(ydb_err) => {
                use ydb_protobuf::generated::ydb::status_ids::StatusCode;
                if let Some(status) = StatusCode::from_i32(ydb_err.operation_status) {
                    match status {
                        StatusCode::Aborted
                        | StatusCode::Unavailable
                        | StatusCode::Overloaded
                        | StatusCode::BadSession
                        | StatusCode::SessionBusy => NeedRetry::True,
                        StatusCode::Undetermined => NeedRetry::IdempotentOnly,
                        _ => NeedRetry::False,
                    }
                } else {
                    NeedRetry::False
                }
            }
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self::Debug::fmt(self, f)
    }
}

impl std::error::Error for Error {}

impl From<http::Error> for Error {
    fn from(e: http::Error) -> Self {
        return Error::Custom(e.to_string());
    }
}

impl From<prost::DecodeError> for Error {
    fn from(e: prost::DecodeError) -> Self {
        return Error::Custom(e.to_string());
    }
}

impl From<std::env::VarError> for Error {
    fn from(e: std::env::VarError) -> Self {
        return Error::Custom(e.to_string());
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        return Error::Custom(e.to_string());
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        return Self::Custom(s.to_string());
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(e: std::num::TryFromIntError) -> Self {
        return Error::Custom(e.to_string());
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(e: FromUtf8Error) -> Self {
        return Error::Custom(e.to_string());
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        return Error::Custom(e.to_string());
    }
}

impl From<strum::ParseError> for Error {
    fn from(e: strum::ParseError) -> Self {
        return Error::Custom(e.to_string());
    }
}

impl From<tonic::codegen::http::uri::InvalidUri> for Error {
    fn from(e: tonic::codegen::http::uri::InvalidUri) -> Self {
        return Error::Custom(e.to_string());
    }
}

impl From<tokio::sync::AcquireError> for Error {
    fn from(e: AcquireError) -> Self {
        return Error::Custom(e.to_string());
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(e: tokio::sync::oneshot::error::RecvError) -> Self {
        return Error::Custom(e.to_string());
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(e: tonic::transport::Error) -> Self {
        return Error::Transport(e.to_string());
    }
}

impl From<tonic::Status> for Error {
    fn from(e: tonic::Status) -> Self {
        return Error::TransportGRPCStatus(Arc::new(e));
    }
}

impl From<url::ParseError> for Error {
    fn from(e: ParseError) -> Self {
        return Self::Custom(e.to_string());
    }
}

impl From<ydb_protobuf::generated::ydb::operations::Operation> for Error {
    fn from(op: ydb_protobuf::generated::ydb::operations::Operation) -> Self {
        return Error::YdbOperation(YdbOperationError {
            message: format!("{:?}", &op),
            operation_status: op.status,
        });
    }
}
