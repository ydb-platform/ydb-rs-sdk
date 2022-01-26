use crate::errors::NeedRetry::IdempotentOnly;
use std::fmt::{Debug, Display, Formatter};
use std::string::FromUtf8Error;
use std::sync::Arc;
use std::time::SystemTimeError;
use tokio::sync::AcquireError;
use url::ParseError;

pub type YdbResult<T> = std::result::Result<T, YdbError>;
pub type YdbResultWithCustomerErr<T> = std::result::Result<T, YdbOrCustomerError>;

#[derive(Clone)]
pub enum YdbOrCustomerError {
    YDB(YdbError),
    Customer(Arc<Box<dyn std::error::Error>>),
}

impl YdbOrCustomerError {
    #[allow(dead_code)]
    pub fn from_mess<T: Into<String>>(s: T) -> Self {
        return Self::Customer(Arc::new(Box::new(YdbError::Custom(s.into()))));
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

impl std::error::Error for YdbOrCustomerError {}

impl From<YdbError> for YdbOrCustomerError {
    fn from(e: YdbError) -> Self {
        return Self::YDB(e);
    }
}

pub(crate) enum NeedRetry {
    True,           // operation guarantee to not completed, error is temporary, need retry
    IdempotentOnly, // operation in unknown state - it may be completed or not, error temporary. Operation may be auto retry for idempotent operations only.
    False, // operation is completed or error is stable (for example yql syntaxt errror) and no need retry
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum YdbError {
    Custom(String),
    Convert(String),
    InternalError(String),
    TransportDial(Arc<tonic::transport::Error>),
    Transport(String),
    TransportGRPCStatus(Arc<tonic::Status>),
    YdbStatusError(YdbStatusError),
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct YdbStatusError {
    #[allow(dead_code)]
    pub message: String,
    pub operation_status: i32,
    pub issues: Vec<YdbIssue>,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct YdbIssue {
    pub code: u32,
    pub message: String,
    pub issues: Vec<YdbIssue>,
}

impl YdbError {
    #[allow(dead_code)]
    pub fn from_str(s: &str) -> YdbError {
        return YdbError::Custom(s.to_string());
    }
    pub(crate) fn need_retry(&self) -> NeedRetry {
        match self {
            Self::Convert(_) => NeedRetry::False,
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
            Self::YdbStatusError(ydb_err) => {
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

impl Display for YdbError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self::Debug::fmt(self, f)
    }
}

impl std::error::Error for YdbError {}

impl From<http::Error> for YdbError {
    fn from(e: http::Error) -> Self {
        return YdbError::Custom(e.to_string());
    }
}

impl From<prost::DecodeError> for YdbError {
    fn from(e: prost::DecodeError) -> Self {
        return YdbError::Custom(e.to_string());
    }
}

impl From<std::env::VarError> for YdbError {
    fn from(e: std::env::VarError) -> Self {
        return YdbError::Custom(e.to_string());
    }
}

impl From<std::io::Error> for YdbError {
    fn from(e: std::io::Error) -> Self {
        return YdbError::Custom(e.to_string());
    }
}

impl From<&str> for YdbError {
    fn from(s: &str) -> Self {
        return Self::Custom(s.to_string());
    }
}

impl From<std::num::TryFromIntError> for YdbError {
    fn from(e: std::num::TryFromIntError) -> Self {
        return YdbError::Custom(e.to_string());
    }
}

impl From<std::string::FromUtf8Error> for YdbError {
    fn from(e: FromUtf8Error) -> Self {
        return YdbError::Custom(e.to_string());
    }
}

impl<T> From<std::sync::PoisonError<T>> for YdbError {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        return YdbError::Custom(e.to_string());
    }
}

impl From<std::time::SystemTimeError> for YdbError {
    fn from(e: SystemTimeError) -> Self {
        return YdbError::Custom(e.to_string());
    }
}

impl From<strum::ParseError> for YdbError {
    fn from(e: strum::ParseError) -> Self {
        return YdbError::Custom(e.to_string());
    }
}

impl From<tonic::codegen::http::uri::InvalidUri> for YdbError {
    fn from(e: tonic::codegen::http::uri::InvalidUri) -> Self {
        return YdbError::Custom(e.to_string());
    }
}

impl From<tokio::sync::AcquireError> for YdbError {
    fn from(e: AcquireError) -> Self {
        return YdbError::Custom(e.to_string());
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for YdbError {
    fn from(e: tokio::sync::oneshot::error::RecvError) -> Self {
        return YdbError::Custom(e.to_string());
    }
}

impl From<tonic::transport::Error> for YdbError {
    fn from(e: tonic::transport::Error) -> Self {
        return YdbError::Transport(e.to_string());
    }
}

impl From<tonic::Status> for YdbError {
    fn from(e: tonic::Status) -> Self {
        return YdbError::TransportGRPCStatus(Arc::new(e));
    }
}

impl From<url::ParseError> for YdbError {
    fn from(e: ParseError) -> Self {
        return Self::Custom(e.to_string());
    }
}
