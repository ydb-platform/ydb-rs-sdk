use crate::errors::NeedRetry::IdempotentOnly;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

pub type YdbResult<T> = std::result::Result<T, YdbError>;
pub type YdbResultWithCustomerErr<T> = std::result::Result<T, YdbOrCustomerError>;

#[derive(Clone)]
pub enum YdbOrCustomerError {
    YDB(YdbError),
    Customer(Arc<Box<dyn std::error::Error>>),
}

impl YdbOrCustomerError {
    #[allow(dead_code)]
    pub(crate) fn from_mess<T: Into<String>>(s: T) -> Self {
        return Self::Customer(Arc::new(Box::new(YdbError::Custom(s.into()))));
    }

    #[allow(dead_code)]
    pub(crate) fn from_err<T: std::error::Error + 'static>(err: T) -> Self {
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
    pub(crate) message: String,
    pub(crate) operation_status: i32,
    pub(crate) issues: Vec<YdbIssue>,
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
    pub(crate) fn from_str(s: &str) -> YdbError {
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
                use ydb_protobuf::ydb_proto::status_ids::StatusCode;
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

macro_rules! to_custom_ydb_err {
    ($($t:ty),+) => {
        $(
        impl From<$t> for YdbError {
            fn from(e: $t) -> Self {
                return YdbError::Custom(e.to_string());
            }
        }
        )+
    };
}

impl std::error::Error for YdbError {}

to_custom_ydb_err!(
    http::Error,
    prost::DecodeError,
    reqwest::Error,
    std::env::VarError,
    std::io::Error,
    std::num::TryFromIntError,
    std::string::FromUtf8Error,
    std::time::SystemTimeError,
    &str,
    strum::ParseError,
    tonic::transport::Error,
    tokio::sync::AcquireError,
    tokio::sync::oneshot::error::RecvError,
    tokio::sync::watch::error::RecvError,
    tokio::task::JoinError,
    tonic::codegen::http::uri::InvalidUri,
    url::ParseError
);

impl From<Box<dyn std::any::Any + Send>> for YdbError {
    fn from(e: Box<dyn std::any::Any + Send>) -> Self {
        return YdbError::Custom(format!("{:?}", e));
    }
}

impl<T> From<std::sync::PoisonError<T>> for YdbError {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        return YdbError::Custom(e.to_string());
    }
}

impl From<tonic::Status> for YdbError {
    fn from(e: tonic::Status) -> Self {
        return YdbError::TransportGRPCStatus(Arc::new(e));
    }
}
