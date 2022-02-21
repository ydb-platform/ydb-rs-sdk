use crate::errors::NeedRetry::IdempotentOnly;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use ydb_protobuf::ydb_proto::status_ids::StatusCode;

pub type YdbResult<T> = std::result::Result<T, YdbError>;
pub type YdbResultWithCustomerErr<T> = std::result::Result<T, YdbOrCustomerError>;

#[derive(Clone)]
pub enum YdbOrCustomerError {
    YDB(YdbError),
    NoneInOption,
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
            Self::NoneInOption => f.write_str("ydb: option field is none"),
            Self::Customer(err) => Debug::fmt(err, f),
        };
    }
}

impl Display for YdbOrCustomerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        return match self {
            Self::YDB(err) => Display::fmt(err, f),
            Self::NoneInOption => f.write_str("ydb: option field is none"),
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

/// Error which can be returned from the crate.
///
/// Now most of errors are simple Custom error with custom text.
/// Please not parse the text - it can be change at any time without compile check.
/// Write about error type you need or PR it.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum YdbError {
    /// Common error
    ///
    /// Not parse text of error for detect error type.
    /// It will change.
    Custom(String),

    /// Errors of convert between native rust types and ydb value
    Convert(String),

    /// Unexpected error. Write issue if it will happen.
    InternalError(String),

    /// Error while dial to ydb server
    TransportDial(Arc<tonic::transport::Error>),

    /// Error on transport level of request/response
    Transport(String),

    /// Error from GRPC status code
    TransportGRPCStatus(Arc<tonic::Status>),

    /// Error from operation status
    YdbStatusError(YdbStatusError),
}

/// Describe operation status from server
///
/// Messages and codes doesn't have stable gurantee. But codes more stable.
/// If you want detect some errors prefer code over text parse. Messages for human usage only.
#[derive(Clone, Debug)]
#[non_exhaustive]
// Combine with YdbIssue?
pub struct YdbStatusError {
    /// Human readable message described status
    #[allow(dead_code)]
    pub message: String,

    /// Operation status code
    ///
    /// Struct field presended as i32 - for repr any of received value
    /// For get typed status use fn YdbStatusError::operation_status()
    ///
    /// ```
    /// # use ydb::YdbStatusError;
    /// # use ydb_protobuf::ydb_proto::status_ids::StatusCode;
    /// # let status = YdbStatusError{message: "test".to_string(), operation_status: StatusCode::AlreadyExists as i32, issues: Vec::new()};
    /// #
    /// assert_eq!(status.operation_status, 400130);
    /// assert_eq!(status.operation_status(), StatusCode::AlreadyExists)
    /// ```
    pub operation_status: i32,

    /// Ydb issue from server for the message
    ///
    /// It describe internal errors, warnings, etc more detail then operation_status or message.
    pub issues: Vec<YdbIssue>,
}

impl YdbStatusError {
    /// Got typed operation status or error
    ///
    /// ```
    /// # use ydb::YdbStatusError;
    /// # use ydb_protobuf::ydb_proto::status_ids::StatusCode;
    /// # let status = YdbStatusError{message: "test".to_string(), operation_status: StatusCode::AlreadyExists as i32, issues: Vec::new()};
    /// #
    /// assert_eq!(status.operation_status, 400130);
    /// assert_eq!(status.operation_status(), StatusCode::AlreadyExists)
    /// ```
    pub fn operation_status(&self) -> YdbResult<StatusCode> {
        return StatusCode::from_i32(self.operation_status).ok_or(YdbError::InternalError(
            format!("unknown status code: {}", self.operation_status),
        ));
    }
}

#[derive(Copy, Clone, Debug)]
#[non_exhaustive]
#[repr(u32)]
pub enum YdbIssueSeverity {
    Fatal = 0,
    Error = 1,
    Warning = 2,
    Info = 3,
}

/// Describe issue from server
///
/// Messages and codes doesn't have stable gurantee. But codes more stable.
/// If you want detect some errors prefer code over text parse. Messages for human usage only.
#[derive(Clone, Debug)]
#[non_exhaustive]
// Combine with YdbStatusError?
pub struct YdbIssue {
    pub issue_code: u32,
    pub message: String,
    pub issues: Vec<YdbIssue>,

    /// Severity of the issue
    ///
    /// The field conains raw u32 severity value.
    /// For get types severity use severity fn
    ///
    /// ```
    /// # use ydb::YdbIssue;
    /// let issue = YdbIssue{issue_code: 1, message: "".to_string(), issues: Vec::new(), severity: 2};
    /// assert_eq!(issue.severity, 2);
    /// assert_eq!(issue.severity(), YdbIssueSeverity::Warning);
    /// ```
    pub severity: u32,
}

impl YdbIssue {
    pub fn severity(&self) -> YdbResult<YdbIssueSeverity> {
        let val = match self.severity {
            0 => YdbIssueSeverity::Fatal,
            1 => YdbIssueSeverity::Error,
            2 => YdbIssueSeverity::Warning,
            3 => YdbIssueSeverity::Info,
            _ => {
                return Err(YdbError::InternalError(format!(
                    "unexpected issue severity: {}",
                    self.severity
                )))
            }
        };
        return Ok(val);
    }
}

impl YdbError {
    #[allow(dead_code)]
    pub(crate) fn from_str<T: Into<String>>(s: T) -> YdbError {
        return YdbError::Custom(s.into());
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
    YdbOrCustomerError,
    std::convert::Infallible,
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
