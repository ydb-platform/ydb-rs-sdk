use crate::errors::NeedRetry::IdempotentOnly;

use crate::grpc_wrapper::raw_errors::RawError;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use ydb_grpc::ydb_proto::status_ids::StatusCode;

/// T result or YdbError as Error
pub type YdbResult<T> = std::result::Result<T, YdbError>;

/// T result or YdbOrCustomerError as Error
pub type YdbResultWithCustomerErr<T> = std::result::Result<T, YdbOrCustomerError>;

/// Error for wrap user errors while return it from callback
#[derive(Clone)]
pub enum YdbOrCustomerError {
    /// Usual YDB errors
    YDB(YdbError),

    /// Wrap for customer error
    Customer(Arc<Box<dyn std::error::Error + Send + Sync>>),
}

impl YdbOrCustomerError {
    #[allow(dead_code)]
    pub(crate) fn from_mess<T: Into<String>>(s: T) -> Self {
        Self::Customer(Arc::new(Box::new(YdbError::Custom(s.into()))))
    }

    /// Create YdbOrCustomerError from customer error
    pub fn from_err<T: std::error::Error + 'static + Send + Sync>(err: T) -> Self {
        Self::Customer(Arc::new(Box::new(err)))
    }

    pub fn to_ydb_error(self) -> YdbError {
        match self {
            Self::YDB(err) => err,
            Self::Customer(err) => YdbError::custom(format!("{}", err)),
        }
    }
}

impl Debug for YdbOrCustomerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::YDB(err) => Debug::fmt(err, f),
            Self::Customer(err) => Debug::fmt(err, f),
        }
    }
}

impl Display for YdbOrCustomerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::YDB(err) => Display::fmt(err, f),
            Self::Customer(err) => Display::fmt(err, f),
        }
    }
}

impl std::error::Error for YdbOrCustomerError {}

impl From<YdbError> for YdbOrCustomerError {
    fn from(e: YdbError) -> Self {
        Self::YDB(e)
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
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub enum YdbError {
    /// Common error
    ///
    /// Not parse text of error for detect error type.
    /// It will change.
    Custom(String),

    /// Errors of convert between native rust types and ydb value
    Convert(String),

    /// No rows in result set
    NoRows,

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

impl YdbError {
    pub(crate) fn custom<T: Into<String>>(message: T) -> Self {
        Self::Custom(message.into())
    }
}

/// Describe operation status from server
///
/// Messages and codes doesn't have stable gurantee. But codes more stable.
/// If you want detect some errors prefer code over text parse. Messages for human usage only.
#[derive(Clone, Debug, Default)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
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
    /// # use ydb::{YdbResult, YdbStatusError};
    /// # use ydb_grpc::ydb_proto::status_ids::StatusCode;
    /// # fn main()->YdbResult<()>{
    /// let mut status =YdbStatusError::default();
    /// status.operation_status = StatusCode::AlreadyExists as i32;
    /// assert_eq!(status.operation_status, 400130);
    /// assert_eq!(status.operation_status()?, StatusCode::AlreadyExists);
    /// # return Ok(());
    /// # }
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
    /// # use ydb::{YdbResult, YdbStatusError};
    /// # use ydb_grpc::ydb_proto::status_ids::StatusCode;
    /// # fn main()->YdbResult<()>{
    /// let mut status = YdbStatusError::default();
    /// status.operation_status= StatusCode::AlreadyExists as i32;
    /// assert_eq!(status.operation_status, 400130);
    /// assert_eq!(status.operation_status()?, StatusCode::AlreadyExists);
    /// # return Ok(());
    /// # }
    /// ```
    pub fn operation_status(&self) -> YdbResult<StatusCode> {
        StatusCode::from_i32(self.operation_status).ok_or_else(|| {
            YdbError::InternalError(format!("unknown status code: {}", self.operation_status))
        })
    }
}

/// Severity of issue
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub enum YdbIssueSeverity {
    #[default]
    Fatal,
    Error,
    Warning,
    Info,

    // no use Unknown for own logic (use for debug/log only) - for prevent broke your code when new level will be defined.
    // use convert to u32 for temporary use int code and ask a maintainer to add new level as explicit value
    Unknown(u32),
}

impl From<YdbIssueSeverity> for u32 {
    fn from(value: YdbIssueSeverity) -> Self {
        match value {
            YdbIssueSeverity::Fatal => 0,
            YdbIssueSeverity::Error => 1,
            YdbIssueSeverity::Warning => 2,
            YdbIssueSeverity::Info => 3,
            YdbIssueSeverity::Unknown(code) => code,
        }
    }
}

impl From<u32> for YdbIssueSeverity {
    fn from(value: u32) -> Self {
        match value {
            0 => YdbIssueSeverity::Fatal,
            1 => YdbIssueSeverity::Error,
            2 => YdbIssueSeverity::Warning,
            3 => YdbIssueSeverity::Info,
            value => YdbIssueSeverity::Unknown(value),
        }
    }
}

/// Describe issue from server
///
/// Messages and codes doesn't have stable gurantee. But codes more stable.
/// If you want detect some errors prefer code over text parse. Messages for human usage only.
#[derive(Clone, Debug, Default)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
// Combine with YdbStatusError?
pub struct YdbIssue {
    pub issue_code: u32,
    pub message: String,

    /// Recursive issues, explained current problems
    pub issues: Vec<YdbIssue>,

    /// Severity of the issue.
    /// For get numeric code - use convert to u32.
    /// ```
    /// # use ydb::{YdbIssue, YdbIssueSeverity, YdbResult};
    /// # fn main()->YdbResult<()>{
    /// let mut issue = YdbIssue::default();
    /// issue.severity = YdbIssueSeverity::Warning;
    /// assert_eq!(u32::from(issue.severity), 2);
    /// # return Ok(());
    /// # }
    /// ```
    pub severity: YdbIssueSeverity,
}

impl YdbError {
    pub(crate) fn from_str<T: Into<String>>(s: T) -> YdbError {
        YdbError::Custom(s.into())
    }

    pub(crate) fn need_retry(&self) -> NeedRetry {
        match self {
            Self::Convert(_) => NeedRetry::False,
            Self::Custom(_) => NeedRetry::False,
            Self::InternalError(_) => NeedRetry::False,
            Self::NoRows => NeedRetry::False,
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
    http::uri::InvalidUriParts,
    reqwest::Error,
    serde_json::Error,
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
        YdbError::Custom(format!("{:?}", e))
    }
}

impl<T> From<std::sync::PoisonError<T>> for YdbError {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        YdbError::Custom(e.to_string())
    }
}

impl From<tonic::Status> for YdbError {
    fn from(e: tonic::Status) -> Self {
        YdbError::TransportGRPCStatus(Arc::new(e))
    }
}

impl From<RawError> for YdbError {
    fn from(e: RawError) -> Self {
        match e {
            RawError::Custom(message) => YdbError::Custom(format!("raw custom error: {}", message)),
            RawError::ProtobufDecodeError(message) => {
                YdbError::Custom(format!("decode protobuf error: {}", message))
            }
            RawError::TonicStatus(s) => YdbError::TransportGRPCStatus(Arc::new(s)),
            RawError::YdbStatus(status_error) => YdbError::YdbStatusError(status_error),
        }
    }
}
