use crate::grpc_wrapper::raw_errors::RawError;
use http::Uri;
use std::fmt::{Debug, Display, Formatter};
use std::ops::ControlFlow;
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
    Customer(Arc<dyn std::error::Error + Send + Sync>),
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
            Self::Customer(err) => YdbError::custom(format!("{err}")),
        }
    }

    pub(crate) fn is_retriable(&self, idempotency: Idempotency) -> bool {
        match self {
            YdbOrCustomerError::YDB(err) => err.is_retriable(idempotency),
            YdbOrCustomerError::Customer(_) => false,
        }
    }

    pub(crate) fn retry_flow<T>(
        self,
        idempotency: Idempotency,
    ) -> ControlFlow<Result<T, Self>, Self> {
        if self.is_retriable(idempotency) {
            ControlFlow::Continue(self)
        } else {
            ControlFlow::Break(Err(self))
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NeedRetry {
    /// The operation is guaranteed not to have completed and may be retried.
    True,
    /// The operation may have completed and may only be retried when it is idempotent.
    IdempotentOnly,
    /// The error is stable or is not documented as safe to retry.
    False,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Idempotency {
    Idempotent,
    NonIdempotent,
}

impl From<bool> for Idempotency {
    fn from(is_idempotent: bool) -> Self {
        if is_idempotent {
            Idempotency::Idempotent
        } else {
            Idempotency::NonIdempotent
        }
    }
}

impl Idempotency {
    pub const fn is_idempotent(self) -> bool {
        match self {
            Idempotency::Idempotent => true,
            Idempotency::NonIdempotent => false,
        }
    }
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

    /// Endpoint URI has no host.
    EndpointHasNoHost(Uri),

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

    /// Attempt failed due to exceeded deadline.
    ///
    /// Occurs when the retried operation times out
    /// on the first attempt.
    DeadlineExceeded,
}

impl YdbError {
    pub(crate) fn custom<T: Into<String>>(message: T) -> Self {
        Self::Custom(message.into())
    }
}

/// Describe operation status from server
///
/// Messages and codes doesn't have stable guarantee. But codes more stable.
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

    /// Context-specific retry classification overriding the default status-code policy.
    ///
    /// `None` uses the documented YDB status-code policy.
    pub(crate) need_retry: Option<NeedRetry>,

    /// Context-specific session-discard decision overriding the default status-code policy.
    ///
    /// `None` uses the documented YDB status-code policy.
    pub(crate) requires_session_discard: Option<bool>,
}

impl YdbStatusError {
    /// Creates a status error using the documented retry and session-discard policies.
    pub(crate) fn new(
        message: impl Into<String>,
        operation_status: i32,
        issues: Vec<YdbIssue>,
    ) -> Self {
        Self {
            message: message.into(),
            operation_status,
            issues,
            need_retry: None,
            requires_session_discard: None,
        }
    }

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
        StatusCode::try_from(self.operation_status)
            .map_err(|e| YdbError::InternalError(format!("unknown status code: {e}")))
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
/// Messages and codes doesn't have stable guarantee. But codes more stable.
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

    /// Whether an operation error makes a pooled session unsafe to reuse.
    ///
    /// This is deliberately separate from retry classification: retryability describes
    /// whether the operation may be repeated, while this describes ownership of the session
    /// on which the failed operation ran.
    pub(crate) fn requires_session_discard(&self) -> bool {
        match self {
            Self::Custom(_)
            | Self::Convert(_)
            | Self::NoRows
            | Self::EndpointHasNoHost(_)
            | Self::InternalError(_) => false,
            Self::TransportDial(_) | Self::Transport(_) | Self::DeadlineExceeded => true,
            Self::TransportGRPCStatus(status) => {
                use tonic::Code::*;

                // Keep this match in the order of the YDB "Recreate session" table:
                // https://ydb.tech/docs/en/reference/ydb-sdk/grpc-status-codes
                match status.code() {
                    Ok => false,
                    Cancelled => true,
                    Unknown => true,
                    InvalidArgument => true,
                    DeadlineExceeded => true,
                    NotFound => true,
                    AlreadyExists => true,
                    PermissionDenied => true,
                    ResourceExhausted => false,
                    FailedPrecondition => true,
                    Aborted => true,
                    OutOfRange => false,
                    Unimplemented => true,
                    Internal => true,
                    Unavailable => true,
                    DataLoss => true,
                    Unauthenticated => true,
                }
            }
            Self::YdbStatusError(status) => {
                if let Some(requires_discard) = status.requires_session_discard {
                    return requires_discard;
                }

                let Ok(status) = StatusCode::try_from(status.operation_status) else {
                    // An unknown server status is not evidence that the session is reusable.
                    return true;
                };
                use StatusCode::*;

                // Keep this match in the order of the YDB "Recreate session" table:
                // https://ydb.tech/docs/en/reference/ydb-sdk/ydb-status-codes
                match status {
                    Success => false,
                    BadRequest => false,
                    Unauthorized => false,
                    InternalError => false,
                    Aborted => false,
                    Unavailable => false,
                    Overloaded => false,
                    SchemeError => false,
                    GenericError => false,
                    Timeout => false,
                    BadSession => true,
                    PreconditionFailed => false,
                    AlreadyExists => false,
                    NotFound => false,
                    SessionExpired => true,
                    Cancelled => false,
                    Undetermined => false,
                    Unsupported => false,
                    SessionBusy => true,
                    ExternalError => false,
                    Unspecified => false,
                }
            }
        }
    }

    pub(crate) fn need_retry(&self) -> NeedRetry {
        use NeedRetry::*;

        match self {
            Self::Convert(_)
            | Self::Custom(_)
            | Self::InternalError(_)
            | Self::NoRows
            | Self::EndpointHasNoHost(_) => False,
            Self::TransportDial(_) => True,
            Self::Transport(_) | Self::DeadlineExceeded => IdempotentOnly,
            Self::TransportGRPCStatus(status) => {
                use tonic::Code::*;

                // Keep this match in the order of the YDB retry table:
                // https://ydb.tech/docs/en/reference/ydb-sdk/grpc-status-codes
                match status.code() {
                    Ok => False,
                    Cancelled => IdempotentOnly,
                    // tonic-generated clients create a new UNKNOWN status for `Service::ready()`
                    // failures and retain only the formatted message, so the original failure
                    // cannot be distinguished reliably from a server-returned UNKNOWN:
                    // https://github.com/grpc/grpc-rust/blob/v0.14.2/tonic-build/src/client.rs#L239-L242
                    // tonic deliberately retains UNKNOWN for transport failures:
                    // https://github.com/grpc/grpc-rust/issues/2488
                    Unknown => IdempotentOnly,
                    InvalidArgument => False,
                    DeadlineExceeded => IdempotentOnly,
                    NotFound => False,
                    AlreadyExists => False,
                    PermissionDenied => False,
                    ResourceExhausted => True,
                    FailedPrecondition => False,
                    Aborted => True,
                    OutOfRange => False,
                    Unimplemented => False,
                    Internal => IdempotentOnly,
                    Unavailable => IdempotentOnly,
                    DataLoss => False,
                    Unauthenticated => False,
                }
            }
            Self::YdbStatusError(status) => {
                if let Some(need_retry) = status.need_retry {
                    return need_retry;
                }

                let Ok(status) = StatusCode::try_from(status.operation_status) else {
                    // An unknown server status is not documented as safe to retry.
                    return False;
                };
                use StatusCode::*;

                // Keep this match in the order of the YDB retry table:
                // https://ydb.tech/docs/en/reference/ydb-sdk/ydb-status-codes
                match status {
                    Success => False,
                    BadRequest => False,
                    Unauthorized => False,
                    InternalError => False,
                    Aborted => True,
                    Unavailable => True,
                    Overloaded => True,
                    SchemeError => False,
                    GenericError => False,
                    Timeout => IdempotentOnly,
                    BadSession => True,
                    PreconditionFailed => False,
                    AlreadyExists => False,
                    NotFound => False,
                    SessionExpired => True,
                    Cancelled => False,
                    Undetermined => IdempotentOnly,
                    Unsupported => False,
                    SessionBusy => True,
                    ExternalError => False,
                    Unspecified => False,
                }
            }
        }
    }

    pub(crate) fn is_retriable(&self, idempotency: Idempotency) -> bool {
        match self.need_retry() {
            NeedRetry::True => true,
            NeedRetry::IdempotentOnly => idempotency.is_idempotent(),
            NeedRetry::False => false,
        }
    }

    pub(crate) fn retry_flow<T>(
        self,
        idempotency: Idempotency,
    ) -> ControlFlow<Result<T, Self>, Self> {
        if self.is_retriable(idempotency) {
            ControlFlow::Continue(self)
        } else {
            ControlFlow::Break(Err(self))
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

#[cfg(test)]
mod error_classification_tests {
    use super::*;
    use std::sync::Arc;
    use tonic::{Code, Status};
    use ydb_grpc::ydb_proto::status_ids::StatusCode;

    fn ydb_status(status: StatusCode) -> YdbError {
        YdbError::YdbStatusError(YdbStatusError::new("test", status as i32, vec![]))
    }

    #[test]
    fn session_discard_is_separate_from_retry_classification() {
        assert!(ydb_status(StatusCode::BadSession).requires_session_discard());
        assert!(ydb_status(StatusCode::SessionBusy).requires_session_discard());
        assert!(ydb_status(StatusCode::SessionExpired).requires_session_discard());
        assert!(!ydb_status(StatusCode::PreconditionFailed).requires_session_discard());
        assert!(!YdbError::Custom("customer".into()).requires_session_discard());
    }

    #[test]
    fn transport_failures_that_can_leave_work_in_flight_discard_session() {
        assert!(YdbError::Transport("connection lost".into()).requires_session_discard());
        assert!(
            YdbError::TransportGRPCStatus(Arc::new(Status::new(Code::Unavailable, "node down")))
                .requires_session_discard()
        );
        assert!(
            YdbError::TransportGRPCStatus(Arc::new(Status::new(
                Code::InvalidArgument,
                "bad request"
            )))
            .requires_session_discard()
        );
        assert!(
            !YdbError::TransportGRPCStatus(Arc::new(Status::new(
                Code::ResourceExhausted,
                "rate limited"
            )))
            .requires_session_discard()
        );
    }

    #[test]
    fn unknown_operation_status_discards_session_conservatively() {
        let err = YdbError::YdbStatusError(YdbStatusError::new("unknown", -1, vec![]));
        assert!(err.requires_session_discard());
    }
}

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
        YdbError::Custom(format!("{e:?}"))
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

impl From<tokio::time::error::Elapsed> for YdbError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::DeadlineExceeded
    }
}

impl From<RawError> for YdbError {
    fn from(e: RawError) -> Self {
        match e {
            RawError::Custom(message) => YdbError::Custom(format!("raw custom error: {message}")),
            RawError::ProtobufDecodeError(message) => {
                YdbError::Custom(format!("decode protobuf error: {message}"))
            }
            RawError::Transport(message) => YdbError::Transport(message),
            RawError::TonicStatus(s) => YdbError::TransportGRPCStatus(Arc::new(*s)),
            RawError::YdbStatus(status_error) => YdbError::YdbStatusError(status_error),
        }
    }
}
