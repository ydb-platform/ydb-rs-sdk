use std::fmt::{Debug, Display, Formatter};
use std::string::FromUtf8Error;
use tokio::sync::AcquireError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub enum Error {
    Custom(String),
    YdbOperation(YdbOperationError),
}

#[derive(Clone, Debug)]
pub struct YdbOperationError {
    pub(crate) message: String,
    pub(crate) operation_status: i32,
}

impl Error {
    #[allow(dead_code)]
    pub fn from_str(s: &str) -> Error {
        return Error::Custom(s.to_string());
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
        return Error::Custom(e.to_string());
    }
}

impl From<tonic::Status> for Error {
    fn from(e: tonic::Status) -> Self {
        return Error::Custom(e.to_string());
    }
}

impl From<ydb_protobuf::generated::ydb::operations::Operation> for Error {
    fn from(op: ydb_protobuf::generated::ydb::operations::Operation) -> Self {
        return Error::YdbOperation(YdbOperationError{message: format!("{:?}", &op), operation_status: op.status});
    }
}
