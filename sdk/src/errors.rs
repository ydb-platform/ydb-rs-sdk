use std::fmt::{Debug, Display, Formatter};
use std::string::FromUtf8Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub enum Error {
    Custom(String),
    YdbStatus(ydb_protobuf::generated::ydb::status_ids::StatusCode),
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

impl From<tonic::codegen::http::uri::InvalidUri> for Error {
    fn from(e: tonic::codegen::http::uri::InvalidUri) -> Self {
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

impl From<ydb_protobuf::generated::ydb::status_ids::StatusCode> for Error {
    fn from(e: ydb_protobuf::generated::ydb::status_ids::StatusCode) -> Self {
        return Error::YdbStatus(e);
    }
}
