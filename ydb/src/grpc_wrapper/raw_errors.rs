use std::fmt::{Display, Formatter};
use std::num::TryFromIntError;
use std::time::SystemTimeError;

pub(crate) type RawResult<T> = std::result::Result<T, RawError>;

#[derive(Debug)]
pub(crate) enum RawError {
    Custom(String),
    ProtobufDecodeError(String),
    YdbStatus(crate::YdbStatusError),
    TonicStatus(tonic::Status),
}

impl RawError {
    pub fn custom<S: Into<String>>(text: S) -> Self {
        RawError::Custom(text.into())
    }
    pub fn decode_error<S: Into<String>>(text: S) -> Self {
        RawError::ProtobufDecodeError(text.into())
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for RawError {
    fn from(value: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::Custom(format!(
            "Internal error while sending message via mpsc channel: {}",
            value
        ))
    }
}

impl From<tonic::Status> for RawError {
    fn from(s: tonic::Status) -> Self {
        Self::TonicStatus(s)
    }
}

impl From<prost::DecodeError> for RawError {
    fn from(e: prost::DecodeError) -> Self {
        Self::ProtobufDecodeError(e.to_string())
    }
}

impl From<std::num::TryFromIntError> for RawError {
    fn from(_: TryFromIntError) -> Self {
        Self::custom("bad convert from int")
    }
}

impl Display for RawError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for RawError {}

impl From<SystemTimeError> for RawError {
    fn from(_value: SystemTimeError) -> Self {
        Self::Custom("System time conversion error".to_string())
    }
}
