use std::fmt::{Display, Formatter};

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

impl Display for RawError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for RawError {}
