pub(crate) type RawResult<T> = std::result::Result<T, RawError>;

pub(crate) enum RawError {
    Custom(String),
    ProtobufDecodeError(String),
    YdbStatus(crate::YdbStatusError),
    TonicStatus(tonic::Status),
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
