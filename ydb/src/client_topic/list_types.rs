use crate::errors;
use crate::grpc_wrapper::raw_topic_service::common::codecs::{RawCodec, RawSupportedCodecs};
use crate::grpc_wrapper::raw_topic_service::common::consumer::RawConsumer;
use crate::grpc_wrapper::raw_topic_service::common::metering_mode::RawMeteringMode;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Clone, Default, PartialEq, Eq)]
pub struct Codec {
    pub code: i32,
}

impl Codec {
    pub const RAW: Codec = Codec { code: 1 };
    pub const GZIP: Codec = Codec { code: 2 };
    pub const LZOP: Codec = Codec { code: 3 };
    pub const ZSTD: Codec = Codec { code: 4 };

    pub fn is_custom(&self) -> bool {
        self.code >= 10000 && self.code < 20000
    }
}

#[derive(Clone, Default)]
pub struct SupportedCodecs {
    pub codecs: Vec<Codec>,
}

impl TryFrom<Codec> for RawCodec {
    type Error = errors::YdbError;

    fn try_from(value: Codec) -> Result<Self, Self::Error> {
        match value {
            Codec::RAW => Ok(RawCodec::Raw),
            Codec::GZIP => Ok(RawCodec::Gzip),
            Codec::LZOP => Ok(RawCodec::Lzop),
            Codec::ZSTD => Ok(RawCodec::Zstd),
            codec_val if codec_val.is_custom() => Ok(RawCodec::Custom(codec_val.code)),
            codec_val => Err(errors::YdbError::Convert(format!(
                "Unexpected codec value {}",
                codec_val.code
            ))),
        }
    }
}

impl TryFrom<SupportedCodecs> for RawSupportedCodecs {
    type Error = errors::YdbError;

    fn try_from(value: SupportedCodecs) -> Result<RawSupportedCodecs, Self::Error> {
        let converted_codecs: Result<Vec<RawCodec>, errors::YdbError> = value // cannot inline cuz then expression type can't be inferred
            .codecs
            .into_iter()
            .map(|x| -> Result<RawCodec, errors::YdbError> { RawCodec::try_from(x) })
            .collect();

        Ok(Self {
            codecs: converted_codecs?,
        })
    }
}

#[derive(Clone)]
pub enum MeteringMode {
    Unspecified,
    ReservedCapacity,
    RequestUnits,
}

impl Default for MeteringMode {
    fn default() -> Self {
        MeteringMode::Unspecified
    }
}

impl From<MeteringMode> for RawMeteringMode {
    fn from(value: MeteringMode) -> Self {
        match value {
            MeteringMode::Unspecified => RawMeteringMode::Unspecified,
            MeteringMode::RequestUnits => RawMeteringMode::RequestUnits,
            MeteringMode::ReservedCapacity => RawMeteringMode::ReservedCapacity,
        }
    }
}

#[derive(Clone)]
pub struct Consumer {
    pub name: String,
    pub important: bool,
    pub read_from: Duration, // seconds since UNIX_EPOCH
    pub supported_codecs: SupportedCodecs,
    pub attributes: HashMap<String, String>,
}

impl TryFrom<Consumer> for RawConsumer {
    type Error = errors::YdbError;

    fn try_from(consumer: Consumer) -> Result<Self, Self::Error> {
        Ok(Self {
            name: consumer.name,
            important: consumer.important,
            read_from: consumer.read_from,
            supported_codecs: consumer.supported_codecs.try_into()?,
            attributes: consumer.attributes,
        })
    }
}
