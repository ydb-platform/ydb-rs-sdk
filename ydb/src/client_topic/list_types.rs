use crate::grpc_wrapper::raw_topic_service::common::codecs::{RawCodec, RawSupportedCodecs};
use crate::grpc_wrapper::raw_topic_service::common::consumer::RawConsumer;
use crate::grpc_wrapper::raw_topic_service::common::metering_mode::RawMeteringMode;
use std::collections::HashMap;
use std::time::{SystemTime};
use std::option::Option;

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

impl From<Codec> for RawCodec {
    fn from(value: Codec) -> Self {
       Self{
           code: value.code
       }
    }
}

impl From<SupportedCodecs> for RawSupportedCodecs {
    fn from(value: SupportedCodecs) -> RawSupportedCodecs {
        Self {
            codecs: value
                .codecs
                .into_iter()
                .map(|x| RawCodec{code: x.code})
                .collect(),
        }
    }
}

#[derive(Clone)]
pub enum MeteringMode {
    ReservedCapacity,
    RequestUnits,
}

impl From<Option<MeteringMode>> for RawMeteringMode {
    fn from(value: Option<MeteringMode>) -> Self {
        match value {
            None => RawMeteringMode::Unspecified,
            Some(MeteringMode::RequestUnits) => RawMeteringMode::RequestUnits,
            Some(MeteringMode::ReservedCapacity) => RawMeteringMode::ReservedCapacity,
        }
    }
}

#[derive(Clone)]
pub struct Consumer {
    pub name: String,
    pub important: bool,
    pub read_from: SystemTime,
    pub supported_codecs: SupportedCodecs,
    pub attributes: HashMap<String, String>,
}

impl From<Consumer> for RawConsumer {

    fn from(consumer: Consumer) -> Self {
        Self {
            name: consumer.name,
            important: consumer.important,
            read_from: consumer.read_from,
            supported_codecs: consumer.supported_codecs.into(),
            attributes: consumer.attributes,
        }
    }
}
