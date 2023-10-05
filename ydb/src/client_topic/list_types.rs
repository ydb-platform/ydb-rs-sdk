use crate::grpc_wrapper::raw_topic_service::common::codecs::{RawCodec, RawSupportedCodecs};
use crate::grpc_wrapper::raw_topic_service::common::consumer::RawConsumer;
use crate::grpc_wrapper::raw_topic_service::common::metering_mode::RawMeteringMode;
use derive_builder::Builder;
use std::collections::HashMap;
use std::option::Option;
use std::time::SystemTime;

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
        Self { code: value.code }
    }
}

impl From<SupportedCodecs> for RawSupportedCodecs {
    fn from(value: SupportedCodecs) -> RawSupportedCodecs {
        Self {
            codecs: value
                .codecs
                .into_iter()
                .map(|x| RawCodec { code: x.code })
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

#[derive(Builder)]
#[builder(build_fn(error = "crate::errors::YdbError"))]
#[derive(Clone)]
pub struct Consumer {
    pub name: String,

    #[builder(default = "false")]
    pub important: bool,

    #[builder(default = "None")]
    pub read_from: Option<SystemTime>,

    #[builder(default = "SupportedCodecs::default()")]
    pub supported_codecs: SupportedCodecs,

    #[builder(default = "HashMap::new()")]
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
