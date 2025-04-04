use crate::grpc_wrapper::raw_common_types::Timestamp;
use crate::grpc_wrapper::raw_topic_service::common::codecs::RawSupportedCodecs;
use std::collections::HashMap;
use ydb_grpc::ydb_proto::topic::{AlterConsumer, Consumer};

#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct RawConsumer {
    pub name: String,
    pub important: bool,
    pub read_from: Option<Timestamp>,
    pub supported_codecs: RawSupportedCodecs,
    pub attributes: HashMap<String, String>,
}

impl From<Consumer> for RawConsumer {
    fn from(value: Consumer) -> Self {
        Self {
            name: value.name,
            important: value.important,
            read_from: value.read_from.map(|x| x.into()),
            supported_codecs: value
                .supported_codecs
                .map_or_else(RawSupportedCodecs::default, |x| x.into()),
            attributes: value.attributes,
        }
    }
}

impl From<RawConsumer> for Consumer {
    fn from(value: RawConsumer) -> Self {
        Self {
            name: value.name,
            important: value.important,
            read_from: value.read_from.map(|x| x.into()),
            supported_codecs: Some(value.supported_codecs.into()),
            attributes: value.attributes,
            consumer_stats: None,
        }
    }
}

#[derive(serde::Serialize, Clone)]
pub(crate) struct RawAlterConsumer {
    pub name: String,
    pub set_important: Option<bool>,
    pub set_read_from: Option<Timestamp>,
    pub set_supported_codecs: Option<RawSupportedCodecs>,
    pub alter_attributes: HashMap<String, String>,
}

impl From<RawAlterConsumer> for AlterConsumer {
    fn from(value: RawAlterConsumer) -> Self {
        Self {
            name: value.name,
            set_important: value.set_important,
            set_read_from: value.set_read_from.map(|x| x.into()),
            set_supported_codecs: value.set_supported_codecs.map(|x| x.into()),
            alter_attributes: value.alter_attributes,
        }
    }
}
