use itertools::Itertools;
use ydb_grpc::ydb_proto::topic::{Codec, SupportedCodecs};

#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct RawCodec {
    pub code: i32,
}

impl RawCodec {
    fn is_raw(&self) -> bool {
        self.code == i32::from(Codec::Raw)
    }
}

#[derive(serde::Serialize, Clone, Default, Debug)]
pub(crate) struct RawSupportedCodecs {
    pub codecs: Vec<RawCodec>,
}

impl From<RawSupportedCodecs> for SupportedCodecs {
    fn from(value: RawSupportedCodecs) -> Self {
        Self {
            codecs: value.codecs.into_iter().map(|x| x.code).collect_vec(),
        }
    }
}

impl From<SupportedCodecs> for RawSupportedCodecs {
    fn from(value: SupportedCodecs) -> Self {
        Self {
            codecs: value
                .codecs
                .into_iter()
                .map(|x| RawCodec { code: x })
                .collect_vec(),
        }
    }
}
