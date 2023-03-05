use itertools::Itertools;
use ydb_grpc::ydb_proto::topic::{Codec, SupportedCodecs};

#[derive(serde::Serialize, Clone)]
pub(crate) enum RawCodec {
    Unspecified,
    Raw,
    Gzip,
    Lzop,
    Zstd,
    Custom(i32),
}

#[derive(serde::Serialize, Clone, Default)]
pub(crate) struct RawSupportedCodecs {
    pub codecs: Vec<RawCodec>,
}

impl From<RawSupportedCodecs> for SupportedCodecs {
    fn from(value: RawSupportedCodecs) -> Self {
        Self {
            codecs: value
                .codecs
                .into_iter()
                .map(|x| match x {
                    RawCodec::Custom(val) => val,
                    RawCodec::Unspecified => Codec::Unspecified.into(),
                    RawCodec::Raw => Codec::Raw.into(),
                    RawCodec::Gzip => Codec::Gzip.into(),
                    RawCodec::Lzop => Codec::Lzop.into(),
                    RawCodec::Zstd => Codec::Zstd.into(),
                })
                .collect_vec(),
        }
    }
}
