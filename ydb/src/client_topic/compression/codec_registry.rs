use crate::client_topic::compression::builtin_codecs::{gzip_compress, gzip_decompress};
use crate::client_topic::list_types::Codec;
use crate::{YdbError, YdbResult};
use prost::bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

pub type EncoderFunc = Arc<dyn Fn(&Bytes) -> YdbResult<Bytes> + Send + Sync>;
pub type DecoderFunc = Arc<dyn Fn(&Bytes) -> YdbResult<Bytes> + Send + Sync>;

#[derive(Clone)]
pub struct CodecRegistry {
    funcs: HashMap<Codec, (EncoderFunc, DecoderFunc)>,
}

impl Default for CodecRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CodecRegistry {
    pub fn new() -> Self {
        let mut funcs: HashMap<Codec, (EncoderFunc, DecoderFunc)> = HashMap::new();
        funcs.insert(
            Codec::GZIP,
            (Arc::new(gzip_compress), Arc::new(gzip_decompress)),
        );
        Self { funcs }
    }

    pub fn register_codec(
        &mut self,
        codec: Codec,
        compress: EncoderFunc,
        decompress: DecoderFunc,
    ) -> YdbResult<()> {
        if !codec.is_custom() {
            return Err(YdbError::custom(format!(
                "non-custom codec {:?} cannot be registered",
                codec
            )));
        }

        if self.funcs.contains_key(&codec) {
            return Err(YdbError::custom(format!(
                "codec {:?} is already registered",
                codec
            )));
        }

        self.funcs.insert(codec, (compress, decompress));
        Ok(())
    }

    fn get_codec(&self, codec: &Codec) -> YdbResult<&(EncoderFunc, DecoderFunc)> {
        self.funcs
            .get(codec)
            .ok_or_else(|| YdbError::custom(format!("unsupported codec {:?}", codec)))
    }

    pub fn compress(&self, data: &Bytes, codec: &Codec) -> YdbResult<Bytes> {
        self.get_codec(codec).and_then(|(encode, _)| encode(data))
    }

    pub fn decompress(&self, data: &Bytes, codec: &Codec) -> YdbResult<Bytes> {
        self.get_codec(codec).and_then(|(_, decode)| decode(data))
    }
}
