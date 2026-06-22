use crate::client_topic::compression::builtin_codecs::*;
use crate::{Codec, YdbError, YdbResult};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

pub trait CompressionEncoder: Debug + Send + Sync {
    fn encode(&self, data: &[u8]) -> YdbResult<Vec<u8>>;

    /// Returns the codec this encoder handles; custom codec IDs must satisfy [`Codec::is_custom`].
    fn codec(&self) -> Codec;
}

pub trait CompressionDecoder: Debug + Send + Sync {
    fn decode(&self, data: &[u8]) -> YdbResult<Vec<u8>>;

    /// Returns the codec this decoder handles; custom codec IDs must satisfy [`Codec::is_custom`].
    fn codec(&self) -> Codec;
}

#[derive(Clone)]
pub(crate) struct CodecRegistry {
    encoders: HashMap<Codec, Arc<dyn CompressionEncoder>>,
    decoders: HashMap<Codec, Arc<dyn CompressionDecoder>>,
}

impl Default for CodecRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CodecRegistry {
    pub(crate) fn new() -> Self {
        let mut registry = Self {
            encoders: HashMap::new(),
            decoders: HashMap::new(),
        };

        registry.register_builtin_encoder(Arc::new(RawEncoder));
        registry.register_builtin_decoder(Arc::new(RawDecoder));
        registry.register_builtin_encoder(Arc::new(GzipEncoder));
        registry.register_builtin_decoder(Arc::new(GzipDecoder));

        registry
    }

    fn register_builtin_encoder(&mut self, encoder: Arc<dyn CompressionEncoder>) {
        let _ = self.encoders.insert(encoder.codec(), encoder);
    }

    fn register_builtin_decoder(&mut self, decoder: Arc<dyn CompressionDecoder>) {
        let _ = self.decoders.insert(decoder.codec(), decoder);
    }

    pub(crate) fn register_encoder(
        &mut self,
        encoder: Arc<dyn CompressionEncoder>,
    ) -> YdbResult<()> {
        validate_codec_id(encoder.codec())?;
        let _ = self.encoders.insert(encoder.codec(), encoder);
        Ok(())
    }

    pub(crate) fn register_decoder(
        &mut self,
        decoder: Arc<dyn CompressionDecoder>,
    ) -> YdbResult<()> {
        validate_codec_id(decoder.codec())?;
        let _ = self.decoders.insert(decoder.codec(), decoder);
        Ok(())
    }

    pub(crate) fn sdk_builtin_codecs() -> Vec<Codec> {
        Self::new().encoders.keys().copied().collect()
    }

    pub(crate) fn get_encoder(&self, codec: Codec) -> Option<Arc<dyn CompressionEncoder>> {
        self.encoders.get(&codec).cloned()
    }

    pub(crate) fn get_decoder(&self, codec: Codec) -> Option<Arc<dyn CompressionDecoder>> {
        self.decoders.get(&codec).cloned()
    }
}

fn is_sdk_builtin(codec: Codec) -> bool {
    CodecRegistry::new().get_encoder(codec).is_some()
}

pub(crate) fn validate_codec_id(codec: Codec) -> YdbResult<()> {
    if is_sdk_builtin(codec) || codec.is_custom() {
        Ok(())
    } else {
        Err(YdbError::custom(format!(
            "invalid codec ID {}: must be a built-in codec or satisfy Codec::is_custom()",
            codec.code
        )))
    }
}
