use crate::client_topic::compression::builtin_codecs::*;
use crate::{Codec, YdbResult};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

pub trait CompressionEncoder: Debug + Send + Sync {
    fn encode(&self, data: &[u8]) -> YdbResult<Vec<u8>>;
    fn codec(&self) -> Codec;
}

pub trait CompressionDecoder: Debug + Send + Sync {
    fn decode(&self, data: &[u8]) -> YdbResult<Vec<u8>>;
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

        registry.register_encoder(Arc::new(RawEncoder));
        registry.register_decoder(Arc::new(RawDecoder));

        registry.register_encoder(Arc::new(GzipEncoder));
        registry.register_decoder(Arc::new(GzipDecoder));

        registry
    }

    pub(crate) fn register_encoder(&mut self, encoder: Arc<dyn CompressionEncoder>) {
        let _ = self.encoders.insert(encoder.codec(), encoder);
    }

    pub(crate) fn register_decoder(&mut self, decoder: Arc<dyn CompressionDecoder>) {
        let _ = self.decoders.insert(decoder.codec(), decoder);
    }

    pub(crate) fn supported_encoders(&self) -> HashSet<Codec> {
        Self::supported(&self.encoders)
    }

    fn supported<T>(container: &HashMap<Codec, T>) -> HashSet<Codec> {
        container.keys().copied().collect()
    }

    pub(crate) fn get_encoder(&self, codec: Codec) -> Option<Arc<dyn CompressionEncoder>> {
        self.encoders.get(&codec).cloned()
    }

    pub(crate) fn get_decoder(&self, codec: Codec) -> Option<Arc<dyn CompressionDecoder>> {
        self.decoders.get(&codec).cloned()
    }
}
