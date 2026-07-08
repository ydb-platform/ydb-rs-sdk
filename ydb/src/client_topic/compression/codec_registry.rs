use crate::Codec;
use crate::client_topic::compression::builtin_codecs::*;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// Encodes topic message payloads for one codec.
pub trait CompressionEncoder: Debug + Send + Sync {
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + 'static>>;

    /// Codec this encoder produces.
    fn codec(&self) -> Codec;
}

/// Decodes topic message payloads for one codec.
pub trait CompressionDecoder: Debug + Send + Sync {
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + 'static>>;

    /// Codec this decoder accepts.
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

    /// Registers an encoder, replacing any encoder already registered for the same codec.
    pub(crate) fn register_encoder(&mut self, encoder: Arc<dyn CompressionEncoder>) {
        let _ = self.encoders.insert(encoder.codec(), encoder);
    }

    /// Registers a decoder, replacing any decoder already registered for the same codec.
    pub(crate) fn register_decoder(&mut self, decoder: Arc<dyn CompressionDecoder>) {
        let _ = self.decoders.insert(decoder.codec(), decoder);
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
