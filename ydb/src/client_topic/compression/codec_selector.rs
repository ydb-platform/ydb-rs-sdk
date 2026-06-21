use crate::client_topic::compression::codec_registry::{CodecRegistry, CompressionEncoder};
use crate::client_topic::list_types::Codec;
use crate::{YdbError, YdbResult};
use std::sync::Arc;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

const DEFAULT_MEASURE_INTERVAL: usize = 100;

/// User-facing codec choice for a topic writer.
/// `Auto` samples each batch and picks the smallest among server-allowed codecs.
/// `Fixed(c)` pins every WriteRequest to `c`.
#[derive(Clone, Debug, Default)]
pub enum CodecSelection {
    #[default]
    Auto,
    Fixed(Codec),
}

pub(crate) enum CodecSelector {
    Fixed(Codec),
    Auto(AutoSelectorState),
}

pub(crate) struct AutoSelectorState {
    accepted_encoders: Vec<Arc<dyn CompressionEncoder>>,
    batch_counter: usize,
    current_codec: Codec,
    measure_interval: usize,
}

impl CodecSelector {
    /// Builds a writer-side codec selector.
    ///
    /// Fixed selection pins one codec. Auto selection stores topic-accepted
    /// encoders and periodically measures them against message samples.
    ///
    /// Empty `server_codecs` means the topic accepts SDK defaults from
    /// `CodecRegistry::default()`.
    ///
    /// # Errors
    ///
    /// Returns an error if the normalized topic codec list does not include
    /// `Codec::RAW`.
    ///
    /// For fixed selection, returns an error if the requested codec is not
    /// accepted by the topic or has no registered encoder.
    ///
    /// For auto selection, returns an error if no topic-accepted codec has a
    /// registered encoder.
    pub(crate) fn new(
        selection: CodecSelection,
        server_codecs: Vec<Codec>,
        codec_registry: Arc<CodecRegistry>,
    ) -> YdbResult<Self> {
        let server_codecs = ensure_default_server_codecs(server_codecs);
        ensure_raw_supported(&server_codecs)?;

        match selection {
            CodecSelection::Fixed(codec) => {
                build_fixed_selector(codec, &server_codecs, &codec_registry)
            }

            CodecSelection::Auto => build_auto_selector(&server_codecs, &codec_registry),
        }
    }

    pub(crate) fn codec(&self) -> Codec {
        match self {
            Self::Fixed(c) => *c,
            Self::Auto(auto) => auto.current_codec,
        }
    }

    pub(crate) fn step(&mut self, sample: &[MessageData]) {
        if let Self::Auto(auto) = self {
            if auto.batch_counter % auto.measure_interval == 0 {
                auto.current_codec = measure_codecs(sample, &auto.accepted_encoders);
            }
            auto.batch_counter += 1;
        }
    }
}

fn build_fixed_selector(
    codec: Codec,
    server_codecs: &[Codec],
    registry: &CodecRegistry,
) -> YdbResult<CodecSelector> {
    if !server_codecs.contains(&codec) {
        return Err(YdbError::custom(format!(
            "codec {:?} is not supported by the topic (supported_codecs: {:?})",
            codec, server_codecs
        )));
    }

    if registry.get_encoder(codec).is_none() {
        return Err(YdbError::custom(format!(
            "codec {:?} is not registered in the codec registry",
            codec
        )));
    }

    Ok(CodecSelector::Fixed(codec))
}

fn build_auto_selector(
    server_codecs: &[Codec],
    registry: &CodecRegistry,
) -> YdbResult<CodecSelector> {
    let accepted_encoders = resolve_accepted_encoders(registry, server_codecs);

    let Some(first_encoder) = accepted_encoders.first() else {
        return Err(YdbError::custom(
            "no common codecs between server and client",
        ));
    };
    let first_codec = first_encoder.codec();

    debug_assert!(accepted_encoders
        .iter()
        .any(|encoder| encoder.codec() == Codec::RAW));

    if accepted_encoders.len() == 1 {
        return Ok(CodecSelector::Fixed(first_codec));
    }

    Ok(CodecSelector::Auto(AutoSelectorState {
        current_codec: first_codec,
        accepted_encoders,
        batch_counter: 0,
        measure_interval: DEFAULT_MEASURE_INTERVAL,
    }))
}

fn resolve_accepted_encoders(
    registry: &CodecRegistry,
    server_codecs: &[Codec],
) -> Vec<Arc<dyn CompressionEncoder>> {
    server_codecs
        .iter()
        .filter_map(|&codec| registry.get_encoder(codec))
        .collect()
}

/// Picks the smallest codec for this sample.
///
/// Assumes that `encoders` is non-empty and contains a RAW encoder, so failed
/// probes of other encoders do not make measurement fallible.
fn measure_codecs(sample: &[MessageData], encoders: &[Arc<dyn CompressionEncoder>]) -> Codec {
    debug_assert!(!encoders.is_empty());
    debug_assert!(encoders.iter().any(|encoder| encoder.codec() == Codec::RAW));

    if sample.is_empty() {
        return Codec::RAW;
    }

    let mut best_codec = Codec::RAW;
    let mut best_size = sample.iter().map(|m| m.data.len()).sum();

    'encoders: for encoder in encoders {
        let mut size = 0;
        for msg in sample {
            match encoder.encode(&msg.data) {
                Ok(compressed) => size += compressed.len(),
                Err(_) => continue 'encoders,
            }
        }

        if size < best_size {
            best_size = size;
            best_codec = encoder.codec();
        }
    }

    best_codec
}

fn ensure_default_server_codecs(server_codecs: Vec<Codec>) -> Vec<Codec> {
    if server_codecs.is_empty() {
        CodecRegistry::default()
            .supported_encoders()
            .into_iter()
            .collect()
    } else {
        server_codecs
    }
}

fn ensure_raw_supported(server_codecs: &[Codec]) -> YdbResult<()> {
    if server_codecs.contains(&Codec::RAW) {
        return Ok(());
    }

    Err(YdbError::custom(format!(
        "codec {:?} is not supported by the topic (supported_codecs: {:?})",
        Codec::RAW,
        server_codecs
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(data: Vec<u8>) -> MessageData {
        MessageData {
            seq_no: 0,
            created_at: None,
            data,
            uncompressed_size: 0,
            metadata_items: vec![],
            partitioning: None,
        }
    }

    const FAILING_CODEC: Codec = Codec { code: 9001 };

    #[derive(Debug)]
    struct FailingEncoder;

    impl CompressionEncoder for FailingEncoder {
        fn encode(&self, _data: &[u8]) -> YdbResult<Vec<u8>> {
            Err(YdbError::custom("test: encoder always fails"))
        }

        fn codec(&self) -> Codec {
            FAILING_CODEC
        }
    }

    #[test]
    fn measure_codecs_falls_back_to_raw_when_other_encoder_fails() {
        let mut registry = CodecRegistry::new();
        registry.register_encoder(Arc::new(FailingEncoder));

        let sample = vec![msg(b"payload".to_vec())];
        let encoders = vec![
            registry.get_encoder(FAILING_CODEC).unwrap(),
            registry.get_encoder(Codec::RAW).unwrap(),
        ];

        assert_eq!(measure_codecs(&sample, &encoders), Codec::RAW);
    }

    #[test]
    fn measure_codecs_selects_best_non_failing() {
        let mut registry = CodecRegistry::new();
        registry.register_encoder(Arc::new(FailingEncoder));

        let sample = vec![msg(vec![0u8; 1024])];
        let encoders = vec![
            registry.get_encoder(Codec::RAW).unwrap(),
            registry.get_encoder(Codec::GZIP).unwrap(),
            registry.get_encoder(FAILING_CODEC).unwrap(),
        ];

        let picked = measure_codecs(&sample, &encoders);

        assert_eq!(picked, Codec::GZIP);
    }

    #[test]
    fn selector_auto_fails_on_missing_raw() {
        let registry = CodecRegistry::new();
        let selector = CodecSelector::new(CodecSelection::Auto, vec![Codec::GZIP], registry.into());

        assert!(selector.is_err());
    }

    #[test]
    fn selector_fixed_fails_on_missing() {
        let registry = CodecRegistry::new();
        let selector = CodecSelector::new(
            CodecSelection::Fixed(Codec::RAW),
            vec![Codec::GZIP, FAILING_CODEC],
            registry.into(),
        );

        assert!(selector.is_err());
    }
}
