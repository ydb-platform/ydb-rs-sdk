use crate::client_topic::compression::codec_registry::{
    validate_codec_id, CodecRegistry, CompressionEncoder,
};
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

pub(super) enum CodecSelector {
    Fixed(Codec),
    Auto(AutoSelectorState),
}

pub(super) struct AutoSelectorState {
    accepted_encoders: Vec<Arc<dyn CompressionEncoder>>,
    batch_counter: usize,
    current_codec: Codec,
    measure_interval: usize,
}

impl CodecSelector {
    /// Builds a writer-side codec selector.
    ///
    /// # Fixed
    /// Validates the codec ID, requires a registered encoder, and — when the
    /// server reports a non-empty codec list — requires the codec to appear in
    /// that list. Empty server list means no topic restriction.
    ///
    /// # Auto
    /// Candidates are `server_codecs ∩ registered_encoders` when the server
    /// list is non-empty, or SDK built-ins (`registered_encoders ∩ [RAW, GZIP]`)
    /// when the server list is empty. Returns an error if there are no candidates.
    pub(super) fn new(
        selection: CodecSelection,
        server_codecs: Vec<Codec>,
        codec_registry: Arc<CodecRegistry>,
    ) -> YdbResult<Self> {
        match selection {
            CodecSelection::Fixed(codec) => {
                build_fixed_selector(codec, &server_codecs, &codec_registry)
            }
            CodecSelection::Auto => build_auto_selector(&server_codecs, &codec_registry),
        }
    }

    pub(super) fn codec(&self) -> Codec {
        match self {
            Self::Fixed(c) => *c,
            Self::Auto(auto) => auto.current_codec,
        }
    }

    pub(super) fn step(&mut self, sample: &[MessageData]) {
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
    validate_codec_id(codec)?;

    if !server_codecs.is_empty() && !server_codecs.contains(&codec) {
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
    let candidates: Vec<Codec> = if server_codecs.is_empty() {
        CodecRegistry::sdk_builtin_codecs()
    } else {
        server_codecs.to_vec()
    };

    let accepted_encoders: Vec<Arc<dyn CompressionEncoder>> = candidates
        .iter()
        .filter_map(|&codec| registry.get_encoder(codec))
        .collect();

    let Some(first_encoder) = accepted_encoders.first() else {
        return Err(YdbError::custom(
            "no common codecs between server and client",
        ));
    };
    let first_codec = first_encoder.codec();

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

/// Picks the codec producing the smallest output for this sample.
/// Falls back to the first encoder if all encoders fail.
fn measure_codecs(sample: &[MessageData], encoders: &[Arc<dyn CompressionEncoder>]) -> Codec {
    debug_assert!(!encoders.is_empty());

    let mut best_codec = encoders[0].codec();
    let mut best_size = usize::MAX;

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

    const FAILING_CODEC: Codec = Codec { code: 10999 };

    #[derive(Debug)]
    struct FailingEncoder;

    impl CompressionEncoder for FailingEncoder {
        fn encode(&self, _data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + 'static>> {
            Err(YdbError::custom("test: encoder always fails").into())
        }

        fn codec(&self) -> Codec {
            FAILING_CODEC
        }
    }

    #[test]
    fn measure_codecs_falls_back_to_first_when_all_fail() {
        let mut registry = CodecRegistry::new();
        registry.register_encoder(Arc::new(FailingEncoder)).unwrap();

        let sample = vec![msg(b"payload".to_vec())];
        let encoders = vec![registry.get_encoder(FAILING_CODEC).unwrap()];

        assert_eq!(measure_codecs(&sample, &encoders), FAILING_CODEC);
    }

    #[test]
    fn measure_codecs_falls_back_to_raw_when_other_encoder_fails() {
        let mut registry = CodecRegistry::new();
        registry.register_encoder(Arc::new(FailingEncoder)).unwrap();

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
        registry.register_encoder(Arc::new(FailingEncoder)).unwrap();

        let sample = vec![msg(vec![0u8; 1024])];
        let encoders = vec![
            registry.get_encoder(Codec::RAW).unwrap(),
            registry.get_encoder(Codec::GZIP).unwrap(),
            registry.get_encoder(FAILING_CODEC).unwrap(),
        ];

        assert_eq!(measure_codecs(&sample, &encoders), Codec::GZIP);
    }

    #[test]
    fn selector_auto_with_non_empty_server_list_no_raw_succeeds() {
        let registry = CodecRegistry::new();
        let selector = CodecSelector::new(CodecSelection::Auto, vec![Codec::GZIP], registry.into());

        assert!(selector.is_ok());
    }

    #[test]
    fn selector_auto_empty_server_list_uses_sdk_builtins() {
        let registry = CodecRegistry::new();
        let selector = CodecSelector::new(CodecSelection::Auto, vec![], registry.into());

        assert!(selector.is_ok());
    }

    #[test]
    fn selector_fixed_empty_server_list_allows_any_valid_codec() {
        let registry = CodecRegistry::new();
        let selector =
            CodecSelector::new(CodecSelection::Fixed(Codec::GZIP), vec![], registry.into());

        assert!(selector.is_ok());
    }

    #[test]
    fn selector_fixed_non_empty_server_list_rejects_missing_codec() {
        let registry = CodecRegistry::new();
        let selector = CodecSelector::new(
            CodecSelection::Fixed(Codec::RAW),
            vec![Codec::GZIP],
            registry.into(),
        );

        assert!(selector.is_err());
    }

    #[test]
    fn selector_rejects_invalid_codec_id() {
        let registry = CodecRegistry::new();
        let selector = CodecSelector::new(
            CodecSelection::Fixed(Codec { code: 9001 }),
            vec![],
            registry.into(),
        );

        assert!(selector.is_err());
    }
}
