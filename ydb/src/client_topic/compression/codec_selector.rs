use crate::client_topic::compression::codec_registry::{CodecRegistry, CompressionEncoder};
use crate::client_topic::compression::executor::Executor;
use crate::client_topic::list_types::Codec;
use crate::{YdbError, YdbResult};
use futures_util::stream::{FuturesUnordered, StreamExt};
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::error;
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
    executor: Arc<dyn Executor>,
}

impl CodecSelector {
    /// Builds a writer-side codec selector.
    ///
    /// Fixed mode requires a registered encoder for the requested codec.
    /// If the server reports codec restrictions, the codec must be allowed by that list.
    ///
    /// Auto mode chooses between registered encoders allowed by the server.
    /// Empty server codec list means no server-side restriction.
    ///
    /// Custom codecs are codec codes in `10_000..20_000`. For stream-write `InitResponse`,
    /// `UNSPECIFIED` in the server list allows registered custom codecs.
    pub(super) fn new(
        selection: CodecSelection,
        server_codecs: Vec<Codec>,
        codec_registry: Arc<CodecRegistry>,
        executor: Arc<dyn Executor>,
    ) -> YdbResult<Self> {
        match selection {
            CodecSelection::Fixed(codec) => {
                build_fixed_selector(codec, &server_codecs, &codec_registry)
            }
            CodecSelection::Auto => build_auto_selector(&server_codecs, &codec_registry, executor),
        }
    }

    pub(super) fn codec(&self) -> Codec {
        match self {
            Self::Fixed(c) => *c,
            Self::Auto(auto) => auto.current_codec,
        }
    }

    pub(super) async fn step(&mut self, sample: &[MessageData]) {
        if let Self::Auto(auto) = self {
            if auto.batch_counter % auto.measure_interval == 0 {
                auto.current_codec =
                    measure_codecs(sample, &auto.accepted_encoders, auto.executor.as_ref()).await;
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
    if !server_codecs.is_empty() {
        let explicitly_allowed = server_codecs.contains(&codec);

        // BUG: This is a workaround for YDB server-side bug: topic writer InitRequest returns custom
        // codecs as [`Codec::UNSPECIFIED`]
        let custom_slot_available =
            codec.is_custom() && server_codecs.contains(&Codec::UNSPECIFIED);

        if !explicitly_allowed && !custom_slot_available {
            return Err(YdbError::custom(format!(
                "codec {:?} is not supported by the topic (supported_codecs: {:?})",
                codec, server_codecs
            )));
        }
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
    executor: Arc<dyn Executor>,
) -> YdbResult<CodecSelector> {
    let mut candidates: Vec<Codec> = if server_codecs.is_empty() {
        CodecRegistry::sdk_builtin_codecs()
    } else {
        server_codecs.to_vec()
    };

    // Ensure deterministic order of codecs
    candidates.sort();

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
        executor,
    }))
}

/// Picks the codec producing the smallest output for this sample by running each
/// candidate encoder in parallel on the executor. Falls back to the first
/// encoder if all encoders fail.
async fn measure_codecs(
    sample: &[MessageData],
    encoders: &[Arc<dyn CompressionEncoder>],
    executor: &dyn Executor,
) -> Codec {
    debug_assert!(!encoders.is_empty());

    // One copy of sample bytes; each task gets a cheap Arc clone.
    let sample_data: Arc<[Arc<[u8]>]> = sample
        .iter()
        .map(|m| Arc::from(m.data.as_slice()))
        .collect();

    let mut pending = FuturesUnordered::new();
    for encoder in encoders {
        let (tx, rx) = oneshot::channel::<Option<(Codec, usize)>>();
        pending.push(rx);

        let encoder = encoder.clone();
        let data = sample_data.clone();
        executor.spawn(Box::new(move || {
            let codec = encoder.codec();
            let mut total = 0usize;
            for bytes in data.iter() {
                match encoder.encode(bytes) {
                    Ok(c) => total += c.len(),
                    Err(err) => {
                        error!(?encoder, err, "failed to measure");
                        let _ = tx.send(None);
                        return;
                    }
                }
            }
            let _ = tx.send(Some((codec, total)));
        }));
    }

    let mut best_codec = encoders[0].codec();
    let mut best_size = usize::MAX;

    while let Some(result) = pending.next().await {
        if let Ok(Some((codec, size))) = result {
            if size < best_size {
                best_size = size;
                best_codec = codec;
            }
        }
    }

    best_codec
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_topic::compression::executor::InplaceExecutor;

    fn test_executor() -> Arc<dyn Executor> {
        Arc::new(InplaceExecutor::new())
    }

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

    #[tokio::test]
    async fn measure_codecs_falls_back_to_first_when_all_fail() {
        let mut registry = CodecRegistry::new();
        registry.register_encoder(Arc::new(FailingEncoder));

        let sample = vec![msg(b"payload".to_vec())];
        let encoders = vec![registry.get_encoder(FAILING_CODEC).unwrap()];

        let executor = test_executor();
        assert_eq!(
            measure_codecs(&sample, &encoders, executor.as_ref()).await,
            FAILING_CODEC
        );
    }

    #[tokio::test]
    async fn measure_codecs_falls_back_to_raw_when_other_encoder_fails() {
        let mut registry = CodecRegistry::new();
        registry.register_encoder(Arc::new(FailingEncoder));

        let sample = vec![msg(b"payload".to_vec())];
        let encoders = vec![
            registry.get_encoder(FAILING_CODEC).unwrap(),
            registry.get_encoder(Codec::RAW).unwrap(),
        ];

        let executor = test_executor();
        assert_eq!(
            measure_codecs(&sample, &encoders, executor.as_ref()).await,
            Codec::RAW
        );
    }

    #[tokio::test]
    async fn measure_codecs_selects_best_non_failing() {
        let mut registry = CodecRegistry::new();
        registry.register_encoder(Arc::new(FailingEncoder));

        let sample = vec![msg(vec![0u8; 1024])];
        let encoders = vec![
            registry.get_encoder(Codec::RAW).unwrap(),
            registry.get_encoder(Codec::GZIP).unwrap(),
            registry.get_encoder(FAILING_CODEC).unwrap(),
        ];

        let executor = test_executor();
        assert_eq!(
            measure_codecs(&sample, &encoders, executor.as_ref()).await,
            Codec::GZIP
        );
    }

    #[test]
    fn selector_auto_with_non_empty_server_list_no_raw_succeeds() {
        let registry = CodecRegistry::new();
        let selector = CodecSelector::new(
            CodecSelection::Auto,
            vec![Codec::GZIP],
            registry.into(),
            test_executor(),
        );

        assert!(selector.is_ok());
    }

    #[test]
    fn selector_auto_empty_server_list_uses_sdk_builtins() {
        let registry = CodecRegistry::new();
        let selector = CodecSelector::new(
            CodecSelection::Auto,
            vec![],
            registry.into(),
            test_executor(),
        );

        assert!(selector.is_ok());
    }

    #[test]
    fn selector_fixed_empty_server_list_allows_any_valid_codec() {
        let registry = CodecRegistry::new();
        let selector = CodecSelector::new(
            CodecSelection::Fixed(Codec::GZIP),
            vec![],
            registry.into(),
            test_executor(),
        );

        assert!(selector.is_ok());
    }

    #[test]
    fn selector_fixed_non_empty_server_list_rejects_missing_codec() {
        let registry = CodecRegistry::new();
        let selector = CodecSelector::new(
            CodecSelection::Fixed(Codec::RAW),
            vec![Codec::GZIP],
            registry.into(),
            test_executor(),
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
            test_executor(),
        );

        assert!(selector.is_err());
    }

    const CUSTOM_CODEC: Codec = Codec { code: 10001 };

    #[derive(Debug)]
    struct CustomEncoder;

    impl CompressionEncoder for CustomEncoder {
        fn encode(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + 'static>> {
            Ok(data.to_vec())
        }
        fn codec(&self) -> Codec {
            CUSTOM_CODEC
        }
    }

    fn registry_with_custom() -> CodecRegistry {
        let mut registry = CodecRegistry::new();
        registry.register_encoder(Arc::new(CustomEncoder));
        registry
    }

    #[test]
    fn selector_fixed_custom_codec_with_unspecified_in_server_list_succeeds() {
        // Server returns [RAW, UNSPECIFIED] — custom codec slot is present.
        let selector = CodecSelector::new(
            CodecSelection::Fixed(CUSTOM_CODEC),
            vec![Codec::RAW, Codec::UNSPECIFIED],
            registry_with_custom().into(),
            test_executor(),
        );
        assert!(selector.is_ok());
    }

    #[test]
    fn selector_fixed_custom_codec_without_unspecified_in_server_list_fails() {
        // Server returns [RAW] only — no custom codec slot.
        let selector = CodecSelector::new(
            CodecSelection::Fixed(CUSTOM_CODEC),
            vec![Codec::RAW],
            registry_with_custom().into(),
            test_executor(),
        );
        assert!(selector.is_err());
    }

    #[test]
    fn selector_fixed_standard_codec_with_unspecified_in_server_list_fails() {
        // UNSPECIFIED does not grant access to a standard codec not in the list.
        let selector = CodecSelector::new(
            CodecSelection::Fixed(Codec::GZIP),
            vec![Codec::RAW, Codec::UNSPECIFIED],
            CodecRegistry::new().into(),
            test_executor(),
        );
        assert!(selector.is_err());
    }
}
