use crate::client_topic::compression::codec_registry::CodecRegistry;
use crate::client_topic::list_types::Codec;
use crate::{YdbError, YdbResult};
use itertools::Itertools;
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
    Auto {
        allowed_codecs: Vec<Codec>,
        codec_registry: Arc<CodecRegistry>,
        batch_counter: usize,
        current_codec: Codec,
        measure_interval: usize,
    },
}

impl CodecSelector {
    pub(crate) fn new(
        selection: CodecSelection,
        server_codecs: Vec<Codec>,
        codec_registry: Arc<CodecRegistry>,
    ) -> YdbResult<Self> {
        match selection {
            CodecSelection::Fixed(codec) => {
                // Empty server_codecs means "all defaults allowed" — same semantics
                // as in calculate_allowed_codecs below.
                if !server_codecs.is_empty() && !server_codecs.contains(&codec) {
                    return Err(YdbError::custom(format!(
                        "codec {:?} is not supported by the topic (supported_codecs: {:?})",
                        codec, server_codecs
                    )));
                }

                if !codec_registry.supported_encoders().contains(&codec) {
                    return Err(YdbError::custom(format!(
                        "codec {:?} is not registered in the codec registry",
                        codec
                    )));
                }

                Ok(Self::Fixed(codec))
            }
            CodecSelection::Auto => {
                let allowed = calculate_allowed_codecs(&codec_registry, server_codecs);
                if allowed.is_empty() {
                    return Err(YdbError::custom(
                        "no common codecs between server and client",
                    ));
                }
                if allowed.len() == 1 {
                    return Ok(Self::Fixed(allowed[0]));
                }
                Ok(Self::Auto {
                    current_codec: allowed[0],
                    allowed_codecs: allowed,
                    codec_registry,
                    batch_counter: 0,
                    measure_interval: DEFAULT_MEASURE_INTERVAL,
                })
            }
        }
    }

    pub(crate) fn codec(&self) -> Codec {
        match self {
            Self::Fixed(c) => *c,
            Self::Auto { current_codec, .. } => *current_codec,
        }
    }

    pub(crate) fn step(&mut self, sample: &[MessageData]) {
        if let Self::Auto {
            allowed_codecs,
            codec_registry,
            batch_counter,
            current_codec,
            measure_interval,
        } = self
        {
            if *batch_counter % *measure_interval == 0 {
                if let Some(best) = measure_codecs(sample, allowed_codecs, codec_registry) {
                    *current_codec = best;
                }
            }
            *batch_counter += 1;
        }
    }
}

fn calculate_allowed_codecs(registry: &CodecRegistry, server_codecs: Vec<Codec>) -> Vec<Codec> {
    let server_encoders = if server_codecs.is_empty() {
        CodecRegistry::default().supported_encoders()
    } else {
        server_codecs.into_iter().collect()
    };

    server_encoders
        .intersection(&registry.supported_encoders())
        .copied()
        .collect_vec()
}

fn measure_codecs(
    sample: &[MessageData],
    codecs: &[Codec],
    registry: &CodecRegistry,
) -> Option<Codec> {
    if sample.is_empty() || codecs.is_empty() {
        return codecs.first().copied();
    }

    let mut best_codec = None;
    let mut best_size = usize::MAX;

    'codecs: for &codec in codecs {
        let total_size: usize = if codec == Codec::RAW {
            sample.iter().map(|m| m.data.len()).sum()
        } else {
            let Some(encoder) = registry.get_encoder(codec) else {
                continue 'codecs;
            };

            let mut size = 0;
            for msg in sample {
                match encoder.encode(&msg.data) {
                    Ok(compressed) => size += compressed.len(),
                    Err(_) => continue 'codecs,
                }
            }

            size
        };

        if total_size < best_size {
            best_size = total_size;
            best_codec = Some(codec);
        }
    }

    best_codec
}
