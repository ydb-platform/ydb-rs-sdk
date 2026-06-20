use crate::{Codec, YdbError, YdbResult};
use flate2::Compression;
use std::io::{Read, Write};

use super::codec_registry::{CompressionDecoder, CompressionEncoder};

#[derive(Debug)]
pub(super) struct RawEncoder;

impl CompressionEncoder for RawEncoder {
    fn encode(&self, data: &[u8]) -> YdbResult<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn codec(&self) -> Codec {
        Codec::RAW
    }
}

#[derive(Debug)]
pub(super) struct RawDecoder;

impl CompressionDecoder for RawDecoder {
    fn decode(&self, data: &[u8]) -> YdbResult<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn codec(&self) -> Codec {
        Codec::RAW
    }
}

#[derive(Debug)]
pub(super) struct GzipEncoder;

impl CompressionEncoder for GzipEncoder {
    fn encode(&self, data: &[u8]) -> YdbResult<Vec<u8>> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(data)
            .and_then(|_| encoder.finish())
            .map_err(|err| YdbError::custom(format!("gzip compress failed: {err}")))
    }

    fn codec(&self) -> Codec {
        Codec::GZIP
    }
}

#[derive(Debug)]
pub(super) struct GzipDecoder;

impl CompressionDecoder for GzipDecoder {
    fn decode(&self, data: &[u8]) -> YdbResult<Vec<u8>> {
        let mut decoder = flate2::read::GzDecoder::new(data.as_ref());
        let mut output = Vec::new();
        decoder
            .read_to_end(&mut output)
            .map_err(|err| YdbError::custom(format!("gzip decompress failed: {err}")))?;

        Ok(output)
    }

    fn codec(&self) -> Codec {
        Codec::GZIP
    }
}
