use crate::Codec;
use flate2::Compression;
use std::{
    error::Error,
    io::{Read, Write},
    vec::Vec,
};

use super::codec_registry::{CompressionDecoder, CompressionEncoder};

#[derive(Debug)]
pub(super) struct RawEncoder;

impl CompressionEncoder for RawEncoder {
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn Error + 'static>> {
        Ok(data.to_vec())
    }

    fn codec(&self) -> Codec {
        Codec::RAW
    }
}

#[derive(Debug)]
pub(super) struct RawDecoder;

impl CompressionDecoder for RawDecoder {
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn Error + 'static>> {
        Ok(data.to_vec())
    }

    fn codec(&self) -> Codec {
        Codec::RAW
    }
}

#[derive(Debug)]
pub(super) struct GzipEncoder;

impl CompressionEncoder for GzipEncoder {
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn Error + 'static>> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data)?;
        Ok(encoder.finish()?)
    }

    fn codec(&self) -> Codec {
        Codec::GZIP
    }
}

#[derive(Debug)]
pub(super) struct GzipDecoder;

impl CompressionDecoder for GzipDecoder {
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn Error + 'static>> {
        let mut decoder = flate2::read::GzDecoder::new(data);
        let mut output = Vec::new();
        decoder.read_to_end(&mut output)?;

        Ok(output)
    }

    fn codec(&self) -> Codec {
        Codec::GZIP
    }
}
