use crate::{YdbError, YdbResult};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use prost::bytes::Bytes;
use std::io::{Read, Write};

pub fn gzip_compress(data: &Bytes) -> YdbResult<Bytes> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(data)
        .and_then(|_| encoder.finish())
        .map_err(|err| YdbError::custom(format!("gzip compress failed: {err}")))
        .map(Bytes::from)
}

pub fn gzip_decompress(data: &Bytes) -> YdbResult<Bytes> {
    let mut decoder = GzDecoder::new(data.as_ref());
    let mut output = Vec::new();
    decoder
        .read_to_end(&mut output)
        .map_err(|err| YdbError::custom(format!("gzip decompress failed: {err}")))
        .map(|_| Bytes::from(output))
}
