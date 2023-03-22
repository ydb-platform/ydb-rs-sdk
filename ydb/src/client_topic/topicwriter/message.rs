// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.

use prost::bytes::Bytes;
use derive_builder::{Builder};
use crate::errors;

#[derive(Builder)]
#[builder(build_fn(error = "errors::YdbError"))]
#[allow(dead_code)]
pub struct TopicWriterMessage{
    seq_no: i64,
    created_at: std::time::Instant,
    data: Bytes
}

impl TopicWriterMessage{
    pub fn new<T: Into<Bytes>>(message: T) -> Self{
        Self{
            seq_no: 0,
            created_at: std::time::Instant::now(),
            data: message.into(),
        }
    }
}
