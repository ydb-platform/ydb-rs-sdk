// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.

use crate::{errors, YdbError, YdbResult};
use derive_builder::Builder;
use std::time;

#[derive(Builder)]
#[builder(build_fn(error = "errors::YdbError", validate = "Self::validate"))]
#[allow(dead_code)]
pub struct TopicWriterMessage {
    #[builder(default = "0")]
    seq_no: i64,
    #[builder(default = "time::SystemTime::now()")]
    created_at: time::SystemTime,

    data: Vec<u8>,
}

impl TopicWriterMessageBuilder {
    fn validate(&self) -> YdbResult<()> {
        if let Some(ref data) = self.data {
            match data {
                data if data.is_empty() => Err(YdbError::Convert(
                    "Expected non empty message content".to_string(),
                )),
                _ => Ok(()),
            }
        } else {
            Ok(())
        }
    }
}
