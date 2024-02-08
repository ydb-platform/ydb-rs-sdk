use crate::errors;
use derive_builder::Builder;
use std::time::Duration;

#[allow(dead_code)]
#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct SessionOptions {
    #[builder(default = "Duration::from_secs(20)")]
    pub(crate) timeout: Duration,

    #[builder(setter(strip_option), default)]
    pub(crate) description: Option<String>,
    // TODO: seq_no: auto / custom
    // TODO: protection_key: auto / custom
}
