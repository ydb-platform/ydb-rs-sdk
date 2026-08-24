use crate::errors;
use std::time::Duration;

#[derive(derive_builder::Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
#[allow(dead_code)]
pub struct AcquireOptions {
    #[builder(default = "Vec::new()")]
    pub(crate) data: Vec<u8>,

    #[builder(default = "false")]
    pub(crate) ephemeral: bool,

    #[builder(default = "Duration::from_secs(20)")]
    pub(crate) timeout: Duration,
}
