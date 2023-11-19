use crate::errors;
use derive_builder::Builder;

#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
#[allow(dead_code)]
pub struct UpdateSemaphoreOptions {
    #[builder(setter(strip_option), default)]
    pub data: Option<Vec<u8>>,
}
