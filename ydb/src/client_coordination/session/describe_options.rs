use crate::errors;
use derive_builder::Builder;

#[derive(Clone)]
#[allow(dead_code)]
pub enum WatchMode {
    Data,
    Owners,
    All,
}

#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
#[allow(dead_code)]
pub struct DescribeOptions {
    #[builder(default = "false")]
    pub(crate) with_owners: bool,

    #[builder(default = "false")]
    pub(crate) with_waiters: bool,
}

#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
#[allow(dead_code)]
pub struct WatchOptions {
    #[builder(default = "WatchMode::Data")]
    pub(crate) watch_mode: WatchMode,

    #[builder(default = "DescribeOptionsBuilder::default().build()?")]
    pub(crate) describe_options: DescribeOptions,
}
