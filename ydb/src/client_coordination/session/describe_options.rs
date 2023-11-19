use crate::errors;
use derive_builder::Builder;
use tokio::sync::mpsc::Sender;

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

    #[builder(default = "WatchMode::Data")]
    pub(crate) watch_mode: WatchMode,

    #[builder(setter(strip_option), default)]
    pub(crate) on_changed: Option<Sender<()>>,
}
