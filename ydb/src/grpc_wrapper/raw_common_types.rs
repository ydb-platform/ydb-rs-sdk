#[derive(Debug)]
pub(crate) struct Duration {
    val: std::time::Duration,
}

impl From<std::time::Duration> for Duration {
    fn from(val: std::time::Duration) -> Self {
        Self { val }
    }
}

impl From<Duration> for pbjson_types::Duration {
    fn from(d: Duration) -> Self {
        Self {
            seconds: d.val.as_secs() as i64,
            nanos: d.val.subsec_nanos() as i32,
        }
    }
}
