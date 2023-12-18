#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum SemaphoreLimit {
    Mutex,
    Unbounded,
    Custom(u64),
}

impl From<SemaphoreLimit> for u64 {
    fn from(value: SemaphoreLimit) -> Self {
        match value {
            SemaphoreLimit::Mutex => 1,
            SemaphoreLimit::Unbounded => u64::MAX,
            SemaphoreLimit::Custom(limit) => limit,
        }
    }
}
