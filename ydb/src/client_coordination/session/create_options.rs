#[derive(Clone)]
#[allow(dead_code)]
pub enum SemaphoreLimit {
    Mutex,
    Unbounded,
    Custom(u32),
}
