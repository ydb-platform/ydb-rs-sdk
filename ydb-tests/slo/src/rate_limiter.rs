use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::time::sleep;

pub struct RateLimiter {
    permits: Arc<Semaphore>,
    interval: Duration,
}

impl RateLimiter {
    pub fn new(rate: u32, duration: Duration) -> Self {
        RateLimiter {
            permits: Semaphore::new(rate as usize),
            interval: duration,
        }
    }

    pub async fn wait(&self) -> Result<(), String> {
        let start = Instant::now();
        if self.permits.acquire().await.is_ok() {
            sleep(self.interval).await;
            Ok(())
        }
        Ok(())
    }
}
