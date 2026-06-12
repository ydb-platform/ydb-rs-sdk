use std::sync::Arc;
use std::time::Duration;

use ratelimit::Ratelimiter;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

pub fn new_rate_limiter(rps: u32) -> Ratelimiter {
    Ratelimiter::builder(rps as u64, Duration::from_secs(1))
        .build()
        .expect("valid ratelimiter")
}

pub async fn run_workers<F, Fut>(ctx: &CancellationToken, workers: usize, limiter: Ratelimiter, f: F)
where
    F: Fn() -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let limiter = Arc::new(limiter);
    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let ctx = ctx.clone();
        let limiter = limiter.clone();
        let f = f.clone();
        handles.push(tokio::spawn(async move {
            while !ctx.is_cancelled() {
                if let Err(wait) = limiter.try_wait() {
                    sleep(wait).await;
                    continue;
                }
                f().await;
            }
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }
}
