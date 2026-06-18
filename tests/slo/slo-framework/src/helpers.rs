use std::sync::Arc;
use std::time::Duration;

use ratelimit::Ratelimiter;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

pub fn new_rate_limiter(rps: u32) -> Ratelimiter {
    let rps = rps.max(1) as u64;
    Ratelimiter::builder(rps, Duration::from_secs(1))
        .max_tokens(rps)
        .build()
        .expect("valid ratelimiter")
}

pub async fn run_workers<F, Fut>(
    ctx: &CancellationToken,
    workers: usize,
    limiter: Ratelimiter,
    f: F,
) where
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

/// Workers fire operations as fast as the SDK allows (no shared rate limiter).
pub async fn run_workers_unlimited<F, Fut>(ctx: &CancellationToken, workers: usize, f: F)
where
    F: Fn() -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let ctx = ctx.clone();
        let f = f.clone();
        handles.push(tokio::spawn(async move {
            while !ctx.is_cancelled() {
                f().await;
            }
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limiter_supports_default_slo_rps() {
        new_rate_limiter(1000);
        new_rate_limiter(100);
    }
}
