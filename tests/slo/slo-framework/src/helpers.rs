use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use ratelimit::Ratelimiter;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

pub fn new_rate_limiter(rps: u32) -> Ratelimiter {
    let rps = rps.max(1) as u64;
    let interval = Duration::from_nanos(1_000_000_000 / rps);
    Ratelimiter::builder(1, interval)
        .max_tokens(1)
        .initial_available(1)
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

pub async fn run_workers_for<I, F, Fut>(tasks: I)
where
    I: IntoIterator<Item = F>,
    F: FnOnce() -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut set = tokio::task::JoinSet::new();
    for task in tasks {
        set.spawn(task());
    }
    while set.join_next().await.is_some() {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_style_rate_limiter_supports_default_slo_rps() {
        let rl = new_rate_limiter(1000);
        assert!((rl.rate() - 1000.0).abs() < 1.0);
        let rl = new_rate_limiter(100);
        assert!((rl.rate() - 100.0).abs() < 1.0);
    }
}
