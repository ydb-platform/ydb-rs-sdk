use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;

pub struct RateLimiter {
    interval: Duration,
    next: Mutex<Instant>,
}

impl RateLimiter {
    pub async fn wait(&self) {
        // Holding the FIFO mutex while sleeping queues every caller behind one
        // timer instead of waking all workers to race for the same permit.
        let mut next = self.next.lock().await;
        sleep_until((*next).max(Instant::now())).await;
        *next = Instant::now() + self.interval;
    }
}

pub fn new_rate_limiter(rps: u32) -> RateLimiter {
    let rps = rps.max(1) as u64;
    let interval = Duration::from_nanos((1_000_000_000 / rps).max(1));

    RateLimiter {
        interval,
        next: Mutex::new(Instant::now()),
    }
}

pub async fn run_workers<F, Fut>(
    ctx: &CancellationToken,
    workers: usize,
    limiter: RateLimiter,
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
            loop {
                tokio::select! {
                    _ = ctx.cancelled() => break,
                    _ = limiter.wait() => {}
                }
                if ctx.is_cancelled() {
                    break;
                }

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
    use tokio::sync::mpsc;
    use tokio::time::{advance, timeout};

    #[tokio::test(start_paused = true)]
    async fn rate_limiter_does_not_catch_up_after_a_delay() {
        let limiter = new_rate_limiter(1000);
        let started = Instant::now();
        limiter.wait().await;
        assert_eq!(Instant::now(), started);

        advance(Duration::from_millis(10)).await;
        limiter.wait().await;

        assert!(timeout(Duration::ZERO, limiter.wait()).await.is_err());
        advance(Duration::from_millis(1)).await;
        limiter.wait().await;
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limiter_wakes_waiters_in_order() {
        let limiter = Arc::new(new_rate_limiter(1000));
        limiter.wait().await;

        let (tx, mut rx) = mpsc::unbounded_channel();
        for worker in 0..3 {
            let limiter = limiter.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                limiter.wait().await;
                tx.send(worker).expect("result receiver remains open");
            });
            tokio::task::yield_now().await;
        }
        drop(tx);

        for worker in 0..3 {
            advance(Duration::from_millis(1)).await;
            assert_eq!(rx.recv().await, Some(worker));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn workers_stop_while_waiting_for_a_permit() {
        let ctx = CancellationToken::new();
        let workers = tokio::spawn({
            let ctx = ctx.clone();
            async move {
                run_workers(&ctx, 2, new_rate_limiter(1), || async {}).await;
            }
        });

        tokio::task::yield_now().await;
        ctx.cancel();
        workers.await.expect("worker supervisor completes");
    }
}
