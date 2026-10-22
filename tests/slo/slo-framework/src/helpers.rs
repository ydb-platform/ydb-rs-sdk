use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
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
        let scheduled = (*next).max(Instant::now());
        sleep_until(scheduled).await;
        *next = scheduled + self.interval;
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

/// Keeps the first failure as context and the later failure as the source.
pub fn preserve_primary_error(primary: Result<()>, additional: Result<()>) -> Result<()> {
    match (primary, additional) {
        (Ok(()), additional) => additional,
        (Err(primary), Ok(())) => Err(primary),
        (Err(primary), Err(additional)) => Err(additional.context(format!("{primary:#}"))),
    }
}

pub async fn run_workers<F, Fut>(
    ctx: &CancellationToken,
    workers: usize,
    limiter: RateLimiter,
    f: F,
) -> Result<()>
where
    F: Fn() -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let limiter = Arc::new(limiter);
    let mut handles = JoinSet::new();
    for _ in 0..workers {
        let ctx = ctx.clone();
        let limiter = limiter.clone();
        let f = f.clone();
        handles.spawn(async move {
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
        });
    }

    let mut failure = None;
    while let Some(result) = handles.join_next().await {
        if let Err(error) = result
            && failure.is_none()
        {
            failure = Some(error);
            ctx.cancel();
        }
    }
    match failure {
        Some(error) => Err(error.into()),
        None => Ok(()),
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
                run_workers(&ctx, 2, new_rate_limiter(1), || async {})
                    .await
                    .expect("planned stop succeeds");
            }
        });

        tokio::task::yield_now().await;
        ctx.cancel();
        workers.await.expect("worker supervisor completes");
    }

    #[tokio::test(start_paused = true)]
    async fn worker_panic_cancels_peers_and_preserves_join_error() {
        let ctx = CancellationToken::new();
        let error = timeout(
            Duration::from_secs(1),
            run_workers(&ctx, 2, new_rate_limiter(1), || async {
                panic!("worker invariant failed");
            }),
        )
        .await
        .expect("supervisor settles without waiting for another permit")
        .expect_err("worker panic must fail the supervisor");

        assert!(
            error
                .downcast_ref::<tokio::task::JoinError>()
                .is_some_and(tokio::task::JoinError::is_panic)
        );
        assert!(ctx.is_cancelled());
    }
}
