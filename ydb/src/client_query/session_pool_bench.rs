//! Pool acquire/release microbenchmark without CreateSession / AttachStream / DeleteSession RPC.
//!
//! Mirrors go-sdk `internal/pool/pool_test.go` (`newBenchPool`, `BenchmarkPoolWith`).
//!
//! Run (release mode recommended):
//! ```text
//! cargo test -p ydb query_session_pool_bench --release -- --ignored --nocapture
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::session_pool::{QuerySessionPool, SessionPoolSettings};

const BENCH_POOL_LIMIT: usize = 500;
const BENCH_PREFILL_ITEMS: usize = BENCH_POOL_LIMIT / 3;
const BENCH_DELETE_PROBABILITY: u64 = 20;
const BENCH_ITERS_PER_GOROUTINE: usize = 10_000;

fn new_bench_pool() -> QuerySessionPool {
    QuerySessionPool::new_explicit_bench(
        SessionPoolSettings::new()
            .with_limit(BENCH_POOL_LIMIT)
            .with_warm_up(BENCH_PREFILL_ITEMS),
    )
}

async fn bench_pool_once(pool: &QuerySessionPool, ops: &AtomicU64) {
    let force_delete = ops.fetch_add(1, Ordering::Relaxed) % BENCH_DELETE_PROBABILITY == 0;
    let mut lease = pool
        .acquire_explicit()
        .await
        .expect("acquire explicit session");
    lease.begin_use();
    lease.end_use();
    if force_delete {
        lease.bench_invalidate_session();
    }
    lease.return_to_pool().await;
}

fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 * pct / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn report_bench_latency(label: &str, samples: &mut [Duration]) {
    samples.sort();
    let n = samples.len();
    let total: Duration = samples.iter().sum();
    let mean = total / n as u32;
    let p50 = percentile(samples, 50.0);
    let p99 = percentile(samples, 99.0);
    eprintln!("{label}: n={n} mean={:?} p50={:?} p99={:?}", mean, p50, p99);
}

async fn benchmark_pool_with_concurrency(goroutines: usize) {
    let pool = new_bench_pool();
    let ops = Arc::new(AtomicU64::new(0));

    if goroutines == 1 {
        let mut samples = Vec::with_capacity(BENCH_ITERS_PER_GOROUTINE);
        for _ in 0..BENCH_ITERS_PER_GOROUTINE {
            let start = Instant::now();
            bench_pool_once(&pool, &ops).await;
            samples.push(start.elapsed());
        }
        report_bench_latency("query_session_pool_bench concurrency=1", &mut samples);
        return;
    }

    let per_worker = BENCH_ITERS_PER_GOROUTINE / goroutines;
    let extra = BENCH_ITERS_PER_GOROUTINE % goroutines;
    let mut handles = Vec::with_capacity(goroutines);
    let start_gate = Arc::new(tokio::sync::Barrier::new(goroutines + 1));

    for g in 0..goroutines {
        let pool = pool.clone();
        let ops = Arc::clone(&ops);
        let iterations = per_worker + usize::from(g < extra);
        let barrier = Arc::clone(&start_gate);
        handles.push(tokio::spawn(async move {
            let mut local = Vec::with_capacity(iterations);
            barrier.wait().await;
            for _ in 0..iterations {
                let t0 = Instant::now();
                bench_pool_once(&pool, &ops).await;
                local.push(t0.elapsed());
            }
            local
        }));
    }

    start_gate.wait().await;
    let mut merged = Vec::with_capacity(BENCH_ITERS_PER_GOROUTINE);
    for handle in handles {
        merged.extend(handle.await.expect("bench worker"));
    }
    report_bench_latency(
        &format!("query_session_pool_bench concurrency={goroutines}"),
        &mut merged,
    );
}

/// Acquire/release explicit session pool under load; RPC excluded via bench stub sessions.
///
/// benchmark name (release, Apple Silicon)   mean        p50         p99
/// query_session_pool_bench concurrency=1    ~165ns      ~166ns      ~250ns
/// query_session_pool_bench concurrency=500  ~8µs        ~875ns      ~76µs
/// query_session_pool_bench concurrency=1000 ~7.6µs      ~834ns      ~69µs
#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual pool microbenchmark; run with --release --ignored --nocapture"]
async fn query_session_pool_bench() {
    for goroutines in [1_usize, 250, 490, 500, 510, 1000] {
        benchmark_pool_with_concurrency(goroutines).await;
    }
}
