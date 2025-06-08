use crate::db::row::{Row, RowID};
use crate::generator::Generator;
use crate::metrics;
use crate::metrics::{MetricsCollector, OperationType};
use async_trait::async_trait;
use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::RateLimiter;
use rand::Rng;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use ydb::YdbResultWithCustomerErr;

pub type Attempts = usize;
type Limiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

#[async_trait]
pub trait ReadWriter: Clone + Send + Sync {
    async fn read(&self, row_id: RowID) -> (YdbResultWithCustomerErr<()>, Attempts);
    async fn write(&self, row: Row) -> (YdbResultWithCustomerErr<()>, Attempts);
}

pub struct Workers<RW: ReadWriter> {
    database: Arc<RW>,
    config: WorkersConfig,
    metrics: MetricsCollector,
}

impl<RW: ReadWriter> Workers<RW> {
    pub fn new(
        database: Arc<RW>,
        config: WorkersConfig,
        prom_pgw: String,
        metrics_ref: String,
        metrics_label: String,
        metrics_job_name: String,
    ) -> Arc<Workers<RW>> {
        let metrics = MetricsCollector::new(prom_pgw, metrics_ref, metrics_label, metrics_job_name);

        Arc::new(Self {
            database,
            config,
            metrics,
        })
    }

    pub async fn start_read_load(&self, limiter: &Limiter) {
        loop {
            limiter.until_ready().await;

            let row_id = rand::thread_rng().gen_range(0..self.config.initial_data_count);
            let span = self.metrics.start(OperationType::Read);

            let read_result = timeout(
                Duration::from_millis(self.config.read_timeout_seconds),
                self.database.read(row_id),
            )
            .await;

            match read_result {
                Ok((Ok(()), attempts)) => {
                    span.finish(attempts, None);
                    continue;
                }
                Ok((Err(e), attempts)) => {
                    span.finish(attempts, Some(e.clone()));
                    println!("Read failed: {}", e);
                    return;
                }
                Err(_) => return,
            }
        }
    }

    pub async fn start_write_load(&self, limiter: &Limiter, generator: &Generator) {
        loop {
            limiter.until_ready().await;

            let row = generator.to_owned().generate();
            let span = self.metrics.start(OperationType::Write);

            let write_result = timeout(
                Duration::from_millis(self.config.write_timeout_seconds),
                self.database.clone().write(row),
            )
            .await;

            match write_result {
                Ok((Ok(()), attempts)) => {
                    span.finish(attempts, None);
                    continue;
                }
                Ok((Err(e), attempts)) => {
                    span.finish(attempts, Some(e.clone()));
                    println!("Write failed: {}", e);
                    return;
                }
                Err(_) => return,
            }
        }
    }

    pub async fn collect_metrics(&self, limiter: &Limiter) {
        loop {
            limiter.until_ready().await;

            if let Err(err) = self.metrics.push_to_gateway().await {
                println!("Failed to collect metrics: {}", err);
                continue;
            }
        }
    }

    pub async fn close(&self) -> Result<(), WorkersCloseError> {
        self.metrics
            .reset()
            .await
            .map_err(|err| WorkersCloseError { value: err })
    }
}

pub struct WorkersConfig {
    pub initial_data_count: u64,
    pub read_timeout_seconds: u64,
    pub write_timeout_seconds: u64,
}

#[derive(Debug)]
pub struct WorkersCloseError {
    value: metrics::MetricsPushError,
}

impl From<metrics::MetricsPushError> for WorkersCloseError {
    fn from(err: metrics::MetricsPushError) -> WorkersCloseError {
        Self { value: err }
    }
}

impl Display for WorkersCloseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.value, f)
    }
}
