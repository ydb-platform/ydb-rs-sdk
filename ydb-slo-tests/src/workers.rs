use crate::args::RunArgs;
use crate::generator::Generator;
use crate::row::{RowID, TestRow};
use async_trait::async_trait;
use rand::Rng;
use ratelimit::Ratelimiter;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use ydb::{Row, YdbResult, YdbResultWithCustomerErr};

#[async_trait]
pub trait ReadWriter: Clone + Send + Sync {
    async fn read(&self, row_id: RowID) -> YdbResult<Row>;
    async fn write(&self, row: TestRow) -> YdbResultWithCustomerErr<()>;
}

pub struct Workers<RW: ReadWriter> {
    database: Arc<RW>,
    config: RunArgs,
}

impl<RW: ReadWriter> Workers<RW> {
    pub fn new(database: Arc<RW>, config: RunArgs) -> Arc<Workers<RW>> {
        Arc::new(Self { database, config })
    }

    pub async fn start_read_load(&self, limiter: &Ratelimiter, cancel: CancellationToken) {
        loop {
            if cancel.is_cancelled() {
                return;
            }

            if let Err(interval) = limiter.try_wait() {
                sleep(interval).await;
                continue;
            }

            let row_id = rand::thread_rng().gen_range(0..self.config.initial_data_count);

            let read_result = timeout(
                Duration::from_millis(self.config.read_timeout),
                self.database.read(row_id),
            )
            .await;

            match read_result {
                Ok(Ok(_)) => {
                    continue;
                }
                Ok(Err(err)) => {
                    println!("read failed: {}", err);
                    return;
                }
                Err(_) => {
                    return;
                }
            }
        }
    }

    pub async fn start_write_load(
        &self,
        limiter: &Ratelimiter,
        generator: &Generator,
        cancel: CancellationToken,
    ) {
        loop {
            if cancel.is_cancelled() {
                return;
            }

            if let Err(interval) = limiter.try_wait() {
                sleep(interval).await;
                continue;
            }

            let row = generator.to_owned().generate();

            let write_result = timeout(
                Duration::from_millis(self.config.write_timeout),
                self.database.clone().write(row),
            )
            .await;

            match write_result {
                Ok(Ok(_)) => {
                    continue;
                }
                Ok(Err(err)) => {
                    println!("write failed: {}", err);
                    return;
                }
                Err(_) => {
                    return;
                }
            }
        }
    }
}
