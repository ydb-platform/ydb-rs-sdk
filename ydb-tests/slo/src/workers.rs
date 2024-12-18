use crate::rate_limiter::RateLimiter;
use log::{error, info};
use std::sync::Arc;
use rand::Rng;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub struct Config {
    pub push_gateway: String,
    pub some_other_config: String,
}

pub struct Generator {
    pub row_id: u64,
}

pub struct Row {
    pub id: u64,
    pub payload: String,
}

#[async_trait::async_trait]
pub trait ReadWriter {
    async fn read(
        &self,
        ctx: &CancellationToken,
        row_id: u64,
    ) -> Result<(Row, usize, Option<String>), String>;
    async fn write(
        &self,
        ctx: &CancellationToken,
        row: Row,
    ) -> Result<(usize, Option<String>), String>;
}

pub struct Metrics {
    pub push_gateway: String,
    pub ref_label: String,
    pub job_name: String,
}

impl Metrics {
    pub fn new(
        push_gateway: &str,
        ref_label: &str,
        label: &str,
        job_name: &str,
    ) -> Result<Self, String> {
        Ok(Metrics {
            push_gateway: push_gateway.to_string(),
            ref_label: ref_label.to_string(),
            job_name: job_name.to_string(),
        })
    }

    pub fn reset(&self) -> Result<(), String> {
        info!("Metrics reset successfully.");
        Ok(())
    }
}

pub struct Workers {
    cfg: Arc<Config>,
    s: Arc<dyn ReadWriter + Send + Sync>,
    m: Arc<Metrics>,
}

impl Workers {
    pub fn new(
        cfg: Arc<Config>,
        s: Arc<dyn ReadWriter + Send + Sync>,
        ref_label: &str,
        label: &str,
        job_name: &str,
    ) -> Result<Self, String> {
        let m = Metrics::new(&cfg.push_gateway, ref_label, label, job_name)?;
        Ok(Workers {
            cfg,
            s,
            m: Arc::new(m),
        })
    }

    pub async fn read(
        &self,
        ctx: CancellationToken,
        limiter: Arc<RateLimiter>,
    ) -> Result<(), String> {
        let limiter = limiter.clone();
        let cancellation = ctx.clone();

        while !cancellation.is_cancelled() {
            if limiter.wait().await.is_err() {
                return Ok(());
            }

            match self.execute_read(ctx.clone()).await {
                Ok(_) => {}
                Err(err) => {
                    if !cancellation.is_cancelled() {
                        error!("Read failed: {}", err);
                    }
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    async fn execute_read(&self, ctx: CancellationToken) -> Result<(), String> {
        let id = rand::thread_rng().gen_range(0..self.cfg.initial_data_count);

        let metric = self.m.start(OperationType::Read);
        let (_result, attempts, err) = self.s.read(ctx.clone(), id);
        metric.finish(err.clone(), attempts);

        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    pub async fn write(
        &self,
        ctx: CancellationToken,
        limiter: Arc<RateLimiter>,
        gen: Arc<Mutex<Generator>>,
    ) -> Result<(), String> {
        let limiter = limiter.clone();
        let cancellation = ctx.clone();

        loop {
            if cancellation.is_cancelled() {
                break;
            }

            if limiter.wait().await.is_err() {
                break;
            }

            match self.execute_write(ctx.clone(), gen.clone()).await {
                Ok(_) => {}
                Err(err) => {
                    if !cancellation.is_cancelled() {
                        error!("Write failed: {}", err);
                    }
                }
            }
        }
        Ok(())
    }

    async fn execute_write(
        &self,
        ctx: CancellationToken,
        gen: Arc<Mutex<Generator>>,
    ) -> Result<(), String> {
        let mut generator = gen.lock().await;
        let row = match generator.generate() {
            Ok(row) => row,
            Err(e) => {
                error!("Generate error: {}", e);
                return Err(e);
            }
        };

        let metric = self.m.start(OperationType::Write);
        let (attempts, err) = self.s.write(ctx.clone(), row).await;
        metric.finish(err.clone(), attempts);

        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    pub async fn close(&self) -> Result<(), String> {
        self.m.reset()
    }
}
