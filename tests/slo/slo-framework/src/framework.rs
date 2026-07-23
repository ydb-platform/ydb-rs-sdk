use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Error, Result};
use tokio::signal::unix::{SignalKind, signal};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::helpers::preserve_primary_error;
use crate::logger::{Logger, Phase};
use crate::metrics::Metrics;

const SHUTDOWN_DURATION: Duration = Duration::from_secs(30);
const WORKLOAD_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone)]
pub struct Framework {
    pub config: Config,
    pub metrics: Metrics,
    pub logger: Arc<Logger>,
}

#[async_trait::async_trait]
pub trait Workload: Send {
    async fn setup(&self, ctx: &CancellationToken) -> Result<()>;
    async fn run(&self, ctx: &CancellationToken) -> Result<()>;
    async fn teardown(&self, ctx: &CancellationToken) -> Result<()>;
}

pub async fn run<F, Fut>(factory: F) -> std::result::Result<(), String>
where
    F: for<'a> Fn(&'a Framework) -> Fut,
    Fut: Future<Output = std::result::Result<Box<dyn Workload>, String>> + Send,
{
    let cancel = CancellationToken::new();
    let cancel_bg = cancel.clone();

    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
        let mut sigquit = signal(SignalKind::quit()).expect("SIGQUIT handler");

        tokio::select! {
            _ = sigterm.recv() => {},
            _ = sigint.recv() => {},
            _ = sigquit.recv() => {},
        }
        cancel_bg.cancel();
    });

    let config = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(err) => {
            return Err(format!("create config failed: {err}"));
        }
    };

    let logger = Arc::new(Logger::new());
    let metrics = match Metrics::new(&config) {
        Ok(m) => m,
        Err(err) => {
            return Err(format!("create metrics failed: {err}"));
        }
    };

    let fw = Framework {
        config,
        metrics: metrics.clone(),
        logger: logger.clone(),
    };

    logger.printf("program started");

    let workload = match factory(&fw).await {
        Ok(w) => w,
        Err(err) => {
            logger.flush();
            return Err(format!("create workload failed: {err}"));
        }
    };

    let teardown_result = async {
        logger.set_phase(Phase::Teardown);
        timeout(SHUTDOWN_DURATION, workload.teardown(&cancel))
            .await
            .context("teardown timed out")?
    };

    let run_result: Result<()> = async {
        logger.set_phase(Phase::Setup);
        workload.setup(&cancel).await?;
        logger.printf("setup ok");

        logger.set_phase(Phase::Run);
        let run_duration = fw.config.run_duration();
        let run_cancel = cancel.child_token();
        let mut run_fut = Box::pin(workload.run(&run_cancel));

        tokio::select! {
            res = &mut run_fut => res?,
            _ = sleep(run_duration) => {
                run_cancel.cancel();
                wait_for_workload_shutdown(&mut run_fut, WORKLOAD_SHUTDOWN_TIMEOUT).await?;
            }
            _ = cancel.cancelled() => {
                run_cancel.cancel();
                wait_for_workload_shutdown(&mut run_fut, WORKLOAD_SHUTDOWN_TIMEOUT).await?;
            }
        }

        logger.printf("run ok");
        Ok(())
    }
    .await;

    if run_result.is_ok() {
        logger.printf("workload completed successfully");
    }

    let teardown_result = teardown_result.await.context("teardown failed");
    let result = preserve_primary_error(run_result, teardown_result);
    let result = preserve_primary_error(result, metrics.check().map_err(Error::msg));

    logger.flush();
    if result.is_ok() {
        logger.printf("program finished");
    }

    result.map_err(|err| format!("{err:#}"))
}

async fn wait_for_workload_shutdown<F>(run_fut: F, shutdown_timeout: Duration) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    timeout(shutdown_timeout, run_fut)
        .await
        .context("workload did not stop after cancellation")?
}
