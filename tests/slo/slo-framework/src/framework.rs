use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::signal::unix::{SignalKind, signal};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::logger::{Logger, Phase};
use crate::metrics::Metrics;

const SHUTDOWN_DURATION: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct Framework {
    pub config: Config,
    pub metrics: Metrics,
    pub logger: Arc<Logger>,
}

#[async_trait::async_trait]
pub trait Workload: Send {
    async fn setup(&self, ctx: &CancellationToken) -> Result<(), String>;
    async fn run(&self, ctx: &CancellationToken) -> Result<(), String>;
    async fn teardown(&self, ctx: &CancellationToken) -> Result<(), String>;
}

pub async fn run<F, Fut>(factory: F)
where
    F: for<'a> Fn(&'a Framework) -> Fut,
    Fut: Future<Output = Result<Box<dyn Workload>, String>> + Send,
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

    let mut exit_code = 0;

    let config = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("create config failed: {err}");
            std::process::exit(1);
        }
    };

    let logger = Arc::new(Logger::new());
    let metrics = match Metrics::new(&config) {
        Ok(m) => m,
        Err(err) => {
            eprintln!("create metrics failed: {err}");
            std::process::exit(1);
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
            logger.errorf(format!("create workload failed: {err}"));
            metrics.push().await;
            metrics.close().await;
            logger.flush();
            std::process::exit(1);
        }
    };

    let teardown_result = async {
        logger.set_phase(Phase::Teardown);
        timeout(SHUTDOWN_DURATION, workload.teardown(&cancel))
            .await
            .unwrap_or_else(|_| Err("teardown timed out".to_string()))
    };

    let run_result: Result<(), String> = async {
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
                let _ = run_fut.await;
            }
            _ = cancel.cancelled() => {
                run_cancel.cancel();
                let _ = run_fut.await;
            }
        }

        logger.printf("run ok");
        Ok(())
    }
    .await;

    if let Err(err) = run_result {
        logger.errorf(format!("workload failed: {err}"));
        exit_code = 1;
    } else {
        logger.printf("workload completed successfully");
    }

    if let Err(err) = teardown_result.await {
        logger.errorf(format!("teardown failed: {err}"));
    }

    metrics.push().await;
    metrics.close().await;
    logger.flush();
    logger.printf("program finished");
    std::process::exit(exit_code);
}
