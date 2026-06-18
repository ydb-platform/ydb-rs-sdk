use std::sync::Arc;

use rand::Rng;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::framework::Workload;
use crate::generator::Generator;
use crate::helpers::{new_rate_limiter, run_workers, run_workers_unlimited};
use crate::metrics::{OPERATION_READ, OPERATION_WRITE};
use crate::Framework;

use super::{Database, Params};

pub struct KvWorkload<D: Database> {
    fw: Arc<Framework>,
    db: Arc<D>,
    params: Params,
}

impl<D: Database + 'static> KvWorkload<D> {
    pub fn new(fw: Framework, params: Params, db: D) -> Self {
        Self {
            fw: Arc::new(fw),
            db: Arc::new(db),
            params,
        }
    }
}

#[async_trait::async_trait]
impl<D: Database + 'static> Workload for KvWorkload<D> {
    async fn setup(&self, ctx: &CancellationToken) -> Result<(), String> {
        self.db.create_table().await?;
        self.fw.logger.printf("create table ok");

        let gen = Generator::new(0);
        let mut tasks = JoinSet::new();
        for _ in 0..self.params.prefill_count {
            if ctx.is_cancelled() {
                return Err("setup cancelled".to_string());
            }
            let db = self.db.clone();
            let row = gen.generate();
            tasks.spawn(async move { db.write(row).await.map(|_| ()) });
        }

        while let Some(res) = tasks.join_next().await {
            res.map_err(|err| err.to_string())??;
        }

        self.fw.logger.printf("entries write ok");
        Ok(())
    }

    async fn run(&self, ctx: &CancellationToken) -> Result<(), String> {
        let run_gen = Generator::new(self.params.prefill_count);
        let read_workers = self.params.read_rps as usize;
        let write_workers = self.params.write_rps as usize;
        let read_rps = self.params.read_rps;
        let write_rps = self.params.write_rps;
        let no_rate_limit = self.params.no_rate_limit;

        let read_handle = {
            let ctx = ctx.clone();
            let worker_ctx = ctx.clone();
            let fw = self.fw.clone();
            let db = self.db.clone();
            let prefill = self.params.prefill_count;
            let read_timeout = self.params.read_timeout;

            tokio::spawn(async move {
                let read_worker = move || {
                    let worker_ctx = worker_ctx.clone();
                    let db = db.clone();
                    let metrics = fw.metrics.clone();
                    let logger = fw.logger.clone();
                    async move {
                        if worker_ctx.is_cancelled() {
                            return;
                        }
                        let id = rand::thread_rng().gen_range(0..prefill);
                        let span = metrics.start(OPERATION_READ);
                        let result = tokio::time::timeout(read_timeout, db.read(id)).await;
                        match result {
                            Ok(Ok((_, attempts))) => span.finish(None, attempts),
                            Ok(Err(err)) => {
                                span.finish(Some(&err), 1);
                                if !worker_ctx.is_cancelled() {
                                    logger.errorf(format!("read failed: {err}"));
                                }
                            }
                            Err(_) => {
                                span.finish(Some("read timeout"), 1);
                                if !worker_ctx.is_cancelled() {
                                    logger.errorf("read failed: timeout");
                                }
                            }
                        }
                    }
                };

                if no_rate_limit {
                    run_workers_unlimited(&ctx, read_workers, read_worker).await;
                } else {
                    let read_limiter = new_rate_limiter(read_rps);
                    run_workers(&ctx, read_workers, read_limiter, read_worker).await;
                }
            })
        };

        let write_handle = {
            let ctx = ctx.clone();
            let worker_ctx = ctx.clone();
            let fw = self.fw.clone();
            let db = self.db.clone();
            let write_timeout = self.params.write_timeout;

            tokio::spawn(async move {
                let write_worker = move || {
                    let worker_ctx = worker_ctx.clone();
                    let db = db.clone();
                    let gen = run_gen.clone();
                    let metrics = fw.metrics.clone();
                    let logger = fw.logger.clone();
                    async move {
                        if worker_ctx.is_cancelled() {
                            return;
                        }
                        let row = gen.generate();
                        let span = metrics.start(OPERATION_WRITE);
                        let result = tokio::time::timeout(write_timeout, db.write(row)).await;
                        match result {
                            Ok(Ok(attempts)) => span.finish(None, attempts),
                            Ok(Err(err)) => {
                                span.finish(Some(&err), 1);
                                if !worker_ctx.is_cancelled() {
                                    logger.errorf(format!("write failed: {err}"));
                                }
                            }
                            Err(_) => {
                                span.finish(Some("write timeout"), 1);
                                if !worker_ctx.is_cancelled() {
                                    logger.errorf("write failed: timeout");
                                }
                            }
                        }
                    }
                };

                if no_rate_limit {
                    run_workers_unlimited(&ctx, write_workers, write_worker).await;
                } else {
                    let write_limiter = new_rate_limiter(write_rps);
                    run_workers(&ctx, write_workers, write_limiter, write_worker).await;
                }
            })
        };

        let _ = tokio::join!(read_handle, write_handle);
        Ok(())
    }

    async fn teardown(&self, _ctx: &CancellationToken) -> Result<(), String> {
        let result = self.db.drop_table().await;
        let _ = self.db.close().await;
        result?;
        self.fw.logger.printf("cleanup table ok");
        Ok(())
    }
}
