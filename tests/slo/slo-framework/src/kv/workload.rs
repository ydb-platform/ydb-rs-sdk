use std::sync::{Arc, OnceLock};
use std::time::{Duration, UNIX_EPOCH};

use anyhow::{Context, Error, Result, bail};
use rand::Rng;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::framework::Workload;
use crate::generator::Generator;
use crate::helpers::{new_rate_limiter, run_workers};
use crate::metrics::{OPERATION_READ, OPERATION_WRITE};
use crate::{Framework, TestRow};

use super::{Database, Params};

pub struct KvWorkload<D: Database> {
    fw: Arc<Framework>,
    db: Arc<D>,
    params: Params,
    prefill: OnceLock<Arc<Vec<TestRow>>>,
}

impl<D: Database + 'static> KvWorkload<D> {
    pub fn new(fw: Framework, params: Params, db: D) -> Self {
        Self {
            fw: Arc::new(fw),
            db: Arc::new(db),
            params,
            prefill: OnceLock::new(),
        }
    }
}

#[async_trait::async_trait]
impl<D: Database + 'static> Workload for KvWorkload<D> {
    async fn setup(&self, ctx: &CancellationToken) -> Result<()> {
        anyhow::ensure!(
            self.params.prefill_count > 0,
            "KV correctness requires prefill rows"
        );
        self.db
            .create_table()
            .await
            .map_err(Error::msg)
            .context("create table")?;
        self.fw.logger.printf("create table ok");

        let r#gen = Generator::new(0);
        let mut tasks = JoinSet::new();
        for _ in 0..self.params.prefill_count {
            if ctx.is_cancelled() {
                bail!("setup cancelled");
            }
            let db = self.db.clone();
            let mut row = r#gen.generate();
            // Immutable generation one: run writers never touch prefill IDs.
            row.payload_str = format!("kv:1:{}", row.payload_str);
            let micros = row
                .payload_timestamp
                .duration_since(UNIX_EPOCH)?
                .as_micros();
            row.payload_timestamp = UNIX_EPOCH + Duration::from_micros(u64::try_from(micros)?);
            tasks.spawn(async move {
                db.write(row.clone()).await?;
                let (actual, _) = db.read(row.id).await?;
                verify_row(&row, &actual)?;
                Ok::<_, String>(row)
            });
        }

        let mut prefill = Vec::new();
        while let Some(res) = tasks.join_next().await {
            prefill.push(res?.map_err(Error::msg).context("verify prefill entry")?);
        }
        self.prefill
            .set(Arc::new(prefill))
            .map_err(|_| Error::msg("prefill already published"))?;

        self.fw.logger.printf("entries write ok");
        Ok(())
    }

    async fn run(&self, ctx: &CancellationToken) -> Result<()> {
        let prefill = self.prefill.get().context("prefill not published")?.clone();
        let run_gen = Generator::new(self.params.prefill_count);
        let read_limiter = new_rate_limiter(self.params.read_rps);
        let write_limiter = new_rate_limiter(self.params.write_rps);
        let read_workers = self.params.read_rps as usize;
        let write_workers = self.params.write_rps as usize;
        let worker_ctx = ctx.child_token();
        let (error_tx, mut error_rx) = tokio::sync::mpsc::unbounded_channel();

        let read_handle = {
            let ctx = worker_ctx.clone();
            let worker_ctx = worker_ctx.clone();
            let fw = self.fw.clone();
            let db = self.db.clone();
            let read_timeout = self.params.read_timeout;
            let error_tx = error_tx.clone();

            tokio::spawn(async move {
                run_workers(&ctx, read_workers, read_limiter, move || {
                    let worker_ctx = worker_ctx.clone();
                    let db = db.clone();
                    let prefill = prefill.clone();
                    let metrics = fw.metrics.clone();
                    let logger = fw.logger.clone();
                    let error_tx = error_tx.clone();
                    async move {
                        if worker_ctx.is_cancelled() {
                            return;
                        }
                        let expected = &prefill[rand::thread_rng().gen_range(0..prefill.len())];
                        let span = metrics.start(OPERATION_READ);
                        let result = tokio::time::timeout(read_timeout, db.read(expected.id)).await;
                        let result = result.map(|result| {
                            result.and_then(|(row, attempts)| {
                                verify_row(expected, &row).map(|()| (row, attempts))
                            })
                        });
                        match result {
                            Ok(Ok((_, attempts))) => span.finish(None, attempts),
                            Ok(Err(err)) => {
                                span.finish(Some(&err), 1);
                                if !worker_ctx.is_cancelled() {
                                    logger.errorf(format!("read failed: {err}"));
                                }
                                let _ = error_tx.send(Error::msg(err).context("read failed"));
                                worker_ctx.cancel();
                            }
                            Err(_) => {
                                span.finish(Some("read timeout"), 1);
                                if !worker_ctx.is_cancelled() {
                                    logger.errorf("read failed: timeout");
                                }
                                let _ = error_tx.send(Error::msg("read timeout"));
                                worker_ctx.cancel();
                            }
                        }
                    }
                })
                .await
            })
        };

        let write_handle = {
            let ctx = worker_ctx.clone();
            let worker_ctx = worker_ctx.clone();
            let fw = self.fw.clone();
            let db = self.db.clone();
            let write_timeout = self.params.write_timeout;
            let error_tx = error_tx.clone();

            tokio::spawn(async move {
                run_workers(&ctx, write_workers, write_limiter, move || {
                    let worker_ctx = worker_ctx.clone();
                    let db = db.clone();
                    let r#gen = run_gen.clone();
                    let metrics = fw.metrics.clone();
                    let logger = fw.logger.clone();
                    let error_tx = error_tx.clone();
                    async move {
                        if worker_ctx.is_cancelled() {
                            return;
                        }
                        let row = r#gen.generate();
                        let span = metrics.start(OPERATION_WRITE);
                        let result = tokio::time::timeout(write_timeout, db.write(row)).await;
                        match result {
                            Ok(Ok(attempts)) => span.finish(None, attempts),
                            Ok(Err(err)) => {
                                span.finish(Some(&err), 1);
                                if !worker_ctx.is_cancelled() {
                                    logger.errorf(format!("write failed: {err}"));
                                }
                                let _ = error_tx.send(Error::msg(err).context("write failed"));
                                worker_ctx.cancel();
                            }
                            Err(_) => {
                                span.finish(Some("write timeout"), 1);
                                if !worker_ctx.is_cancelled() {
                                    logger.errorf("write failed: timeout");
                                }
                                let _ = error_tx.send(Error::msg("write timeout"));
                                worker_ctx.cancel();
                            }
                        }
                    }
                })
                .await
            })
        };

        let (read_result, write_result) = tokio::join!(read_handle, write_handle);
        if let Ok(error) = error_rx.try_recv() {
            return Err(error);
        }
        read_result.context("read worker supervisor failed")??;
        write_result.context("write worker supervisor failed")??;
        Ok(())
    }

    async fn teardown(&self, _ctx: &CancellationToken) -> Result<()> {
        let result = self.db.drop_table().await;
        let _ = self.db.close().await;
        result.map_err(Error::msg).context("drop table")?;
        self.fw.logger.printf("cleanup table ok");
        Ok(())
    }
}

fn verify_row(expected: &TestRow, actual: &TestRow) -> std::result::Result<(), String> {
    if actual.id != expected.id
        || actual.payload_str != expected.payload_str
        || actual.payload_double.to_bits() != expected.payload_double.to_bits()
        || actual.payload_timestamp != expected.payload_timestamp
    {
        return Err(format!(
            "KV row mismatch: expected {expected:?}, actual {actual:?}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn immutable_generation_and_all_payload_fields_are_checked() {
        let expected = TestRow::new(7, "kv:1:payload".into(), 0.5, UNIX_EPOCH);
        assert!(verify_row(&expected, &expected).is_ok());
        let mutations: [fn(&mut TestRow); 5] = [
            |row| row.id = 8,
            |row| row.payload_str = "kv:0:payload".into(),
            |row| row.payload_str = "kv:1:corrupt".into(),
            |row| row.payload_double = 0.25,
            |row| row.payload_timestamp += Duration::from_micros(1),
        ];
        for mutate in mutations {
            let mut actual = expected.clone();
            mutate(&mut actual);
            assert!(verify_row(&expected, &actual).is_err());
        }
    }
}
