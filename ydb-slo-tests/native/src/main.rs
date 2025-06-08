use crate::db::Database;
use clap::Parser;
use governor::{Quota, RateLimiter};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use ydb::{YdbError, YdbResult};
use ydb_slo_tests_lib::cli::{Command, SloTestsCli};
use ydb_slo_tests_lib::db::row::RowID;
use ydb_slo_tests_lib::generator::Generator;
use ydb_slo_tests_lib::workers::{ReadWriter, Workers, WorkersConfig};

mod db;

#[tokio::main]
async fn main() -> YdbResult<()> {
    let cli = SloTestsCli::parse();

    let token = CancellationToken::new();
    let shutdown_token = token.clone();
    let program_token = token.clone();

    let timeout = choose_timeout(&cli.command);

    println!("program is started");
    tokio::spawn(wait_for_shutdown(shutdown_token, timeout));

    let result = program(cli.clone(), program_token).await;

    println!("program is finished");

    result
}

async fn program(cli: SloTestsCli, token: CancellationToken) -> YdbResult<()> {
    let database = tokio::select! {
        _ = token.cancelled() => {
            return Err(YdbError::Custom("failed to initialize YDB client: cancelled or timeout".to_string()))
        }
        res = Database::new(cli.clone()) => {
            res.map_err(|err| YdbError::Custom(format!("failed to initialize YDB client: {}", err)))?
        }
    };

    println!("initialized database");

    match cli.command {
        Command::Create(create_args) => {
            tokio::select! {
                _ = token.cancelled() => {
                    return Err(YdbError::Custom("failed to create table: cancelled or timeout".to_string()))
                },
                res = timeout(
                    Duration::from_secs(cli.write_timeout_seconds),
                    database.create_table(&create_args)
                ) => {
                    match res {
                        Err(elapsed) => {
                            return Err(YdbError::Custom(format!("failed to create table: {}", elapsed)))
                        }
                        Ok(Err(err)) => {
                            return Err(YdbError::Custom(format!("failed to create table: {}", err)))
                        }
                        _ => {
                            println!("created table");
                        }
                    }
                }
            }

            let mut join_set = JoinSet::new();
            let database = Arc::new(database);
            let generator = Arc::new(Mutex::new(Generator::new(0)));

            for _ in 0..create_args.initial_data_count {
                let database = Arc::clone(&database);
                let generator = Arc::clone(&generator);
                let token = token.clone();

                join_set.spawn(async move {
                    let row = generator.lock().await.generate();

                    tokio::select! {
                        _ = token.cancelled() => {
                            Err(YdbError::Custom("failed to create row: cancelled or timeout".to_string()))
                        },
                        res = timeout(
                            Duration::from_secs(cli.write_timeout_seconds),
                            database.write(row)
                        ) => {
                            match res {
                                Err(elapsed) => {
                                    Err(YdbError::Custom(format!("failed to create row: {}", elapsed)))
                                }
                                Ok((Err(err), _)) => {
                                    Err(YdbError::Custom(format!("failed to create row: {}", err)))
                                }
                                _ => {
                                    Ok(())
                                }
                            }
                        }
                    }
                });
            }

            while let Some(join_result) = join_set.join_next().await {
                match join_result {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        return Err(err);
                    }
                    Err(join_err) => {
                        return Err(YdbError::Custom(format!(
                            "failed to create row: {}",
                            join_err
                        )));
                    }
                }
            }

            println!("inserted {} rows", create_args.initial_data_count);
        }
        Command::Cleanup => {
            tokio::select! {
                _ = token.cancelled() => {
                   return Err(YdbError::Custom("failed to clean up table: cancelled or timeout".to_string()))
                }
                res = timeout(
                    Duration::from_secs(cli.write_timeout_seconds),
                    database.drop_table()
                ) => {
                    match res {
                        Err(elapsed) => {
                            return Err(YdbError::Custom(format!("failed to clean up table: {}", elapsed)))

                        }
                        Ok(Err(err)) => {
                            return Err(YdbError::Custom(format!("failed to clean up table: {}", err)))
                        }
                        _ => {
                            println!("cleaned up table");
                        }
                    }
                }
            }
        }
        Command::Run(run_args) => {
            let metrics_ref = std::env::var("METRICS_REF").unwrap_or("metrics_ref".to_string());
            let metrics_label =
                std::env::var("METRICS_LABEL").unwrap_or("metrics_label".to_string());
            let metrics_job_name =
                std::env::var("METRICS_JOB_NAME").unwrap_or("metrics_test_job".to_string());

            let workers_token = token.clone();

            let generator = Arc::new(Mutex::new(Generator::new(
                run_args.initial_data_count as RowID,
            )));

            let workers_config = WorkersConfig {
                initial_data_count: run_args.initial_data_count,
                read_timeout_seconds: run_args.read_timeout_seconds,
                write_timeout_seconds: cli.write_timeout_seconds,
            };

            let workers = Workers::new(
                Arc::new(database),
                workers_config,
                run_args.prom_pgw,
                metrics_ref,
                metrics_label,
                metrics_job_name,
            );

            let tracker = TaskTracker::new();

            let read_rate_limiter = Arc::new(RateLimiter::direct(
                Quota::per_second(NonZeroU32::new(run_args.read_rps).unwrap())
                    .allow_burst(NonZeroU32::new(1).unwrap()),
            ));

            for _ in 0..run_args.read_rps {
                let token = workers_token.clone();
                let workers = Arc::clone(&workers);
                let read_rate_limiter = Arc::clone(&read_rate_limiter);

                tracker.spawn(async move {
                    tokio::select! {
                        _ = token.cancelled() => {}
                        _ = workers.start_read_load(&read_rate_limiter) => {}
                    }
                });
            }

            println!("started {} read workers", run_args.read_rps);

            let write_rate_limiter = Arc::new(RateLimiter::direct(
                Quota::per_second(NonZeroU32::new(run_args.write_rps).unwrap())
                    .allow_burst(NonZeroU32::new(1).unwrap()),
            ));

            for _ in 0..run_args.write_rps {
                let token = workers_token.clone();
                let workers = Arc::clone(&workers);
                let write_rate_limiter = Arc::clone(&write_rate_limiter);
                let generator = Arc::clone(&generator);

                tracker.spawn(async move {
                    let generator = generator.lock().await;

                    tokio::select! {
                        _ = token.cancelled() => {}
                        _ = workers.start_write_load(&write_rate_limiter, &generator) => {}
                    }
                });
            }

            println!("started {} write workers", run_args.write_rps);

            let metrics_rate_limiter = Arc::new(RateLimiter::direct(
                Quota::with_period(Duration::from_secs(run_args.report_period_seconds))
                    .unwrap()
                    .allow_burst(NonZeroU32::new(1).unwrap()),
            ));

            let metrics_worker = Arc::clone(&workers);
            let metrics_token = workers_token.clone();
            tracker.spawn(async move {
                tokio::select! {
                    _ = metrics_token.cancelled() => {}
                    _ = metrics_worker.collect_metrics(&metrics_rate_limiter) => {}
                }
            });

            println!("started metrics worker");

            tracker.close();
            tracker.wait().await;

            match timeout(
                Duration::from_secs(run_args.shutdown_time_seconds),
                workers.close(),
            )
            .await
            {
                Err(elapsed) => {
                    return Err(YdbError::Custom(format!(
                        "failed to close workers: {}",
                        elapsed
                    )))
                }
                Ok(Err(err)) => {
                    return Err(YdbError::Custom(format!(
                        "failed to close workers: {}",
                        err
                    )))
                }
                _ => {
                    println!("all workers are completed")
                }
            }
        }
    }

    Ok(())
}

async fn wait_for_shutdown(token: CancellationToken, timeout_secs: Duration) {
    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    let mut sigquit = signal(SignalKind::quit()).unwrap();

    tokio::select! {
        _ = sigint.recv() => {
            println!("received SIGINT signal");
        }
        _ = sigterm.recv() => {
            println!("received SIGTERM signal");
        }
        _ = sigquit.recv() => {
            println!("received SIGQUIT signal");
        }
        _ = time::sleep(timeout_secs) => {
            println!("timeout of {} seconds reached", timeout_secs.as_secs());
        }
    }

    token.cancel();
}

fn choose_timeout(cmd: &Command) -> Duration {
    match cmd {
        Command::Create(_) | Command::Cleanup => Duration::from_secs(30),
        Command::Run(run_args) => Duration::from_secs(run_args.time_seconds),
    }
}
