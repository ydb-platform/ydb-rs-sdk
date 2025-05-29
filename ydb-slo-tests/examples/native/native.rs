extern crate ydb_slo_tests;

use crate::db::Database;
use clap::Parser;
use governor::{Quota, RateLimiter};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use ydb_slo_tests::cli::{Command, SloTestsCli};
use ydb_slo_tests::generator::Generator;
use ydb_slo_tests::row::RowID;
use ydb_slo_tests::workers::{ReadWriter, Workers};

mod db;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = SloTestsCli::parse();
    let command = cli.command.clone();
    println!("Program is started");

    let database = Database::new(cli)
        .await
        .unwrap_or_else(|err| panic!("Failed to initialize YDB client: {}", err));
    println!("Initialized database");

    match command {
        Command::Create(create_args) => {
            database
                .create_table(&create_args)
                .await
                .unwrap_or_else(|err| panic!("Failed to create table: {}", err));

            println!("Created table");

            let tracker = TaskTracker::new();
            let database = Arc::new(database.clone());
            let generator = Arc::new(Mutex::new(Generator::new(0)));

            for _ in 0..create_args.initial_data_count {
                let database = Arc::clone(&database);
                let generator = Arc::clone(&generator);

                tracker.spawn(async move {
                    let database = &database;
                    let row = generator.lock().await.generate();

                    timeout(
                        Duration::from_millis(create_args.write_timeout),
                        database.write(row),
                    )
                    .await
                    .unwrap()
                });
            }

            tracker.close();
            tracker.wait().await;

            println!("Inserted {} rows", create_args.initial_data_count);
        }
        Command::Cleanup => {
            database
                .drop_table()
                .await
                .unwrap_or_else(|err| panic!("Failed to clean up table: {}", err));

            println!("Cleaned up table");
        }
        Command::Run(run_args) => {
            let metrics_ref = std::env::var("METRICS_REF").unwrap_or("metrics_ref".to_string());
            let metrics_label =
                std::env::var("METRICS_LABEL").unwrap_or("metrics_label".to_string());
            let metrics_job_name =
                std::env::var("METRICS_JOB_NAME").unwrap_or("metrics-test-job".to_string());

            let generator = Arc::new(Mutex::new(Generator::new(
                run_args.initial_data_count as RowID,
            )));

            let workers = Workers::new(
                Arc::new(database),
                run_args.clone(),
                metrics_ref,
                metrics_label,
                metrics_job_name,
            );
            let tracker = TaskTracker::new();
            let token = CancellationToken::new();

            let read_rate_limiter = Arc::new(RateLimiter::direct(
                Quota::per_second(NonZeroU32::new(run_args.read_rps).unwrap())
                    .allow_burst(NonZeroU32::new(1).unwrap()),
            ));

            for _ in 0..run_args.read_rps {
                let cloned_token = token.clone();
                let workers = Arc::clone(&workers);
                let read_rate_limiter = Arc::clone(&read_rate_limiter);

                tracker.spawn(async move {
                    let workers = &workers;
                    let read_rate_limiter = &read_rate_limiter;

                    workers
                        .start_read_load(read_rate_limiter, cloned_token.clone())
                        .await
                });
            }
            println!("Started {} read workers", run_args.read_rps);

            let write_rate_limiter = Arc::new(RateLimiter::direct(
                Quota::per_second(NonZeroU32::new(run_args.write_rps).unwrap())
                    .allow_burst(NonZeroU32::new(1).unwrap()),
            ));

            for _ in 0..run_args.write_rps {
                let cloned_token = token.clone();
                let workers = Arc::clone(&workers);
                let write_rate_limiter = Arc::clone(&write_rate_limiter);
                let generator = Arc::clone(&generator);

                tracker.spawn(async move {
                    let workers = &workers;
                    let write_rate_limiter = &write_rate_limiter;
                    let generator = generator.lock().await;

                    workers
                        .start_write_load(write_rate_limiter, &generator, cloned_token.clone())
                        .await
                });
            }
            println!("Started {} write workers", run_args.write_rps);

            let metrics_rate_limiter = Arc::new(RateLimiter::direct(
                Quota::with_period(Duration::from_millis(run_args.report_period))
                    .unwrap()
                    .allow_burst(NonZeroU32::new(1).unwrap()),
            ));

            let metrics_worker = Arc::clone(&workers);
            let metrics_token = token.clone();
            tracker.spawn(async move {
                metrics_worker
                    .collect_metrics(&metrics_rate_limiter, metrics_token)
                    .await
            });

            println!("Started metrics worker");

            {
                let tracker = tracker.clone();
                tokio::spawn(async move {
                    time::sleep(Duration::from_secs(run_args.time)).await;
                    tracker.close();
                    token.cancel();
                });
            }

            tracker.wait().await;

            workers
                .close()
                .await
                .unwrap_or_else(|err| panic!("Failed to close workers: {}", err));

            println!("All workers are completed");
        }
    }

    println!("Program is finished");
    Ok(())
}
