extern crate ydb_slo_tests;

use crate::db::Database;
use clap::Parser;
use ratelimit::Ratelimiter;
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

    let database = Database::new(cli)
        .await
        .unwrap_or_else(|err| panic!("Failed to initialize YDB client: {}", err));

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
            let generator = Arc::new(Mutex::new(Generator::new(
                run_args.initial_data_count as RowID,
            )));

            let workers = Workers::new(Arc::new(database), run_args.clone());
            let tracker = TaskTracker::new();
            let token = CancellationToken::new();

            let read_rate_limiter = Arc::new(
                Ratelimiter::builder(run_args.read_rps, Duration::from_secs(1))
                    .max_tokens(run_args.read_rps)
                    .build()?,
            );

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

            let write_rate_limiter = Arc::new(
                Ratelimiter::builder(run_args.write_rps, Duration::from_secs(1))
                    .max_tokens(run_args.write_rps)
                    .build()?,
            );

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

            {
                let tracker = tracker.clone();
                tokio::spawn(async move {
                    time::sleep(Duration::from_secs(run_args.time)).await;
                    tracker.close();
                    token.cancel();
                });
            }

            tracker.wait().await;

            println!("All workers are completed");
        }
    }

    println!("Program is finished");
    Ok(())
}
