use clap::Parser;
use slo::cli::{Commands, SloTestsCli};
use slo::workers::Workers;
use std::sync::Arc;
use tokio::sync::Notify;
use slo::generator::{Generator, RowID};

mod storage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let shutdown_signal = Arc::new(Notify::new());
    let shutdown_signal_cloned = shutdown_signal.clone();
    let ctrl_c_signal = tokio::signal::ctrl_c();

    let cli = SloTestsCli::parse();

    let signal_task = tokio::spawn(async move {
        let _ = ctrl_c_signal.await;
        shutdown_signal_cloned.notify_one();
    });

    let client = ydb::ClientBuilder::new_from_connection_string(cli.endpoint)?
        .with_database(cli.db)
        .client()?;

    client.wait().await?;

    let table_client = client.table_client();

    match &cli.command {
        Commands::Create(create_args) => {
            table_client
                .retry_execute_scheme_query(format!(
                    "CREATE TABLE {} (
                        hash Uint64 NOT NULL,
                        id Uint64 NOT NULL,
                        payload_str Utf8,
                        payload_double Double,
                        payload_timestamp Timestamp,
                        payload_hash Uint64,
                        PRIMARY KEY (hash, id)
                    )",
                    cli.table_name
                ))
                .await?;

            println!("Created table");

            let generator = Generator::new(create_args.initial_data_count as RowID);

            let tasks: Vec<_> = (0..create_args.initial_data_count).map(|_| {
                let generator = generator.clone();
                let storage = storage.clone();
                tokio::spawn(async move {
                    let row = generator.generate().await;
                    storage.write(row).await.unwrap();
                })
            }).collect();

            join_all(tasks).await;

            println!("entries write ok");
        }
        Commands::Cleanup => {
            let _ = table_client
                .retry_execute_scheme_query(format!("DROP TABLE {}", cli.table_name))
                .await; // ignore drop error

            println!("Cleaned up table");
        }
        Commands::Run(run_args) => {
            let workers = Workers::new(cli.clone(), storage.clone()).await?;
            let worker_shutdown = shutdown_signal.clone();

            let mut read_tasks = Vec::new();
            let mut write_tasks = Vec::new();

            for _ in 0..run_args.read_rps {
                let read_worker = workers.clone();
                let worker_shutdown = worker_shutdown.clone();
                let task = tokio::spawn(async move {
                    read_worker.read(worker_shutdown).await.unwrap();
                });
                read_tasks.push(task);
            }

            for _ in 0..run_args.write_rps {
                let write_worker = workers.clone();
                let worker_shutdown = worker_shutdown.clone();
                let task = tokio::spawn(async move {
                    write_worker.write(worker_shutdown).await.unwrap();
                });
                write_tasks.push(task);
            }

            // let metrics_task = tokio::spawn(async move {
            //     workers.report_metrics(worker_shutdown).await.unwrap();
            // });

            let _ = tokio::try_join!(
                signal_task,
                join_all(read_tasks),
                join_all(write_tasks),
                // metrics_task
            );

            println!("workers completed");
        }
    }

    println!("program finished");
    Ok(())
}
