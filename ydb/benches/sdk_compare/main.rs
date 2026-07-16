mod config;
mod metrics;
mod payload;
mod result;
mod topic;

use std::env;
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use config::{Scenario, Workload};
use tokio::runtime::Builder;

fn main() -> Result<()> {
    let scenario_path = scenario_path()?;
    let scenario = Scenario::load(&scenario_path)?;
    let runtime = Builder::new_multi_thread()
        .worker_threads(scenario.execution.worker_threads)
        .enable_all()
        .build()
        .context("failed to build Tokio runtime")?;

    let result = match &scenario.workload {
        Workload::Topic(workload) => runtime.block_on(topic::run(&scenario, workload))?,
    };
    let stdout = io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer_pretty(&mut output, &result)
        .context("failed to serialize result JSON")?;
    writeln!(output).context("failed to finish result JSON")
}

fn scenario_path() -> Result<PathBuf> {
    let mut arguments = env::args_os();
    let executable = arguments.next().unwrap_or_else(|| "sdk_compare".into());
    let Some(path) = arguments.next() else {
        bail!(
            "usage: {} <scenario.json>",
            PathBuf::from(executable).display()
        );
    };
    // Cargo appends `--bench` when it runs a harness-free benchmark target.
    match arguments.next() {
        None => {}
        Some(argument) if argument == "--bench" && arguments.next().is_none() => {}
        Some(_) => bail!("expected exactly one scenario file argument"),
    }
    Ok(path.into())
}
