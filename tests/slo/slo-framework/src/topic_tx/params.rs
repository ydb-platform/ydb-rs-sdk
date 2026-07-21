use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Duration;

use clap::Parser;

use crate::Framework;

const CONSUMER_NAME: &str = "slo-topic-tx-consumer";

#[derive(Debug, Clone, Parser)]
#[command(disable_help_flag = true, disable_version_flag = true)]
struct TopicTxFlags {
    #[arg(long, default_value = "16")]
    partition_count: NonZeroUsize,
    #[arg(long, default_value = "16")]
    session_pool_size: NonZeroUsize,
    #[arg(long, default_value_t = 120_000, value_parser = clap::value_parser!(u64).range(1..))]
    operation_timeout: u64,
}

#[derive(Debug, Clone)]
pub struct Params {
    /// Number of independent partition chains; one worker owns each partition.
    pub partition_count: usize,
    /// Warmed session count and hard limit of the query session pool.
    pub session_pool_size: usize,
    /// Wall-clock limit for one chain advance and its supporting YDB operations.
    pub operation_timeout: Duration,
    pub topic_path: String,
    pub table_path: String,
    pub consumer_name: String,
}

pub fn parse_params(framework: &Framework) -> Params {
    let flags = TopicTxFlags::parse();
    let resource_directory = Path::new(&framework.config.database).join(&framework.config.label);
    let resource_name = &framework.config.ref_name;

    Params {
        partition_count: flags.partition_count.get(),
        session_pool_size: flags.session_pool_size.get(),
        operation_timeout: Duration::from_millis(flags.operation_timeout),
        topic_path: resource_path(&resource_directory, resource_name, "topic"),
        table_path: resource_path(&resource_directory, resource_name, "transitions"),
        consumer_name: CONSUMER_NAME.to_string(),
    }
}

fn resource_path(directory: &Path, name: &str, suffix: &str) -> String {
    directory
        .join(format!("{name}-{suffix}"))
        .to_string_lossy()
        .into_owned()
}
