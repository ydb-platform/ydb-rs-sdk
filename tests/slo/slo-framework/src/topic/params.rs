use std::path::Path;
use std::time::Duration;

use clap::Parser;

use crate::Framework;

#[derive(Debug, Clone, Parser)]
#[command(disable_help_flag = true, disable_version_flag = true)]
pub struct QueueFlags {
    #[arg(long, default_value_t = 1_000)]
    pub write_rps: u32,
    #[arg(long, default_value_t = 5_000)]
    pub delivery_timeout: u64,
    #[arg(long, default_value_t = 5_000)]
    pub commit_timeout: u64,
    #[arg(long, default_value_t = 5_000)]
    pub write_timeout: u64,
    #[arg(long, default_value_t = 100)]
    pub commit_delay: u64,
    #[arg(long, default_value_t = 10)]
    pub partition_count: u32,
    #[arg(long, default_value = "slo-consumer")]
    pub consumer_name: String,
    #[arg(long, default_value_t = 5)]
    pub reader_count: u32,
    #[arg(long, default_value = "producer")]
    pub producer_id_prefix: String,
    #[arg(long, default_value_t = 20)]
    pub writer_count: u32,
}

#[derive(Debug, Clone)]
pub struct Params {
    pub write_rps: u32,
    pub delivery_timeout: Duration,
    pub commit_timeout: Duration,
    pub write_timeout: Duration,
    pub commit_delay: Duration,
    pub partition_count: u32,
    pub consumer_name: String,
    pub reader_count: u32,
    pub producer_id_prefix: String,
    pub writer_count: u32,
    pub topic_path: String,
}

pub fn parse_params(fw: &Framework) -> Params {
    let flags = QueueFlags::parse();

    let topic_path = Path::new(&fw.config.database)
        .join(&fw.config.label)
        .join(&fw.config.ref_name)
        .to_string_lossy()
        .into_owned();

    Params {
        write_rps: flags.write_rps,

        delivery_timeout: Duration::from_millis(flags.delivery_timeout),
        commit_timeout: Duration::from_millis(flags.commit_timeout),
        write_timeout: Duration::from_millis(flags.write_timeout),
        commit_delay: Duration::from_millis(flags.commit_delay),

        partition_count: flags.partition_count,
        consumer_name: flags.consumer_name,
        reader_count: flags.reader_count,
        producer_id_prefix: flags.producer_id_prefix,
        writer_count: flags.writer_count,

        topic_path,
    }
}
