use std::path::Path;

use clap::Parser;

use crate::Framework;

#[derive(Debug, Clone, Parser)]
#[command(disable_help_flag = true, disable_version_flag = true)]
pub struct KvFlags {
    #[arg(long, default_value_t = 1000)]
    pub read_rps: u32,
    #[arg(long, default_value_t = 100)]
    pub write_rps: u32,
    #[arg(long, default_value_t = 10000)]
    pub read_timeout: u64,
    #[arg(long, default_value_t = 10000)]
    pub write_timeout: u64,
    #[arg(long, default_value_t = 1000)]
    pub prefill_count: u64,
    #[arg(long, default_value_t = 1)]
    pub partition_size: u64,
    #[arg(long, default_value_t = 6)]
    pub min_partition_count: u64,
    #[arg(long, default_value_t = 1000)]
    pub max_partition_count: u64,
}

#[derive(Debug, Clone)]
pub struct Params {
    pub read_rps: u32,
    pub write_rps: u32,
    pub read_timeout: std::time::Duration,
    pub write_timeout: std::time::Duration,
    pub prefill_count: u64,
    pub table_path: String,
    pub partition_size: u64,
    pub min_partition_count: u64,
    pub max_partition_count: u64,
}

impl Params {
    pub fn pool_size(&self) -> u32 {
        self.read_rps + self.write_rps
    }
}

pub fn parse_params(fw: &Framework) -> Params {
    let flags = KvFlags::parse_from(std::env::args().skip(1).collect::<Vec<_>>());

    let table_path = Path::new(&fw.config.database)
        .join(&fw.config.label)
        .join(&fw.config.ref_name)
        .to_string_lossy()
        .into_owned();

    Params {
        read_rps: flags.read_rps,
        write_rps: flags.write_rps,
        read_timeout: std::time::Duration::from_millis(flags.read_timeout),
        write_timeout: std::time::Duration::from_millis(flags.write_timeout),
        prefill_count: flags.prefill_count,
        table_path,
        partition_size: flags.partition_size,
        min_partition_count: flags.min_partition_count,
        max_partition_count: flags.max_partition_count,
    }
}
