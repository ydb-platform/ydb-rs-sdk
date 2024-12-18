use clap::Args;

#[derive(Args, Clone)]
pub struct CreateArgs {
    /// minimum amount of partitions in table
    #[arg(long = "min-partitions-count", default_value_t = 6)]
    pub min_partitions_count: u64,

    /// maximum amount of partitions in table
    #[arg(long = "max-partitions-count", default_value_t = 1000)]
    pub max_partitions_count: u64,

    /// partition size in mb
    #[arg(long = "partition-size", default_value_t = 1)]
    pub partition_size: u64,

    /// amount of initially created rows
    #[arg(long = "initial-data-count", short = 'c', default_value_t = 1000)]
    pub initial_data_count: u64,

    /// write timeout milliseconds
    #[arg(long = "write-timeout", default_value_t = 10000)]
    pub write_timeout: u64,
}

#[derive(Args, Clone)]
pub struct RunArgs {
    /// amount of initially created rows
    #[arg(long = "initial-data-count", short = 'c', default_value_t = 1000)]
    pub initial_data_count: u64,

    /// read RPS
    #[arg(long = "read-rps", default_value_t = 1000)]
    pub read_rps: u64,

    /// read timeout milliseconds
    #[arg(long = "read-timeout", default_value_t = 10000)]
    pub read_timeout: u64,

    /// write RPS
    #[arg(long = "write-rps", default_value_t = 100)]
    pub write_rps: u64,

    /// write timeout milliseconds
    #[arg(long = "write-timeout", default_value_t = 10000)]
    pub write_timeout: u64,

    /// run time in seconds
    #[arg(long, default_value_t = 600)]
    pub time: u64,
}
