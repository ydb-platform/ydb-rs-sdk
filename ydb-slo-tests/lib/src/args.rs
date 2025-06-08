use clap::Args;

#[derive(Args, Clone)]
pub struct CreateArgs {
    /// amount of initially created rows
    #[arg(long = "initial-data-count", short = 'c', default_value_t = 1000)]
    pub initial_data_count: u64,

    /// minimum amount of partitions in table
    #[arg(long = "min-partitions-count", default_value_t = 6)]
    pub min_partitions_count: u64,

    /// maximum amount of partitions in table
    #[arg(long = "max-partitions-count", default_value_t = 1000)]
    pub max_partitions_count: u64,

    /// partition size in mb
    #[arg(long = "partition-size", default_value_t = 1)]
    pub partition_size_mb: u64,
}

#[derive(Args, Clone)]
pub struct RunArgs {
    /// amount of initially created rows
    #[arg(long = "initial-data-count", short = 'c', default_value_t = 1000)]
    pub initial_data_count: u64,

    /// read RPS
    #[arg(long = "read-rps", default_value_t = 1000)]
    pub read_rps: u32,

    /// write RPS
    #[arg(long = "write-rps", default_value_t = 100)]
    pub write_rps: u32,

    /// prometheus push gateway
    #[arg(long = "prom-pgw", default_value_t = String::from(""))]
    pub prom_pgw: String,

    /// read timeout in seconds
    #[arg(long = "read-timeout", default_value_t = 10)]
    pub read_timeout_seconds: u64,

    /// run time in seconds
    #[arg(long = "time", default_value_t = 600)]
    pub time_seconds: u64,

    /// prometheus push period in seconds
    #[arg(long = "report-period", default_value_t = 1)]
    pub report_period_seconds: u64,

    /// time to wait before force kill workers in seconds
    #[arg(long = "shutdown-time", default_value_t = 30)]
    pub shutdown_time_seconds: u64,
}
