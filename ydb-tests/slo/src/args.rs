use clap::Args;

#[derive(Args)]
pub struct CreateArgs {
    /// minimum amount of partitions in table
    #[arg(long = "min-partitions-count", default_value_t = 6)]
    pub min_partitions_count: i64,

    /// maximum amount of partitions in table
    #[arg(long = "max-partitions-count", default_value_t = 1000)]
    pub max_partitions_count: i64,

    /// partition size in mb
    #[arg(long = "partition-size", default_value_t = 1)]
    pub partition_size: i64,

    /// amount of initially created rows
    #[arg(long = "initial-data-count", short = 'c', default_value_t = 1000)]
    pub initial_data_count: i64,
}

#[derive(Args)]
pub struct RunArgs {
    /// amount of initially created rows
    #[arg(long = "initial-data-count", short = 'c', default_value_t = 1000)]
    pub initial_data_count: i64,

    /// prometheus push gateway
    #[arg(long = "prom-pgw", default_value_t = "")]
    pub prom_pgw: String,

    /// prometheus push period in milliseconds
    #[arg(long = "report-period", default_value_t = 250)]
    pub report_period: i64,

    /// read RPS
    #[arg(long = "read-rps", default_value_t = 1000)]
    pub read_rps: i64,

    /// read timeout milliseconds
    #[arg(long = "read-timeout", default_value_t = 10000)]
    pub read_timeout: i64,

    /// write RPS
    #[arg(long = "write-rps", default_value_t = 100)]
    pub write_rps: i64,

    /// run time in seconds
    #[arg(long, default_value_t = 600)]
    pub time: i64,

    /// graceful shutdown time in seconds
    #[arg(long = "shutdown-time", default_value_t = 30)]
    pub shutdown_time: i64,
}
