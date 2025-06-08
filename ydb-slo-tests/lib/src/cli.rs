use crate::args;
use clap::{Parser, Subcommand};

#[derive(Clone, Parser)]
pub struct SloTestsCli {
    /// YDB endpoint to connect to
    pub endpoint: String,

    /// YDB database to connect to
    pub db: String,

    /// table name to create
    #[arg(long = "table-name", short = 't', default_value_t = String::from("testingTable"))]
    pub table_name: String,

    /// write timeout in seconds
    #[arg(long = "write-timeout", default_value_t = 10)]
    pub write_timeout_seconds: u64,

    /// YDB database initialization timeout in seconds
    #[arg(long = "db-init-timeout", default_value_t = 3)]
    pub db_init_timeout_seconds: u64,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Clone, Subcommand)]
pub enum Command {
    /// creates table in database
    Create(args::CreateArgs),

    /// drops table in database
    Cleanup,

    /// runs workload (read and write to table with sets RPS)
    Run(args::RunArgs),
}
