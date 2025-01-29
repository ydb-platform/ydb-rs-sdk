use crate::args;
use clap::{Parser, Subcommand};

#[derive(Clone, Parser)]
pub struct SloTestsCli {
    #[command(subcommand)]
    pub command: Command,

    /// YDB endpoint to connect to
    pub endpoint: String,

    /// YDB database to connect to
    pub db: String,

    /// table name to create
    pub table_name: String,
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
