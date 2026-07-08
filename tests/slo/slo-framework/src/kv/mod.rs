mod params;
mod workload;

pub use params::{Params, parse_params};
pub use workload::KvWorkload;

use async_trait::async_trait;

use crate::RowID;
use crate::row::TestRow;

#[async_trait]
pub trait Database: Send + Sync {
    async fn create_table(&self) -> Result<(), String>;
    async fn drop_table(&self) -> Result<(), String>;
    async fn read(&self, id: RowID) -> Result<(TestRow, u64), String>;
    async fn write(&self, row: TestRow) -> Result<u64, String>;
    async fn close(&self) -> Result<(), String>;
}
