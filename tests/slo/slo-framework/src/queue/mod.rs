mod params;
mod workload;

pub use params::{parse_params, Params, QueueFlags};
pub use workload::QueueWorkload;

use async_trait::async_trait;

use crate::partition::{MessageReader, WriterHandle};

#[async_trait]
pub trait Queue: Send + Sync {
    async fn create_topic(&self) -> Result<(), String>;
    async fn drop_topic(&self) -> Result<(), String>;
    async fn open_writers(&self) -> Result<Vec<WriterHandle>, String>;
    async fn open_readers(&self) -> Result<Vec<Box<dyn MessageReader>>, String>;
    async fn close(&self) -> Result<(), String>;
}
