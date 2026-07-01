#[async_trait::async_trait]
pub trait TopicService: Send + Sync {
    async fn create_topic(&self) -> Result<(), String>;
    async fn drop_topic(&self) -> Result<(), String>;
    async fn open_writers(&self) -> Result<Vec<ydb::TopicWriter>, String>;
    async fn open_readers(&self) -> Result<Vec<ydb::TopicReader>, String>;
    async fn close(&self) -> Result<(), String>;
}
