use super::commands::TopicServerCommand;
use super::events::TopicClientEvent;
use futures_util::{stream, Stream, StreamExt};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Server;
use ydb::{ClientBuilder, TopicReader, YdbResult};
use ydb_grpc::ydb_proto::topic::v1::topic_service_server::{TopicService, TopicServiceServer};
use ydb_grpc::ydb_proto::topic::{self, stream_read_message, stream_write_message};

type ReadStream = Pin<
    Box<dyn Stream<Item = Result<stream_read_message::FromServer, tonic::Status>> + Send + 'static>,
>;

type WriteStream = Pin<
    Box<
        dyn Stream<Item = Result<stream_write_message::FromServer, tonic::Status>> + Send + 'static,
    >,
>;

pub struct MockServer {
    endpoint: String,
    addr: SocketAddr,
    events_rx: mpsc::UnboundedReceiver<TopicClientEvent>,
    active_stream: Arc<Mutex<Option<mpsc::UnboundedSender<TopicServerCommand>>>>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

pub async fn create_mock_topic_reader(
    database: impl AsRef<str>,
    topic_path: impl Into<String>,
    consumer: impl Into<String>,
) -> YdbResult<(TopicReader, MockServer)> {
    let database = database.as_ref();
    let topic_path = topic_path.into();
    let consumer = consumer.into();

    let server = MockServer::start().await;

    let client = ClientBuilder::new_from_connection_string(format!(
        "{}?database={database}&use_discovery=false",
        server.endpoint()
    ))?
    .client()?;

    let reader = client
        .topic_client()
        .create_reader(consumer, topic_path)
        .await?;

    Ok((reader, server))
}

impl MockServer {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock server failed to bind tcp listener");
        let addr = listener
            .local_addr()
            .expect("mock server failed to read local address");
        Self::start_with_listener(listener, addr).await
    }

    async fn start_on_addr(addr: SocketAddr) -> Self {
        let listener = TcpListener::bind(addr)
            .await
            .expect("mock server failed to re-bind tcp listener");
        Self::start_with_listener(listener, addr).await
    }

    async fn start_with_listener(listener: TcpListener, addr: SocketAddr) -> Self {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let active_stream = Arc::new(Mutex::new(None));

        let service = MockTopicService {
            events_tx,
            active_stream: active_stream.clone(),
            next_stream_id: Arc::new(AtomicU64::new(1)),
        };

        let incoming = stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });

        let task = tokio::spawn(async move {
            let result = Server::builder()
                .add_service(TopicServiceServer::new(service))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await;

            if let Err(err) = result {
                panic!("mock server failed: {err}");
            }
        });

        Self {
            endpoint: endpoint(addr),
            addr,
            events_rx,
            active_stream,
            shutdown: Some(shutdown_tx),
            task: Some(task),
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub async fn shutdown(mut self) {
        self.shutdown_inner();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }

    pub async fn restart(&mut self) {
        self.shutdown_inner();
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }

        *self = Self::start_on_addr(self.addr).await;
    }

    fn shutdown_inner(&mut self) {
        self.try_close_stream();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

macro_rules! expect_client_message_methods {
    (
        $(
            fn $method:ident() -> $ty:ty = $variant:ident;
        )+
    ) => {
        $(
            pub async fn $method(&mut self) -> $ty {
                let (_, message) = self.expect_stream_read_message().await;
                match message.client_message {
                    Some(stream_read_message::from_client::ClientMessage::$variant(value)) => {
                        value
                    }
                    other => panic!(
                        "expected {}, got {other:?}",
                        stringify!($variant),
                    ),
                }
            }
        )+
    };
}

impl MockServer {
    pub async fn next_event(&mut self) -> TopicClientEvent {
        self.next_event_timeout(Duration::from_secs(5)).await
    }

    pub async fn next_event_timeout(&mut self, timeout: Duration) -> TopicClientEvent {
        tokio::time::timeout(timeout, async {
            self.events_rx
                .recv()
                .await
                .expect("mock topic event channel closed")
        })
        .await
        .expect("timed out waiting for mock topic event")
    }

    pub async fn expect_stream_read_opened(&mut self) -> u64 {
        match self.next_event().await {
            TopicClientEvent::Opened { stream_id } => stream_id,
            event => panic!("expected TopicClientEvent::Opened, got {event:?}"),
        }
    }

    pub async fn expect_stream_read_message(&mut self) -> (u64, stream_read_message::FromClient) {
        match self.next_event().await {
            TopicClientEvent::Message { stream_id, message } => (stream_id, message),
            event => panic!("expected TopicClientEvent::Message, got {event:?}"),
        }
    }

    expect_client_message_methods! {
        fn expect_init_request() -> stream_read_message::InitRequest = InitRequest;
        fn expect_read_request() -> stream_read_message::ReadRequest = ReadRequest;
        fn expect_start_partition_session_response() -> stream_read_message::StartPartitionSessionResponse = StartPartitionSessionResponse;
        fn expect_stop_partition_session_response() -> stream_read_message::StopPartitionSessionResponse = StopPartitionSessionResponse;
        fn expect_commit_offset_request() -> stream_read_message::CommitOffsetRequest = CommitOffsetRequest;
    }

    pub fn send(&self, message: stream_read_message::FromServer) {
        self.command(TopicServerCommand::SendReadStreamMessage(message));
    }

    pub fn close_stream(&self) {
        self.command(TopicServerCommand::CloseReadStream);
    }

    pub fn try_close_stream(&self) {
        if let Some(sender) = self
            .active_stream
            .lock()
            .expect("mock topic active stream mutex poisoned")
            .clone()
        {
            let _ = sender.send(TopicServerCommand::CloseReadStream);
        }
    }

    pub fn fail_stream(&self, status: tonic::Status) {
        self.command(TopicServerCommand::FailReadStream(status));
    }

    fn command(&self, command: TopicServerCommand) {
        let sender = self
            .active_stream
            .lock()
            .expect("mock topic active stream mutex poisoned")
            .clone()
            .expect("mock topic stream is not active");

        sender
            .send(command)
            .expect("mock topic command channel closed");
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.shutdown_inner();
    }
}

struct MockTopicService {
    events_tx: mpsc::UnboundedSender<TopicClientEvent>,
    active_stream: Arc<Mutex<Option<mpsc::UnboundedSender<TopicServerCommand>>>>,
    next_stream_id: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl TopicService for MockTopicService {
    type StreamWriteStream = WriteStream;
    type StreamReadStream = ReadStream;

    async fn stream_write(
        &self,
        _request: tonic::Request<tonic::Streaming<stream_write_message::FromClient>>,
    ) -> Result<tonic::Response<Self::StreamWriteStream>, tonic::Status> {
        unimplemented!()
    }

    async fn stream_read(
        &self,
        request: tonic::Request<tonic::Streaming<stream_read_message::FromClient>>,
    ) -> Result<tonic::Response<Self::StreamReadStream>, tonic::Status> {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();

        {
            let mut active_stream = self
                .active_stream
                .lock()
                .expect("mock topic active stream mutex poisoned");
            *active_stream = Some(commands_tx);
        }

        self.emit(TopicClientEvent::Opened { stream_id });
        self.spawn_client_reader(stream_id, request.into_inner());

        let responses =
            UnboundedReceiverStream::new(commands_rx).map(move |command| match command {
                TopicServerCommand::SendReadStreamMessage(message) => Some(Ok(message)),
                TopicServerCommand::CloseReadStream => None,
                TopicServerCommand::FailReadStream(status) => Some(Err(status)),
            });

        let responses = stream::unfold(responses, |mut responses| async move {
            match responses.next().await {
                Some(Some(response)) => Some((response, responses)),
                Some(None) | None => None,
            }
        });

        Ok(tonic::Response::new(Box::pin(responses)))
    }

    async fn commit_offset(
        &self,
        _request: tonic::Request<topic::CommitOffsetRequest>,
    ) -> Result<tonic::Response<topic::CommitOffsetResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn update_offsets_in_transaction(
        &self,
        _request: tonic::Request<topic::UpdateOffsetsInTransactionRequest>,
    ) -> Result<tonic::Response<topic::UpdateOffsetsInTransactionResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn create_topic(
        &self,
        _request: tonic::Request<topic::CreateTopicRequest>,
    ) -> Result<tonic::Response<topic::CreateTopicResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn describe_topic(
        &self,
        _request: tonic::Request<topic::DescribeTopicRequest>,
    ) -> Result<tonic::Response<topic::DescribeTopicResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn describe_consumer(
        &self,
        _request: tonic::Request<topic::DescribeConsumerRequest>,
    ) -> Result<tonic::Response<topic::DescribeConsumerResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn alter_topic(
        &self,
        _request: tonic::Request<topic::AlterTopicRequest>,
    ) -> Result<tonic::Response<topic::AlterTopicResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn drop_topic(
        &self,
        _request: tonic::Request<topic::DropTopicRequest>,
    ) -> Result<tonic::Response<topic::DropTopicResponse>, tonic::Status> {
        unimplemented!()
    }
}

impl MockTopicService {
    fn emit(&self, event: TopicClientEvent) {
        let _ = self.events_tx.send(event);
    }

    fn spawn_client_reader(
        &self,
        stream_id: u64,
        mut request: tonic::Streaming<stream_read_message::FromClient>,
    ) {
        let events_tx = self.events_tx.clone();

        tokio::spawn(async move {
            loop {
                match request.message().await {
                    Ok(Some(message)) => {
                        let _ = events_tx.send(TopicClientEvent::Message { stream_id, message });
                    }
                    Ok(None) => {
                        let _ = events_tx.send(TopicClientEvent::Closed { stream_id });
                        break;
                    }
                    Err(status) => {
                        let _ = events_tx.send(TopicClientEvent::Error { stream_id, status });
                        break;
                    }
                }
            }
        });
    }
}

fn endpoint(addr: SocketAddr) -> String {
    format!("grpc://{}:{}", addr.ip(), addr.port())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ydb::YdbResult;

    const TOPIC_PATH: &str = "/local/mock-topic";
    const CONSUMER: &str = "mock-consumer";
    const DATABASE: &str = "/local";

    #[tokio::test]
    async fn server_receives_topic_reader_init_request() -> YdbResult<()> {
        let (_reader, mut server) =
            create_mock_topic_reader(DATABASE, TOPIC_PATH, CONSUMER).await?;

        let stream_id = server.expect_stream_read_opened().await;
        assert_eq!(stream_id, 1);

        let init = server.expect_init_request().await;
        assert_eq!(init.consumer, CONSUMER);
        assert_eq!(init.topics_read_settings[0].path, TOPIC_PATH);

        Ok(())
    }
}
