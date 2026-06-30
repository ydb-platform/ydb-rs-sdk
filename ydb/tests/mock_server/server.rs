use super::{
    handler::{
        FromHandlerToService, FromServerToServiceRx, FromServiceToServerRx, Handler, Incoming,
        Reply,
    },
    topic::{
        default::TopicDefaultHandler, handler::TopicTx, sender::WriteStreamSender, MockTopicService,
    },
};
use futures_util::stream;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use ydb_grpc::ydb_proto::topic::v1::topic_service_server::TopicServiceServer;

struct ForwardChannels {
    topic_tx: TopicTx,
}

impl ForwardChannels {
    fn resend(&self, reply: Reply) {
        match reply {
            Reply::Topic(reply) => {
                let _ = self.topic_tx.send(reply);
            }
            Reply::Scheme(_) => unimplemented!(),
        }
    }
}

struct DefaultHandler {
    topic: TopicDefaultHandler,
}

impl DefaultHandler {
    fn with_tx(tx: FromHandlerToService) -> Self {
        Self {
            topic: TopicDefaultHandler::with_tx(tx.clone()),
        }
    }
}

impl Handler for DefaultHandler {
    fn set_channel(&mut self, _tx: FromHandlerToService) {
        unimplemented!()
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        match incoming {
            Incoming::Topic(_) => self.topic.handle(incoming),
            Incoming::Scheme(_) => todo!(),
        }
    }
}

pub struct MockServer {
    endpoint: String,
    addr: SocketAddr,
    shutdown: CancellationToken,
    _tonic_services: tokio::task::JoinHandle<()>,
    write_sender: WriteStreamSender,
}

impl MockServer {
    pub async fn start(handler: impl Handler) -> (Self, FromHandlerToService) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock server failed to bind tcp listener");
        let addr = listener
            .local_addr()
            .expect("mock server failed to read local address");
        Self::start_with_listener(listener, addr, handler).await
    }

    async fn start_with_listener(
        listener: TcpListener,
        addr: SocketAddr,
        mut handler: impl Handler,
    ) -> (Self, FromHandlerToService) {
        let (from_service_to_server_tx, from_service_to_server_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (from_server_to_service_tx, from_server_to_service_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (topic_tx, topic_rx) = tokio::sync::mpsc::unbounded_channel();

        let topic_service = MockTopicService::new(from_service_to_server_tx, topic_rx);
        let write_sender = topic_service.write_sender.clone();

        let tcp_streams = stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });

        let shutdown = CancellationToken::new();
        let shutdown_signal = shutdown.clone();

        let tonic_services = tokio::spawn(async move {
            let result = Server::builder()
                .add_service(TopicServiceServer::new(topic_service))
                .serve_with_incoming_shutdown(tcp_streams, shutdown_signal.cancelled())
                .await;

            if let Err(err) = result {
                panic!("mock server failed: {err}");
            }
        });

        handler.set_channel(from_server_to_service_tx.clone());

        tokio::spawn(Self::dispatch_loop(
            from_service_to_server_rx,
            handler,
            DefaultHandler::with_tx(from_server_to_service_tx.clone()),
        ));

        tokio::spawn(Self::forwarding_loop(
            ForwardChannels { topic_tx },
            from_server_to_service_rx,
        ));

        let server = Self {
            endpoint: endpoint(addr),
            addr,
            shutdown,
            _tonic_services: tonic_services,
            write_sender,
        };

        (server, from_server_to_service_tx)
    }

    async fn dispatch_loop(
        mut rx: FromServiceToServerRx,
        handler: impl Handler,
        default: impl Handler,
    ) {
        while let Some(incoming) = rx.recv().await {
            let Some(incoming) = handler.handle(incoming) else {
                continue;
            };

            let _ = default.handle(incoming);
        }
    }

    async fn forwarding_loop(channels: ForwardChannels, mut rx: FromServerToServiceRx) {
        while let Some(reply) = rx.recv().await {
            channels.resend(reply);
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn write_sender(&self) -> WriteStreamSender {
        self.write_sender.clone()
    }

    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

fn endpoint(addr: SocketAddr) -> String {
    format!("grpc://{}:{}", addr.ip(), addr.port())
}
