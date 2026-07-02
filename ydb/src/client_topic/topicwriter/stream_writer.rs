use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{trace, warn};

use ydb_grpc::ydb_proto::topic::stream_write_message;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;
use ydb_grpc::ydb_proto::topic::stream_write_message::WriteRequest;
use ydb_grpc::ydb_proto::topic::TransactionIdentity;

use crate::client_topic::compression::{CodecRegistry, CompressionWorker, Executor};
use crate::client_topic::list_types::Codec;
use crate::client_topic::topicwriter::message_write_status::WriteAck;
use crate::client_topic::topicwriter::queue::Queue;
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_topic_service::stream_write::RawServerMessage;
use crate::{YdbError, YdbResult};

/// Manages the gRPC stream communications: write loop and receive-messages loop.
/// Reports error via error_tx.
pub(crate) struct StreamWriter {
    tasks: JoinSet<()>,
    cancellation_token: CancellationToken,
}

impl StreamWriter {
    pub(crate) async fn new(
        writer_options: &TopicWriterOptions,
        stream: AsyncGrpcStreamWrapper<
            stream_write_message::FromClient,
            stream_write_message::FromServer,
        >,
        queue: Queue,
        error_tx: oneshot::Sender<YdbError>,
        server_codecs: Vec<Codec>,
        executor: Arc<dyn Executor>,
        tx_identity: Option<TransactionIdentity>,
    ) -> YdbResult<Self> {
        let cancellation_token = CancellationToken::new();

        // Both loops share the same oneshot error channel.
        let shared_error_tx = Arc::new(Mutex::new(Some(error_tx)));

        let mut codec_registry = CodecRegistry::new();
        for enc in &writer_options.extra_encoders {
            codec_registry.register_encoder(enc.clone());
        }

        let worker = CompressionWorker::new(
            writer_options.codec_selector.clone(),
            Arc::new(codec_registry),
            executor,
            server_codecs,
        )?;

        let (batch_tx, batch_rx) = mpsc::unbounded_channel::<Vec<MessageData>>();
        let (compressed_tx, compressed_rx) = mpsc::unbounded_channel::<YdbResult<WriteRequest>>();

        let request_stream = stream.clone_sender();

        let mut tasks = JoinSet::new();

        tasks.spawn(StreamWriter::write_messages_loop(
            cancellation_token.clone(),
            shared_error_tx.clone(),
            queue.clone(),
            writer_options.write_request_messages_chunk_size,
            writer_options.write_request_send_messages_period,
            batch_tx,
        ));

        worker.spawn_into(&mut tasks, batch_rx, compressed_tx);

        tasks.spawn(StreamWriter::grpc_send_loop(
            cancellation_token.clone(),
            shared_error_tx.clone(),
            compressed_rx,
            request_stream,
            tx_identity,
        ));

        tasks.spawn(StreamWriter::receive_messages_loop(
            cancellation_token.clone(),
            shared_error_tx,
            queue,
            stream,
        ));

        Ok(Self {
            tasks,
            cancellation_token,
        })
    }

    async fn write_messages_loop(
        cancellation_token: CancellationToken,
        error_tx: Arc<Mutex<Option<oneshot::Sender<YdbError>>>>,
        queue: Queue,
        chunk_size: usize,
        period: Duration,
        batch_tx: mpsc::UnboundedSender<Vec<MessageData>>,
    ) {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => { return; }
                messages = queue.get_messages_to_send(chunk_size, period) => {
                    if messages.is_empty() {
                        continue;
                    }
                    if batch_tx.send(messages).is_err() {
                        let err = YdbError::custom("compression worker input channel closed");
                        warn!("error sending message in topic writer write_messages_loop: {}", &err);
                        if let Err(send_err) = StreamWriter::loop_iteration_error(cancellation_token, error_tx, err).await {
                            warn!("can't send error from stream writer write_messages_loop: {send_err}");
                        }
                        break;
                    }
                }
            }
        }
    }

    async fn grpc_send_loop(
        cancellation_token: CancellationToken,
        error_tx: Arc<Mutex<Option<oneshot::Sender<YdbError>>>>,
        mut compressed_rx: mpsc::UnboundedReceiver<YdbResult<WriteRequest>>,
        request_stream: mpsc::UnboundedSender<stream_write_message::FromClient>,
        tx_identity: Option<TransactionIdentity>,
    ) {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => { return; }
                next = compressed_rx.recv() => {
                    let Some(chunk_result) = next else { return; };
                    let result = chunk_result.and_then(|mut write_request| {
                        write_request.tx = tx_identity.clone();
                        if write_request.messages.is_empty() {
                            return Ok(());
                        }
                        trace!("sending topic message to grpc stream");
                        request_stream
                            .send(stream_write_message::FromClient {
                                client_message: Some(ClientMessage::WriteRequest(write_request)),
                            })
                            .map_err(|err| YdbError::Transport(err.to_string()))
                    });

                    let Err(err) = result else { continue; };

                    warn!("error sending message in topic writer grpc_send_loop: {}", &err);
                    if let Err(send_err) = StreamWriter::loop_iteration_error(cancellation_token, error_tx, err).await {
                        warn!("can't send error from stream writer grpc_send_loop: {send_err}");
                    }
                    break;
                }
            }
        }
    }

    async fn receive_messages_loop(
        cancellation_token: CancellationToken,
        error_tx: Arc<Mutex<Option<oneshot::Sender<YdbError>>>>,
        queue: Queue,
        mut stream: AsyncGrpcStreamWrapper<
            stream_write_message::FromClient,
            stream_write_message::FromServer,
        >,
    ) {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => { return; }
                result = StreamWriter::receive_messages_loop_iteration(
                    &queue,
                    &mut stream,
                ) => {
                    let Err(receive_messages_iteration_error) = result else {
                        continue;
                    };

                    warn!(
                        "error receiving message in topic writer receiver stream loop: {}",
                        &receive_messages_iteration_error
                    );

                    if let Err(send_err) = StreamWriter::loop_iteration_error(cancellation_token, error_tx, receive_messages_iteration_error).await {
                        warn!("can't send error from stream writer receive_messages_loop: {send_err}");
                    }
                    break;
                }
            }
        }
    }

    async fn receive_messages_loop_iteration(
        queue: &Queue,
        server_messages_receiver: &mut AsyncGrpcStreamWrapper<
            stream_write_message::FromClient,
            stream_write_message::FromServer,
        >,
    ) -> YdbResult<()> {
        match server_messages_receiver.receive::<RawServerMessage>().await {
            Ok(message) => match message {
                RawServerMessage::Init(_init_response_body) => {
                    return Err(YdbError::custom(
                        "unexpected message type in stream reader: init_response",
                    ));
                }
                RawServerMessage::Write(write_response_body) => {
                    for raw_ack in write_response_body.acks {
                        let write_ack = WriteAck::from(raw_ack);
                        queue.acknowledge_message(write_ack).await?;
                    }
                }
                RawServerMessage::UpdateToken(_update_token_response_body) => {}
            },
            Err(some_err) => {
                return Err(YdbError::from(some_err));
            }
        }
        Ok(())
    }

    async fn loop_iteration_error(
        cancellation_token: CancellationToken,
        error_tx: Arc<Mutex<Option<oneshot::Sender<YdbError>>>>,
        error: YdbError,
    ) -> Result<(), YdbError> {
        cancellation_token.cancel();

        let Some(tx) = error_tx.lock().await.take() else {
            return Ok(());
        };

        tx.send(error)
    }

    pub(crate) async fn stop(mut self) -> YdbResult<()> {
        trace!("stopping...");

        self.cancellation_token.cancel();

        while let Some(join_result) = self.tasks.join_next().await {
            join_result?;
        }

        trace!("stream writer stopped");
        Ok(())
    }
}
