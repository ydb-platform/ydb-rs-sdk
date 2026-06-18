use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{trace, warn};

use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::WriteRequest;
use ydb_grpc::ydb_proto::topic::{stream_write_message, Codec};

use crate::client_topic::topicwriter::message_write_status::WriteAck;
use crate::client_topic::topicwriter::queue::Queue;
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_topic_service::stream_write::RawServerMessage;
use crate::{YdbError, YdbResult};

/// Manages the gRPC stream communications: write loop and receive-messages loop.
/// Reports error via error_tx.
pub(crate) struct StreamWriter {
    write_messages_loop: JoinHandle<()>,
    receive_messages_loop: JoinHandle<()>,
    cancellation_token: CancellationToken,
}

struct WriterLoopParams {
    write_request_messages_chunk_size: usize,
    write_request_send_messages_period: Duration,
    request_stream: mpsc::UnboundedSender<stream_write_message::FromClient>,
}

impl StreamWriter {
    pub(crate) async fn new(
        writer_options: TopicWriterOptions,
        stream: AsyncGrpcStreamWrapper<
            stream_write_message::FromClient,
            stream_write_message::FromServer,
        >,
        queue: Queue,
        error_tx: oneshot::Sender<YdbError>,
    ) -> Self {
        let cancellation_token = CancellationToken::new();

        // Both loops share the same oneshot error channel.
        let shared_error_tx = Arc::new(Mutex::new(Some(error_tx)));

        let write_messages_loop = tokio::spawn(StreamWriter::write_messages_loop(
            cancellation_token.clone(),
            shared_error_tx.clone(),
            queue.clone(),
            WriterLoopParams {
                write_request_messages_chunk_size: writer_options.write_request_messages_chunk_size,
                write_request_send_messages_period: writer_options
                    .write_request_send_messages_period,
                request_stream: stream.clone_sender(),
            },
        ));

        let receive_messages_loop = tokio::spawn(StreamWriter::receive_messages_loop(
            cancellation_token.clone(),
            shared_error_tx,
            queue.clone(),
            stream,
        ));

        Self {
            write_messages_loop,
            receive_messages_loop,
            cancellation_token,
        }
    }

    async fn write_messages_loop(
        cancellation_token: CancellationToken,
        error_tx: Arc<Mutex<Option<oneshot::Sender<YdbError>>>>,
        queue: Queue,
        task_params: WriterLoopParams,
    ) {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => { return; }
                result = StreamWriter::write_messages_loop_iteration(&queue, &task_params) => {
                    let Err(write_messages_iteration_error) = result else {
                        continue;
                    };

                    warn!(
                        "error sending message in topic writer write_messages_loop: {}",
                        &write_messages_iteration_error
                    );

                    if let Err(send_err) = StreamWriter::loop_iteration_error(cancellation_token, error_tx, write_messages_iteration_error).await {
                        warn!("can't send error from stream writer write_messages_loop: {send_err}");
                    }

                    break;
                }
            }
        }
    }

    async fn write_messages_loop_iteration(
        queue: &Queue,
        task_params: &WriterLoopParams,
    ) -> YdbResult<()> {
        let messages_to_send = queue
            .get_messages_to_send(
                task_params.write_request_messages_chunk_size,
                task_params.write_request_send_messages_period,
            )
            .await;

        if messages_to_send.is_empty() {
            return Ok(());
        }

        trace!("sending topic message to grpc stream");
        task_params
            .request_stream
            .send(stream_write_message::FromClient {
                client_message: Some(ClientMessage::WriteRequest(WriteRequest {
                    messages: messages_to_send,
                    codec: Codec::Raw as i32,
                    tx: None,
                })),
            })
            .map_or_else(|err| Err(YdbError::Transport(err.to_string())), |_| Ok(()))
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

    pub(crate) async fn stop(self) -> YdbResult<()> {
        trace!("stopping...");

        self.cancellation_token.cancel();

        let write_messages_loop_result = self.write_messages_loop.await.map_err(|err| {
            let err = YdbError::custom(format!(
                "stop: error while waiting for write_messages_loop_result to finish: {err}"
            ));
            trace!("{err}");
            err
        });
        trace!("write messages loop stopped");

        let receive_messages_loop_result = self.receive_messages_loop.await.map_err(|err| {
            let err = YdbError::custom(format!(
                "stop: error while waiting for receive_messages_loop to finish: {err}"
            ));
            trace!("{err}");
            err
        });
        trace!("receive messages loop stopped");

        write_messages_loop_result?;
        receive_messages_loop_result?;

        Ok(())
    }
}
