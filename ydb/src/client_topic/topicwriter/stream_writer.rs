use std::borrow::{Borrow, BorrowMut};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex as TokioMutex;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::log::{trace, warn};

use ydb_grpc::ydb_proto::topic::stream_write_message;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::{InitRequest, WriteRequest};

use crate::client_topic::topicwriter::connection::ConnectionInfo;
use crate::client_topic::topicwriter::message_queue::MessageQueue;
use crate::client_topic::topicwriter::message_write_status::WriteAck;
use crate::client_topic::topicwriter::writer_reception_queue::TopicWriterReceptionQueue;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_topic_service::client::RawTopicClient;
use crate::grpc_wrapper::raw_topic_service::stream_write::init::RawInitResponse;
use crate::grpc_wrapper::raw_topic_service::stream_write::RawServerMessage;
use crate::{TopicWriterOptions, YdbError, YdbResult};

/// Manages the gRPC stream communications: write loop and receive-messages loop.
/// Reports error via error_tx.
pub(crate) struct StreamWriter {
    writer_loop: JoinHandle<()>,
    receive_messages_loop: JoinHandle<()>,
    cancellation_token: CancellationToken,
}

struct WriterLoopParams {
    write_request_messages_chunk_size: usize,
    write_request_send_messages_period: Duration,
    request_stream: mpsc::UnboundedSender<stream_write_message::FromClient>,
}

impl StreamWriter {
    pub async fn new(
        writer_options: TopicWriterOptions,
        producer_id: String,
        message_queue: MessageQueue,
        connection_manager: GrpcConnectionManager,
        connection_info: Arc<TokioMutex<ConnectionInfo>>,
        confirmation_reception_queue: Arc<TokioMutex<TopicWriterReceptionQueue>>,
        error_tx: oneshot::Sender<YdbError>,
    ) -> YdbResult<Self> {
        let init_request_body = InitRequest {
            path: writer_options.topic_path.clone(),
            producer_id: producer_id.clone(),
            write_session_meta: writer_options.session_metadata.clone().unwrap_or_default(),
            get_last_seq_no: writer_options.auto_seq_no,
            partitioning: Some(
                writer_options
                    .partitioning
                    .to_grpc_init_partitioning(params.producer_id.clone()),
            ),
        };

        let mut topic_service = connection_manager
            .get_auth_service(RawTopicClient::new)
            .await?;

        let mut stream = topic_service
            .stream_write(init_request_body.clone())
            .await?;
        let init_response = RawInitResponse::try_from(stream.receive::<RawServerMessage>().await?)?;
        {
            let mut connection_info = connection_info.lock().await;
            connection_info.partition_id = init_response.partition_id;
            connection_info.session_id = init_response.session_id;
            connection_info.last_seq_no_assigned = init_response.last_seq_no;
            connection_info.codecs_from_server = init_response.supported_codecs;
        }

        let cancellation_token = CancellationToken::new();

        // Both loops share the same oneshot error channel.
        let shared_error_tx = Arc::new(TokioMutex::new(Some(error_tx)));

        let writer_loop_cancellation_token = cancellation_token.clone();
        let writer_loop_error_tx = shared_error_tx.clone();
        let writer_loop_message_queue = message_queue.clone();
        let writer_loop_task_params = WriterLoopParams {
            write_request_messages_chunk_size: writer_options.write_request_messages_chunk_size,
            write_request_send_messages_period: writer_options.write_request_send_messages_period,
            request_stream: stream.clone_sender(),
        };

        let message_receive_loop_cancellation_token = cancellation_token.clone();
        let message_receive_loop_error_tx = shared_error_tx.clone();
        let message_receive_loop_message_queue = message_queue.clone();
        let message_receive_loop_reception_queue = confirmation_reception_queue.clone();

        let writer_loop = tokio::spawn(StreamWriter::writer_loop(
            writer_loop_cancellation_token,
            writer_loop_error_tx,
            writer_loop_message_queue,
            writer_loop_task_params,
        ));

        let receive_messages_loop = tokio::spawn(StreamWriter::receive_messages_loop(
            message_receive_loop_cancellation_token,
            message_receive_loop_error_tx,
            message_receive_loop_message_queue,
            message_receive_loop_reception_queue,
            stream,
        ));

        Ok(Self {
            writer_loop,
            receive_messages_loop,
            cancellation_token,
        })
    }

    async fn writer_loop(
        cancellation_token: CancellationToken,
        error_tx: Arc<TokioMutex<Option<oneshot::Sender<YdbError>>>>,
        message_queue: MessageQueue,
        task_params: WriterLoopParams,
    ) {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => { return; }
                result = StreamWriter::writer_loop_iteration(&message_queue, task_params.borrow()) => {
                    let Err(writer_iteration_error) = result else {
                        continue;
                    };

                    cancellation_token.cancel();

                    let Some(tx) = error_tx.lock().await.take() else {
                        break;
                    };

                    let Err(send_err) = tx.send(writer_iteration_error.clone()) else {
                        break;
                    };

                    warn!("can't send error from stream writer writer_loop: {send_err} (original error: {writer_iteration_error})");
                    break;
                }
            }
        }
    }

    async fn writer_loop_iteration(
        message_queue: &MessageQueue,
        task_params: &WriterLoopParams,
    ) -> YdbResult<()> {
        let messages_to_send = match message_queue
            .get_messages_to_send(
                task_params.write_request_messages_chunk_size,
                task_params.write_request_send_messages_period,
            )
            .await
        {
            Ok(messages) => messages,
            Err(err) => return Err(err),
        };
        if messages_to_send.is_empty() {
            return Ok(());
        }

        trace!("Sending topic message to grpc stream");
        task_params
            .request_stream
            .send(stream_write_message::FromClient {
                client_message: Some(ClientMessage::WriteRequest(WriteRequest {
                    messages: messages_to_send,
                    codec: 1,
                    tx: None,
                })),
            })
            .map_or_else(|err| Err(YdbError::Transport(err.to_string())), |_| Ok(()))
    }

    async fn receive_messages_loop(
        cancellation_token: CancellationToken,
        error_tx: Arc<TokioMutex<Option<oneshot::Sender<YdbError>>>>,
        message_queue: MessageQueue,
        reception_queue: Arc<TokioMutex<TopicWriterReceptionQueue>>,
        mut stream: AsyncGrpcStreamWrapper<
            stream_write_message::FromClient,
            stream_write_message::FromServer,
        >,
    ) {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => { return; }
                message_receive_it_res = StreamWriter::receive_messages_loop_iteration(
                    &message_queue,
                    stream.borrow_mut(),
                    &reception_queue,
                ) => {
                    let Err(receive_message_it_error) = message_receive_it_res else {
                        continue;
                    };

                    cancellation_token.cancel();
                    warn!(
                        "error receiving message in topic writer receiver stream loop: {}",
                        &receive_message_it_error
                    );

                    let Some(tx) = error_tx.lock().await.take() else {
                        break;
                    };

                    let Err(send_err) = tx.send(receive_message_it_error.clone()) else {
                        break;
                    };

                    warn!("can't send error from stream writer receive_messages_loop: {send_err} (original error: {receive_message_it_error})");
                    break;
                }
            }
        }
    }

    async fn receive_messages_loop_iteration(
        message_queue: &MessageQueue,
        server_messages_receiver: &mut AsyncGrpcStreamWrapper<
            stream_write_message::FromClient,
            stream_write_message::FromServer,
        >,
        confirmation_reception_queue: &Arc<TokioMutex<TopicWriterReceptionQueue>>,
    ) -> YdbResult<()> {
        match server_messages_receiver.receive::<RawServerMessage>().await {
            Ok(message) => match message {
                RawServerMessage::Init(_init_response_body) => {
                    return Err(YdbError::custom(
                        "Unexpected message type in stream reader: init_response",
                    ));
                }
                RawServerMessage::Write(write_response_body) => {
                    for raw_ack in write_response_body.acks {
                        let write_ack = WriteAck::from(raw_ack);
                        let ticket = {
                            let mut reception_queue = confirmation_reception_queue.lock().await;
                            let reception_ticket = reception_queue.try_get_ticket();
                            match reception_ticket {
                                None => {
                                    return Err(YdbError::custom(
                                        "Expected reception ticket to be actually present",
                                    ));
                                }
                                Some(ticket) => {
                                    if write_ack.seq_no != ticket.get_seq_no() {
                                        return Err(YdbError::custom(format!(
                                            "Reception ticket and write ack seq_no mismatch. Seqno from ack: {}, expected: {}",
                                            write_ack.seq_no,
                                            ticket.get_seq_no()
                                        )));
                                    }
                                    ticket
                                }
                            }
                        };
                        message_queue.acknowledge_message(write_ack.seq_no).await?;
                        ticket.send_confirmation_if_needed(write_ack.status);
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

    pub async fn stop(self) -> YdbResult<()> {
        trace!("Stopping...");

        self.cancellation_token.cancel();

        let writer_loop_result = self.writer_loop.await.map_err(|err| {
            let err = YdbError::custom(format!(
                "stop: error while waiting for writer_loop to finish: {err}"
            ));
            trace!("{err}");
            err
        });
        trace!("Writer loop stopped");

        let receive_messages_loop_result = self.receive_messages_loop.await.map_err(|err| {
            let err = YdbError::custom(format!(
                "stop: error while waiting for receive_messages_loop to finish: {err}"
            ));
            trace!("{err}");
            err
        });
        trace!("Message receive loop stopped");

        writer_loop_result?;
        receive_messages_loop_result?;

        Ok(())
    }
}
