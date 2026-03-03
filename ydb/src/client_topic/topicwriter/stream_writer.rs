use std::borrow::{Borrow, BorrowMut};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, UNIX_EPOCH};

use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::log::{trace, warn};

use ydb_grpc::ydb_proto::topic::stream_write_message;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::{message_data, MessageData};
use ydb_grpc::ydb_proto::topic::stream_write_message::{InitRequest, WriteRequest};

use crate::client_topic::topicwriter::connection::ConnectionInfo;
use crate::client_topic::topicwriter::message::TopicWriterMessage;
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

pub(crate) struct StreamWriterParams {
    pub(crate) writer_options: TopicWriterOptions,
    pub(crate) producer_id: String,
    pub(crate) message_queue: Arc<MessageQueue>,
}

struct WriterLoopParams {
    write_request_messages_chunk_size: usize,
    write_request_send_messages_period: Duration,
    producer_id: Option<String>,
    request_stream: mpsc::UnboundedSender<stream_write_message::FromClient>,
}

impl StreamWriter {
    pub async fn new(
        params: StreamWriterParams,
        connection_manager: GrpcConnectionManager,
        connection_info: Arc<TokioMutex<ConnectionInfo>>,
        confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,
        messages_receiver: mpsc::Receiver<TopicWriterMessage>,
        error_tx: oneshot::Sender<YdbError>,
    ) -> YdbResult<Self> {
        let init_request_body = InitRequest {
            path: params.writer_options.topic_path.clone(),
            producer_id: params.producer_id.clone(),
            write_session_meta: params
                .writer_options
                .session_metadata
                .clone()
                .unwrap_or_default(),
            get_last_seq_no: params.writer_options.auto_seq_no,
            partitioning: Some(
                params
                    .writer_options
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
            let mut guard = connection_info.lock().await;
            guard.partition_id = init_response.partition_id;
            guard.session_id = init_response.session_id;
            guard.last_seq_no_assigned = init_response.last_seq_no;
            guard.codecs_from_server = init_response.supported_codecs;
        }

        let cancellation_token = CancellationToken::new();

        // Both loops share the same oneshot error channel.
        let shared_error_tx = Arc::new(TokioMutex::new(Some(error_tx)));

        let writer_loop_cancellation_token = cancellation_token.clone();
        let writer_loop_error_tx = shared_error_tx.clone();

        let message_receive_loop_cancellation_token = cancellation_token.clone();
        let message_receive_loop_error_tx = shared_error_tx.clone();
        let message_loop_reception_queue = confirmation_reception_queue.clone();

        let writer_loop_task_params = WriterLoopParams {
            write_request_messages_chunk_size: params
                .writer_options
                .write_request_messages_chunk_size,
            write_request_send_messages_period: params
                .writer_options
                .write_request_send_messages_period,
            producer_id: Some(params.producer_id.clone()),
            request_stream: stream.clone_sender(),
        };

        let writer_loop = tokio::spawn(async move {
            let mut message_receiver = messages_receiver; // force move inside
            let task_params = writer_loop_task_params; // force move inside
            let message_queue = params.message_queue;

            loop {
                if writer_loop_cancellation_token.is_cancelled() {
                    break;
                }

                let Err(writer_iteration_error) = StreamWriter::write_loop_iteration(
                    message_queue,
                    message_receiver.borrow_mut(),
                    task_params.borrow(),
                )
                .await
                else {
                    continue;
                };

                writer_loop_cancellation_token.cancel();

                let Some(tx) = writer_loop_error_tx.lock().await.take() else {
                    break;
                };

                let Err(send_err) = tx.send(writer_iteration_error.clone()) else {
                    break;
                };

                warn!("can't send error to stream writer: {send_err} (original error: {writer_iteration_error})");
                break;
            }
        });

        let receive_messages_loop = tokio::spawn(async move {
            let mut stream = stream; // force move inside
            let mut reception_queue = message_loop_reception_queue; // force move inside

            loop {
                tokio::select! {
                    _ = message_receive_loop_cancellation_token.cancelled() => { return; }
                    message_receive_it_res = async {
                        StreamWriter::receive_messages_loop_iteration(
                            stream.borrow_mut(),
                            reception_queue.borrow_mut(),
                        ).await
                    } => {
                        let Err(receive_message_it_error) = message_receive_it_res else {
                            continue;
                        };

                        message_receive_loop_cancellation_token.cancel();
                        warn!(
                            "error receiving message in topic writer receiver stream loop: {}",
                            &receive_message_it_error
                        );

                        let Some(tx) = message_receive_loop_error_tx.lock().await.take() else {
                            break;
                        };

                        let Err(send_err) = tx.send(receive_message_it_error.clone()) else {
                            break;
                        };

                        warn!("can't send error to stream writer: {send_err} (original error: {receive_message_it_error})");
                        break;
                    }
                }
            }
        });

        Ok(Self {
            writer_loop,
            receive_messages_loop,
            cancellation_token,
        })
    }

    async fn write_loop_iteration(
        message_queue: &Arc<MessageQueue>,
        messages_receiver: &mut Receiver<TopicWriterMessage>,
        task_params: &WriterLoopParams,
    ) -> YdbResult<()> {
        let start = Instant::now();

        // wait for messages loop
        'messages_loop: loop {
            let messages = message_queue.get_messages_to_be_sent();
            let elapsed = start.elapsed();
            let (messages_len, messages_is_empty) = {
                let len = messages.len();
                (len, len == 0)
            };

            if messages_len >= task_params.write_request_messages_chunk_size
                || (!messages_is_empty && elapsed >= task_params.write_request_send_messages_period)
            {
                break;
            }

            // TODO: IT MUST NOT BE HERE - writing a message to the queue should be in a separate loop (maybe even on the Reconnector level)

            // TODO: Getting messages from the queue and checking length should be a single operation.
            // Maybe there should be a Queue method like get_messages_to_be_sent_if_length_is_greater_than()
            // And then check if it for once returns Some(messages). If it doesn't after a timeout, just call get_messages_to_be_sent().
            match timeout(
                task_params.write_request_send_messages_period - elapsed,
                messages_receiver.recv(),
            )
            .await
            {
                Ok(Some(message)) => {
                    let data_size = message.data.len() as i64;
                    let mut messages_guard = messages;
                    messages_guard.push(MessageData {
                        seq_no: message
                            .seq_no
                            .ok_or_else(|| YdbError::custom("empty message seq_no"))?,
                        created_at: Some(ydb_grpc::google_proto_workaround::protobuf::Timestamp {
                            seconds: message.created_at.duration_since(UNIX_EPOCH)?.as_secs()
                                as i64,
                            nanos: message.created_at.duration_since(UNIX_EPOCH)?.as_nanos() as i32,
                        }),
                        metadata_items: vec![],
                        data: message.data,
                        uncompressed_size: data_size,
                        partitioning: Some(message_data::Partitioning::MessageGroupId(
                            task_params.producer_id.clone().unwrap_or_default(),
                        )),
                    });
                }
                Ok(None) => {
                    trace!("Channel has been closed. Stop topic send messages loop.");
                    return Ok(());
                }
                Err(_elapsed) => {
                    break 'messages_loop;
                }
            }
        }

        let messages_to_send = {
            let mut messages_guard = messages.lock().await;
            if messages_guard.is_empty() {
                return Ok(());
            }
            // Here we preventively assume that all messages are read ("clear, restore if failed").
            // - If write succeeds, then it's all right and we just keep going
            // - If write fails, then we put these unwritten messages back WITH taking into account messages that might've been written into messages.
            //
            // The "clone, clear if success" approach is dangerous because we can lose messages that are appended in another coroutine.
            messages_guard.drain(..).collect::<Vec<MessageData>>()
        };

        if messages_to_send.is_empty() {
            return Ok(());
        }

        trace!("Sending topic message to grpc stream");
        let send_result = task_params
            .request_stream
            .send(stream_write_message::FromClient {
                client_message: Some(ClientMessage::WriteRequest(WriteRequest {
                    messages: messages_to_send,
                    codec: 1,
                    tx: None,
                })),
            });

        match send_result {
            Ok(_) => Ok(()),
            Err(err) => {
                // If sending fails, put messages back for next iteration
                let mut messages_guard = messages.lock().await;

                // Prepend failed messages back to the front
                let err_message = err.to_string();
                let mut failed = match err.0.client_message {
                    Some(ClientMessage::WriteRequest(write_request)) => write_request.messages,
                    _ => Vec::new(),
                };
                failed.extend(messages_guard.drain(..));
                *messages_guard = failed;

                Err(YdbError::Transport(err_message))
            }
        }
    }

    async fn receive_messages_loop_iteration(
        server_messages_receiver: &mut AsyncGrpcStreamWrapper<
            stream_write_message::FromClient,
            stream_write_message::FromServer,
        >,
        confirmation_reception_queue: &Arc<Mutex<TopicWriterReceptionQueue>>,
    ) -> YdbResult<()> {
        match server_messages_receiver.receive::<RawServerMessage>().await {
            Ok(message) => match message {
                RawServerMessage::Init(_init_response_body) => {
                    return Err(YdbError::Custom(
                        "Unexpected message type in stream reader: init_response".to_string(),
                    ));
                }
                RawServerMessage::Write(write_response_body) => {
                    for raw_ack in write_response_body.acks {
                        let write_ack = WriteAck::from(raw_ack);
                        let mut reception_queue = confirmation_reception_queue.lock().unwrap();
                        let reception_ticket = reception_queue.try_get_ticket();
                        match reception_ticket {
                            None => {
                                return Err(YdbError::Custom(
                                    "Expected reception ticket to be actually present".to_string(),
                                ));
                            }
                            Some(ticket) => {
                                if write_ack.seq_no != ticket.get_seq_no() {
                                    return Err(YdbError::custom(format!(
                                        "Reception ticket and write ack seq_no mismatch. Seqno from ack: {}, expected: {}",
                                        write_ack.seq_no, ticket.get_seq_no()
                                    )));
                                }
                                ticket.send_confirmation_if_needed(write_ack.status);
                            }
                        }
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

        self.writer_loop.await.map_err(|err| {
            YdbError::custom(format!(
                "stop: error while waiting for writer_loop to finish: {err}"
            ))
        })?; // TODO: handle error
        trace!("Writer loop stopped");

        self.receive_messages_loop.await.map_err(|err| {
            YdbError::custom(format!(
                "stop: error while waiting for receive_messages_loop to finish: {err}"
            ))
        })?; // TODO: handle error
        trace!("Message receive loop stopped");
        Ok(())
    }
}
