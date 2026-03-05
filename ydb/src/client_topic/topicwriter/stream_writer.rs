use std::borrow::{Borrow, BorrowMut};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, UNIX_EPOCH};

use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::log::{trace, warn};

use ydb_grpc::ydb_proto::topic::stream_write_message;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::{message_data, MessageData};
use ydb_grpc::ydb_proto::topic::stream_write_message::{InitRequest, WriteRequest};

use crate::client_topic::topicwriter::connection::ConnectionInfo;
use crate::client_topic::topicwriter::message::TopicWriterMessageWithAck;
use crate::client_topic::topicwriter::message_queue::MessageQueue;
use crate::client_topic::topicwriter::message_write_status::WriteAck;
use crate::client_topic::topicwriter::writer_reception_queue::{
    TopicWriterReceptionQueue, TopicWriterReceptionTicket, TopicWriterReceptionType,
};
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
    process_new_messages_loop: JoinHandle<()>,
    cancellation_token: CancellationToken,
}

pub(crate) struct StreamWriterParams {
    pub(crate) writer_options: TopicWriterOptions,
    pub(crate) producer_id: String,
    pub(crate) message_queue: Arc<Mutex<MessageQueue>>,
}

struct WriterLoopParams {
    write_request_messages_chunk_size: usize,
    write_request_send_messages_period: Duration,
    request_stream: mpsc::UnboundedSender<stream_write_message::FromClient>,
}

impl StreamWriter {
    pub async fn new(
        params: StreamWriterParams,
        connection_manager: GrpcConnectionManager,
        connection_info: Arc<TokioMutex<ConnectionInfo>>,
        confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,
        messages_receiver: mpsc::Receiver<TopicWriterMessageWithAck>,
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
        let writer_loop_message_queue = params.message_queue.clone();
        let writer_loop_task_params = WriterLoopParams {
            write_request_messages_chunk_size: params
                .writer_options
                .write_request_messages_chunk_size,
            write_request_send_messages_period: params
                .writer_options
                .write_request_send_messages_period,
            request_stream: stream.clone_sender(),
        };

        let message_receive_loop_cancellation_token = cancellation_token.clone();
        let message_receive_loop_error_tx = shared_error_tx.clone();
        let message_receive_loop_message_queue = params.message_queue.clone();
        let message_loop_reception_queue = confirmation_reception_queue.clone();

        let process_new_messages_loop_cancellation_token = cancellation_token.clone();
        let process_new_messages_loop_error_tx = shared_error_tx.clone();
        let process_new_messages_loop_message_queue = params.message_queue.clone();
        let process_new_messages_loop_reception_queue = confirmation_reception_queue.clone();

        let writer_loop = tokio::spawn(async move {
            let task_params = writer_loop_task_params; // force move inside
            let message_queue = writer_loop_message_queue;

            loop {
                tokio::select! {
                    _ = writer_loop_cancellation_token.cancelled() => { return; }
                    result = StreamWriter::write_loop_iteration(&message_queue, task_params.borrow()) => {
                        let Err(writer_iteration_error) = result else {
                            continue;
                        };

                        writer_loop_cancellation_token.cancel();

                        let Some(tx) = writer_loop_error_tx.lock().await.take() else {
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
        });

        let receive_messages_loop = tokio::spawn(async move {
            let mut stream = stream; // force move inside
            let mut reception_queue = message_loop_reception_queue; // force move inside
            let message_queue = message_receive_loop_message_queue;

            loop {
                tokio::select! {
                    _ = message_receive_loop_cancellation_token.cancelled() => { return; }
                    message_receive_it_res = StreamWriter::receive_messages_loop_iteration(
                        &message_queue,
                        stream.borrow_mut(),
                        reception_queue.borrow_mut(),
                    ) => {
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

                        warn!("can't send error from stream writer receive_messages_loop: {send_err} (original error: {receive_message_it_error})");
                        break;
                    }
                }
            }
        });

        let process_new_messages_loop = tokio::spawn(async move {
            let message_queue = process_new_messages_loop_message_queue;
            let mut messages_receiver = messages_receiver;
            let reception_queue = process_new_messages_loop_reception_queue;
            let producer_id = params.producer_id.clone();

            loop {
                tokio::select! {
                    _ = process_new_messages_loop_cancellation_token.cancelled() => {
                        return;
                    }
                    result = StreamWriter::process_new_messages_loop_iteration(
                        &message_queue,
                        &mut messages_receiver,
                        &reception_queue,
                        producer_id.clone(),
                        params.writer_options.write_request_send_messages_period,
                    ) => {
                        let Err(incoming_messages_it_error) = result else {
                            continue;
                        };

                        process_new_messages_loop_cancellation_token.cancel();
                        warn!(
                            "error processing a new message to be writte in topic writer stream loop: {}",
                            &incoming_messages_it_error
                        );

                        let Some(tx) = process_new_messages_loop_error_tx.lock().await.take() else {
                            break;
                        };

                        let Err(send_err) = tx.send(incoming_messages_it_error.clone()) else {
                            break;
                        };

                        warn!("can't send error from stream writer process_new_messages_loop: {send_err} (original error: {incoming_messages_it_error})");
                        break;
                    }
                }
            }
        });

        Ok(Self {
            writer_loop,
            receive_messages_loop,
            cancellation_token,
            process_new_messages_loop,
        })
    }

    async fn get_messages_to_send(
        message_queue: &Arc<Mutex<MessageQueue>>,
        size_threshold: usize,
        send_messages_period: Duration,
    ) -> Option<Vec<MessageData>> {
        let start = Instant::now();

        loop {
            let (maybe_messages, length) = {
                let mut message_queue_guard = message_queue.lock().unwrap();
                message_queue_guard.get_messages_to_send_if_big_enough(size_threshold)
            };

            match maybe_messages {
                Some(messages) => return Some(messages),
                None => {
                    let elapsed = start.elapsed();
                    if elapsed >= send_messages_period {
                        if length != 0 {
                            let mut message_queue_guard = message_queue.lock().unwrap();
                            return Some(message_queue_guard.get_messages_to_send());
                        }
                        return None;
                    }
                    let remaining = send_messages_period.saturating_sub(elapsed);
                    // TODO: mitigate without bombarding message_queue mutex
                    sleep(remaining.min(Duration::from_millis(50))).await;
                }
            }
        }
    }

    async fn write_loop_iteration(
        message_queue: &Arc<Mutex<MessageQueue>>,
        task_params: &WriterLoopParams,
    ) -> YdbResult<()> {
        let messages_to_send = StreamWriter::get_messages_to_send(
            message_queue,
            task_params.write_request_messages_chunk_size,
            task_params.write_request_send_messages_period,
        )
        .await;
        let Some(messages_to_send) = messages_to_send else {
            return Ok(());
        };
        if messages_to_send.is_empty() {
            return Ok(());
        }

        trace!("Sending topic message to grpc stream");
        match task_params
            .request_stream
            .send(stream_write_message::FromClient {
                client_message: Some(ClientMessage::WriteRequest(WriteRequest {
                    messages: messages_to_send,
                    codec: 1,
                    tx: None,
                })),
            }) {
            Ok(_) => Ok(()),
            Err(err) => Err(YdbError::Transport(err.to_string())),
        }
    }

    async fn receive_messages_loop_iteration(
        message_queue: &Arc<Mutex<MessageQueue>>,
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
                                {
                                    let mut message_queue_guard = message_queue.lock().unwrap();
                                    message_queue_guard.acknowledge_message(write_ack.seq_no)?;
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

    async fn process_new_messages_loop_iteration(
        message_queue: &Arc<Mutex<MessageQueue>>,
        messages_receiver: &mut Receiver<TopicWriterMessageWithAck>,
        confirmation_reception_queue: &Arc<Mutex<TopicWriterReceptionQueue>>,
        producer_id: String,
        duration: Duration,
    ) -> YdbResult<()> {
        match timeout(duration, messages_receiver.recv()).await {
            Ok(Some(message_with_ack)) => {
                let message = message_with_ack.message;
                let data_size = message.data.len() as i64;

                let seq_no = message
                    .seq_no
                    .ok_or_else(|| YdbError::custom("empty message seq_no is provided"))?;

                {
                    let mut message_queue_guard = message_queue.lock().unwrap();
                    message_queue_guard.add_message(MessageData {
                        seq_no: seq_no,
                        created_at: Some(ydb_grpc::google_proto_workaround::protobuf::Timestamp {
                            seconds: message.created_at.duration_since(UNIX_EPOCH)?.as_secs()
                                as i64,
                            nanos: message.created_at.duration_since(UNIX_EPOCH)?.as_nanos() as i32,
                        }),
                        metadata_items: vec![],
                        data: message.data,
                        uncompressed_size: data_size,
                        partitioning: Some(message_data::Partitioning::MessageGroupId(
                            producer_id.clone(),
                        )),
                    })?;
                }

                let ack = message_with_ack.ack;
                let reception_type = ack.map_or(
                    TopicWriterReceptionType::NoConfirmationExpected,
                    TopicWriterReceptionType::AwaitingConfirmation,
                );

                {
                    let mut reception_queue = confirmation_reception_queue.lock().unwrap();
                    reception_queue
                        .add_ticket(TopicWriterReceptionTicket::new(seq_no, reception_type));
                }
            }
            Ok(None) => {
                trace!("Channel has been closed. Stop topic send messages loop.");
            }
            Err(_elapsed) => {}
        };

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

        let process_new_messages_loop_result =
            self.process_new_messages_loop.await.map_err(|err| {
                let err = YdbError::custom(format!(
                    "stop: error while waiting for process_new_messages_loop to finish: {err}"
                ));
                trace!("{err}");
                err
            });
        trace!("Process new messages loop stopped");

        writer_loop_result?;
        receive_messages_loop_result?;
        process_new_messages_loop_result?;

        Ok(())
    }
}
