use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::message_write_status::{MessageWriteStatus, WriteAck};
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::client_topic::topicwriter::writer_reception_queue::{
    TopicWriterReceptionQueue, TopicWriterReceptionTicket, TopicWriterReceptionType,
};
use crate::errors::NeedRetry;
use crate::grpc_connection_manager::GrpcConnectionManager;

use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_topic_service::common::codecs::RawSupportedCodecs;
use crate::grpc_wrapper::raw_topic_service::stream_write::init::RawInitResponse;
use crate::grpc_wrapper::raw_topic_service::stream_write::RawServerMessage;
use crate::{grpc_wrapper, YdbError, YdbResult};
use std::borrow::{Borrow, BorrowMut};

use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;
use std::time::{Duration, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

use tokio_util::sync::CancellationToken;
use tracing::log::trace;
use tracing::warn;
use ydb_grpc::ydb_proto::topic::stream_write_message;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::{message_data, MessageData};
use ydb_grpc::ydb_proto::topic::stream_write_message::{InitRequest, WriteRequest};

pub(crate) enum TopicWriterState {
    Working,
    FinishedWithError(YdbError),
}

/// TopicWriter is currently in development.
/// It is mostly usable, but has some unimplemented features.
#[allow(dead_code)]
pub struct TopicWriter {
    pub(crate) path: String,
    pub(crate) producer_id: Option<String>,
    pub(crate) write_request_messages_chunk_size: usize,
    pub(crate) write_request_send_messages_period: Duration,

    pub(crate) auto_set_seq_no: bool,
    pub(crate) init_state: Arc<TokioMutex<ConnectionInfo>>,

    flush_timeout: Duration,

    writer_message_sender: Arc<TokioMutex<mpsc::Sender<TopicWriterMessage>>>,

    cancellation_token: CancellationToken,
    writer_state: Arc<Mutex<TopicWriterState>>,

    confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,

    reconnection_loop: JoinHandle<()>,
}

#[allow(dead_code)]
pub struct AckFuture {
    receiver: oneshot::Receiver<MessageWriteStatus>,
}

impl Future for AckFuture {
    type Output = YdbResult<MessageWriteStatus>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(_cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(Ok(result)),
            Poll::Ready(Err(_)) => Poll::Ready(Err(YdbError::custom("message writer was closed"))),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectionInfo {
    partition_id: i64,
    session_id: String,
    last_seq_no_assigned: i64,
    codecs_from_server: RawSupportedCodecs,
}

struct ReconnectionLoopParams {
    writer_options: TopicWriterOptions,
    producer_id: String,
    connection_manager: GrpcConnectionManager,
    writer_state: Arc<Mutex<TopicWriterState>>,
    cancellation_token: CancellationToken,
    writer_message_sender: Arc<TokioMutex<mpsc::Sender<TopicWriterMessage>>>,
    confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,
    connection_info: Arc<TokioMutex<ConnectionInfo>>,
    initial_messages_receiver: mpsc::Receiver<TopicWriterMessage>,
}

impl TopicWriter {
    pub(crate) async fn new(
        writer_options: TopicWriterOptions,
        connection_manager: GrpcConnectionManager,
    ) -> YdbResult<Self> {
        let producer_id = writer_options
            .producer_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let cancellation_token = CancellationToken::new();

        let writer_state = Arc::new(Mutex::new(TopicWriterState::Working));

        let (initial_messages_sender, initial_messages_receiver) = mpsc::channel(32_usize);
        let writer_message_sender = Arc::new(TokioMutex::new(initial_messages_sender));

        let confirmation_reception_queue = Arc::new(Mutex::new(TopicWriterReceptionQueue::new()));
        let connection_info = Arc::new(TokioMutex::new(ConnectionInfo {
            partition_id: 0,
            session_id: String::new(),
            last_seq_no_assigned: 0,
            codecs_from_server: RawSupportedCodecs::default(),
        }));
        let reconnection_loop = TopicWriter::spawn_reconnection_loop(ReconnectionLoopParams {
            writer_options: writer_options.clone(),
            producer_id: producer_id.clone(),
            connection_manager,
            writer_state: writer_state.clone(),
            cancellation_token: cancellation_token.clone(),
            writer_message_sender: writer_message_sender.clone(),
            confirmation_reception_queue: confirmation_reception_queue.clone(),
            connection_info: connection_info.clone(),
            initial_messages_receiver,
        })
        .await?;

        Ok(Self {
            path: writer_options.topic_path.clone(),
            producer_id: Some(producer_id),
            write_request_messages_chunk_size: writer_options.write_request_messages_chunk_size,
            write_request_send_messages_period: writer_options.write_request_send_messages_period,
            auto_set_seq_no: writer_options.auto_seq_no,
            init_state: connection_info,
            flush_timeout: writer_options.flush_timeout,
            writer_message_sender,
            cancellation_token,
            writer_state,
            confirmation_reception_queue,
            reconnection_loop,
        })
    }

    async fn spawn_reconnection_loop(params: ReconnectionLoopParams) -> YdbResult<JoinHandle<()>> {
        let (connection_info_filled_tx, connection_info_filled_rx) =
            oneshot::channel::<YdbResult<()>>();
        let reconnection_loop = tokio::spawn(async move {
            let mut messages_receiver = params.initial_messages_receiver;
            let mut connection_info_filled_tx = Some(connection_info_filled_tx);
            // TODO: buffer might grow quite big if reconnection keeps failing.
            let messages = Arc::new(TokioMutex::new(Vec::<MessageData>::new()));

            loop {
                let (want_reconnect_sender, want_reconnect_receiver) = oneshot::channel();

                let supervisor = match WriteSupervisor::new(
                    WriteSupervisorParams {
                        writer_options: params.writer_options.clone(),
                        producer_id: params.producer_id.clone(),
                        messages: messages.clone(),
                    },
                    params.connection_manager.clone(),
                    params.connection_info.clone(),
                    params.confirmation_reception_queue.clone(),
                    params.writer_state.clone(),
                    messages_receiver,
                    want_reconnect_sender,
                )
                .await
                {
                    Ok(supervisor) => supervisor,
                    Err(err) => {
                        trace!("Error creating write loop supervisor: {}", err);
                        if TopicWriter::is_retry_allowed(&err) {
                            TopicWriter::wait_before_reconnect(&err).await;
                            messages_receiver = TopicWriter::recreate_message_channel(
                                &params.writer_message_sender,
                            )
                            .await;
                            continue;
                        }

                        if let Some(tx) = connection_info_filled_tx.take() {
                            let _ = tx.send(Err(err.clone()));
                        }
                        let mut writer_state = params.writer_state.lock().unwrap();
                        *writer_state = TopicWriterState::FinishedWithError(err);
                        break;
                    }
                };

                if let Some(tx) = connection_info_filled_tx.take() {
                    let _ = tx.send(Ok(()));
                };

                tokio::select! {
                    _ = params.cancellation_token.cancelled() => {
                        let _ = supervisor.stop().await;
                        break;
                    }
                    err = want_reconnect_receiver => {
                        let err = match err {
                            Ok(err) => err,
                            Err(chan_err) => {
                                // TODO: ???
                                trace!("Channel error: {}", chan_err);
                                let _ = supervisor.stop().await;  // TODO: handle error
                                let mut writer_state = params.writer_state.lock().unwrap();
                                *writer_state = TopicWriterState::FinishedWithError(
                                    YdbError::custom(format!("write supervisor channel closed: {chan_err}")),
                                );
                                break;
                            }
                        };

                        if TopicWriter::is_retry_allowed(&err) {
                            TopicWriter::wait_before_reconnect(&err).await;
                        } else {
                            trace!("Unknown error: {}", err);
                            let _ = supervisor.stop().await;  // TODO: handle error
                            let mut writer_state = params.writer_state.lock().unwrap();
                            *writer_state = TopicWriterState::FinishedWithError(err);
                            break;
                        };
                    }
                }

                messages_receiver =
                    TopicWriter::recreate_message_channel(&params.writer_message_sender).await;
            }
        });

        match connection_info_filled_rx.await {
            Ok(Ok(())) => Ok(reconnection_loop),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(YdbError::custom("connection info filled channel closed")),
        }
    }

    async fn recreate_message_channel(
        writer_message_sender: &Arc<TokioMutex<mpsc::Sender<TopicWriterMessage>>>,
    ) -> mpsc::Receiver<TopicWriterMessage> {
        let (new_messages_sender, new_messages_receiver) = mpsc::channel(32_usize);
        {
            let mut sender_guard = writer_message_sender.lock().await;
            *sender_guard = new_messages_sender;
        }
        new_messages_receiver
    }

    fn is_retry_allowed(err: &YdbError) -> bool {
        match err.need_retry() {
            NeedRetry::True => true,
            NeedRetry::IdempotentOnly => false, // TODO: ???
            NeedRetry::False => false,
        }
    }

    fn retry_wait_duration() -> Duration {
        // TODO: fine now, but smarter approach is needed
        Duration::from_secs(1)
    }

    async fn wait_before_reconnect(err: &YdbError) {
        trace!("Error, trying to reconnect: {}", err);
        sleep(TopicWriter::retry_wait_duration()).await;
    }

    pub async fn write(&mut self, message: TopicWriterMessage) -> YdbResult<()> {
        self.write_message(message, None).await?;
        Ok(())
    }

    pub async fn write_with_ack(
        &mut self,
        message: TopicWriterMessage,
    ) -> YdbResult<MessageWriteStatus> {
        let (tx, rx) = oneshot::channel();

        self.write_message(message, Some(tx)).await?;
        Ok(rx.await?)
    }

    pub async fn write_with_ack_future(
        &mut self,
        _message: TopicWriterMessage,
    ) -> YdbResult<AckFuture> {
        let (tx, rx) = oneshot::channel();

        self.write_message(_message, Some(tx)).await?;
        Ok(AckFuture { receiver: rx })
    }

    async fn write_message(
        &mut self,
        mut message: TopicWriterMessage,
        wait_ack: Option<oneshot::Sender<MessageWriteStatus>>,
    ) -> YdbResult<()> {
        self.is_cancelled().await?;

        let message_seqno = {
            let mut init_state = self.init_state.lock().await;
            if self.auto_set_seq_no {
                if message.seq_no.is_some() {
                    return Err(YdbError::custom(
                        "force set message seqno possible only if auto_set_seq_no disabled",
                    ));
                }
                message.seq_no = Some(init_state.last_seq_no_assigned + 1);
            };

            if let Some(mess_seqno) = message.seq_no {
                init_state.last_seq_no_assigned = mess_seqno;
                mess_seqno
            } else {
                return Err(YdbError::custom("need to set message seq_no"));
            }
        };

        let sender = { self.writer_message_sender.lock().await.clone() };
        sender
            .send(message)
            .await
            .map_err(|err| YdbError::custom(format!("can't send the message to channel: {err}")))?;

        let reception_type = wait_ack.map_or(
            TopicWriterReceptionType::NoConfirmationExpected,
            TopicWriterReceptionType::AwaitingConfirmation,
        );

        {
            // bracket needs for release mutex as soon as possible - before await
            let mut reception_queue = self.confirmation_reception_queue.lock().unwrap();
            reception_queue.add_ticket(TopicWriterReceptionTicket::new(
                message_seqno,
                reception_type,
            ));
        }

        Ok(())
    }

    pub async fn flush(&self) -> YdbResult<()> {
        self.is_cancelled().await?;

        let flush_op_completed = {
            let mut reception_queue = self.confirmation_reception_queue.lock().unwrap();
            reception_queue.init_flush_op()?
        };

        Ok(flush_op_completed.await?)
    }

    async fn is_cancelled(&self) -> YdbResult<()> {
        let state = self.writer_state.lock().unwrap();
        match state.deref() {
            TopicWriterState::Working => Ok(()),
            TopicWriterState::FinishedWithError(err) => Err(err.clone()),
        }
    }

    pub async fn stop(self) -> YdbResult<()> {
        trace!("Stopping...");

        match timeout(self.flush_timeout, self.flush()).await {
            Ok(result) => result?,
            Err(_) => {
                return Err(YdbError::custom(
                    "flush timed out while stopping topic writer",
                ))
            }
        }
        self.cancellation_token.cancel();

        self.reconnection_loop.await.map_err(|err| {
            YdbError::custom(format!(
                "stop: error while waiting for reconnection_loop to finish: {err}"
            ))
        })?; // TODO: handle error
        trace!("Reconnection loop stopped");

        Ok(())
    }
}

/// A supervisor for the write loop and the receive messages loop.
/// Reports when it wants to reconnect through the want_reconnect_tx channel.
struct WriteSupervisor {
    writer_loop: JoinHandle<()>,
    receive_messages_loop: JoinHandle<()>,
    cancellation_token: CancellationToken,
}

struct WriteSupervisorParams {
    writer_options: TopicWriterOptions,
    producer_id: String,
    messages: Arc<TokioMutex<Vec<MessageData>>>,
}

struct WriterPeriodicTaskParams {
    write_request_messages_chunk_size: usize,
    write_request_send_messages_period: Duration,
    producer_id: Option<String>,
    request_stream: mpsc::UnboundedSender<stream_write_message::FromClient>,
}

impl WriteSupervisor {
    pub async fn new(
        params: WriteSupervisorParams,
        connection_manager: GrpcConnectionManager,
        connection_info: Arc<TokioMutex<ConnectionInfo>>,
        confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,
        writer_state: Arc<Mutex<TopicWriterState>>,
        messages_receiver: mpsc::Receiver<TopicWriterMessage>,
        want_reconnect_tx: oneshot::Sender<YdbError>,
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
            .get_auth_service(grpc_wrapper::raw_topic_service::client::RawTopicClient::new)
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

        let writer_loop_cancellation_token = cancellation_token.clone();
        let writer_loop_writer_state = writer_state.clone();

        let message_receive_loop_cancellation_token = cancellation_token.clone();
        let message_receive_loop_writer_state = writer_state.clone();
        let message_loop_reception_queue = confirmation_reception_queue.clone();

        let writer_loop_task_params = WriterPeriodicTaskParams {
            write_request_messages_chunk_size: params
                .writer_options
                .write_request_messages_chunk_size,
            write_request_send_messages_period: params
                .writer_options
                .write_request_send_messages_period,
            producer_id: Some(params.producer_id.clone()),
            request_stream: stream.clone_sender(),
        };

        let writer_loop_messages = params.messages.clone();

        let writer_loop = tokio::spawn(async move {
            let mut message_receiver = messages_receiver; // force move inside
            let task_params = writer_loop_task_params; // force move inside
            let mut want_reconnect_tx = Some(want_reconnect_tx); // force move inside + wrap in Option

            loop {
                if writer_loop_cancellation_token.is_cancelled() {
                    break;
                }

                let Err(writer_iteration_error) = WriteSupervisor::write_loop_iteration(
                    &writer_loop_messages,
                    message_receiver.borrow_mut(),
                    task_params.borrow(),
                )
                .await
                else {
                    continue;
                };

                writer_loop_cancellation_token.cancel();
                let mut writer_state = writer_loop_writer_state.lock().unwrap(); // TODO: handle error

                *writer_state = TopicWriterState::FinishedWithError(writer_iteration_error.clone());

                let Some(tx) = want_reconnect_tx.take() else {
                    break;
                };

                if let Err(err) = tx.send(writer_iteration_error.clone()) {
                    *writer_state = TopicWriterState::FinishedWithError(
                        YdbError::custom(format!("can't send error to supervisor: {err} (original error: {writer_iteration_error})"))
                    );
                }
            }
        });

        let receive_messages_loop = tokio::spawn(async move {
            let mut stream = stream; // force move inside
            let mut reception_queue = message_loop_reception_queue; // force move inside

            loop {
                tokio::select! {
                    _ = message_receive_loop_cancellation_token.cancelled() => { return ; }
                    message_receive_it_res = async {
                        WriteSupervisor::receive_messages_loop_iteration(
                            stream.borrow_mut(),
                            reception_queue.borrow_mut()
                        ).await
                    } => {
                        match message_receive_it_res {
                            Ok(_) => {}
                            Err(receive_message_iteration_error) => {
                                message_receive_loop_cancellation_token.cancel();
                                warn!("error receive message for topic writer receiver stream loop: {}", &receive_message_iteration_error);
                                let mut writer_state =
                                    message_receive_loop_writer_state.lock().unwrap(); // TODO handle error
                                *writer_state =
                                    TopicWriterState::FinishedWithError(receive_message_iteration_error);
                                return ;
                            }
                        }
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
        messages: &Arc<TokioMutex<Vec<MessageData>>>,
        messages_receiver: &mut Receiver<TopicWriterMessage>,
        task_params: &WriterPeriodicTaskParams,
    ) -> YdbResult<()> {
        let start = Instant::now();

        // wait for messages loop
        'messages_loop: loop {
            let elapsed = start.elapsed();
            let (messages_len, messages_is_empty) = {
                let messages_guard = messages.lock().await;
                let len = messages_guard.len();
                (len, len == 0)
            };

            if messages_len >= task_params.write_request_messages_chunk_size
                || (!messages_is_empty && elapsed >= task_params.write_request_send_messages_period)
            {
                break;
            }

            match timeout(
                task_params.write_request_send_messages_period - elapsed,
                messages_receiver.recv(),
            )
            .await
            {
                Ok(Some(message)) => {
                    let data_size = message.data.len() as i64;
                    let mut messages_guard = messages.lock().await;
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
