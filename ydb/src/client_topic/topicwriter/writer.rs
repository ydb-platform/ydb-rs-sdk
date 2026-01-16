use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::message_write_status::{MessageWriteStatus, WriteAck};
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::client_topic::topicwriter::writer_reception_queue::{
    TopicWriterReceptionQueue, TopicWriterReceptionTicket, TopicWriterReceptionType,
};
use crate::grpc_connection_manager::GrpcConnectionManager;

use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_topic_service::common::codecs::RawSupportedCodecs;
use crate::grpc_wrapper::raw_topic_service::stream_write::init::RawInitResponse;
use crate::grpc_wrapper::raw_topic_service::stream_write::RawServerMessage;
use crate::retry::{Retry, RetryParams};
use crate::{grpc_wrapper, YdbError, YdbResult};
use std::borrow::{Borrow, BorrowMut};

use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::Mutex as TokioMutex;

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::log::trace;
use tracing::warn;
use ydb_grpc::ydb_proto::topic::stream_write_message;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::init_request::Partitioning;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::{message_data, MessageData};
use ydb_grpc::ydb_proto::topic::stream_write_message::{InitRequest, WriteRequest};

pub(crate) enum TopicWriterState {
    Working,
    FinishedWithError(YdbError),
}

/// TopicWriter at initial state of implementation
/// it really doesn't ready for use. For example
/// It isn't handle lost connection to the server and have some unimplemented method.
#[allow(dead_code)]
pub struct TopicWriter {
    pub(crate) path: String,
    pub(crate) producer_id: Option<String>,
    pub(crate) write_request_messages_chunk_size: usize,
    pub(crate) write_request_send_messages_period: Duration,

    pub(crate) auto_set_seq_no: bool,
    pub(crate) init_state: Arc<TokioMutex<ConnectionInfo>>,

    writer_message_sender: Arc<TokioMutex<mpsc::Sender<TopicWriterMessage>>>,

    cancellation_token: CancellationToken,
    writer_state: Arc<Mutex<TopicWriterState>>,

    confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,

    reconnector_loop: JoinHandle<()>,
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

struct WriterPeriodicTaskParams {
    write_request_messages_chunk_size: usize,
    write_request_send_messages_period: Duration,
    producer_id: Option<String>,
    stream: Arc<
        TokioMutex<
            AsyncGrpcStreamWrapper<
                stream_write_message::FromClient,
                stream_write_message::FromServer,
            >,
        >,
    >,
    // TODO: ???
    connection_manager: GrpcConnectionManager,
    // TODO: ???
    init_request: InitRequest,
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectionInfo {
    partition_id: i64,
    session_id: String,
    last_seq_num_handled: i64,
    codecs_from_server: RawSupportedCodecs,
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

        let (initial_messages_sender, initial_messages_receiver): (
            mpsc::Sender<TopicWriterMessage>,
            mpsc::Receiver<TopicWriterMessage>,
        ) = mpsc::channel(32_usize);
        let writer_message_sender = Arc::new(TokioMutex::new(initial_messages_sender));
        let reconnector_loop_writer_message_sender = writer_message_sender.clone();

        let confirmation_reception_queue = Arc::new(Mutex::new(TopicWriterReceptionQueue::new()));
        let reconnector_loop_confirmation_reception_queue = confirmation_reception_queue.clone();
        let connection_info = Arc::new(TokioMutex::new(ConnectionInfo {
            partition_id: 0,
            session_id: String::new(),
            last_seq_num_handled: 0,
            codecs_from_server: RawSupportedCodecs::default(),
        }));
        let reconnector_loop_connection_info = connection_info.clone();
        let (connection_info_filled_tx, connection_info_filled_rx) = oneshot::channel::<YdbResult<()>>();
        

        let reconnector_loop_writer_options = writer_options.clone();
        let reconnector_loop_producer_id = producer_id.clone();
        let reconnector_loop_writer_state = writer_state.clone();
        let reconnector_loop_cancellation_token = cancellation_token.clone();

        let reconnector_loop = tokio::spawn(async move {
            let connection_info = reconnector_loop_connection_info;
            let mut messages_receiver = initial_messages_receiver;
            let connection_manager = connection_manager;
            let mut connection_info_filled_tx = Some(connection_info_filled_tx);
            let messages = Arc::new(TokioMutex::new(Vec::<MessageData>::new()));

            loop {
                // TODO: rename?
                let (want_reconnect_sender, want_reconnect_receiver): (
                    oneshot::Sender<YdbError>,
                    oneshot::Receiver<YdbError>,
                ) = oneshot::channel();

                let reconnector = match Reconnector::new(
                    reconnector_loop_writer_options.clone(),
                    connection_manager.clone(),
                    reconnector_loop_producer_id.clone(),
                    reconnector_loop_confirmation_reception_queue.clone(),
                    messages_receiver,
                    want_reconnect_sender,
                    reconnector_loop_writer_state.clone(),
                    connection_info.clone(),
                    messages.clone(),
                )
                .await
                {
                    Ok(reconnector) => reconnector,
                    Err(err) => {
                        println!("Error creating reconnector: {}", err);
                        messages_receiver = TopicWriter::recreate_message_channel(&reconnector_loop_writer_message_sender).await;
                        continue;
                    }
                };

                if let Some(tx) = connection_info_filled_tx.take() {
                    let _ = tx.send(Ok(()));
                };

                // TODO: check if retry is needed (function is somewhere in src/client_table.rs)
                tokio::select! {
                    _ = reconnector_loop_cancellation_token.cancelled() => {
                        let _ = reconnector.stop().await;
                        break;
                    }
                    err = want_reconnect_receiver => {
                        match err {
                            Ok(err) => {
                                println!("Error, trying to reconnect: {}", err);
                            }
                            Err(chan_err) => {
                                println!("Channel error: {}", chan_err);
                            }
                        }
                    }
                }

                messages_receiver = TopicWriter::recreate_message_channel(&reconnector_loop_writer_message_sender).await;

            }
        });

        match connection_info_filled_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err),
            Err(_) => return Err(YdbError::custom("connection info filled channel closed")),
        };

        Ok(Self {
            path: writer_options.topic_path.clone(),
            producer_id: Some(producer_id.clone()),
            write_request_messages_chunk_size: writer_options.write_request_messages_chunk_size,
            write_request_send_messages_period: writer_options.write_request_send_messages_period,
            auto_set_seq_no: writer_options.auto_seq_no,
            init_state: connection_info,
            writer_message_sender,
            cancellation_token,
            writer_state,
            confirmation_reception_queue,
            reconnector_loop,
        })
    }

    async fn recreate_message_channel(
        writer_message_sender: &Arc<TokioMutex<mpsc::Sender<TopicWriterMessage>>>,
    ) -> mpsc::Receiver<TopicWriterMessage> {
        let (new_messages_sender, new_messages_receiver): (
            mpsc::Sender<TopicWriterMessage>,
            mpsc::Receiver<TopicWriterMessage>,
        ) = mpsc::channel(32_usize);
        {
            let mut sender_guard = writer_message_sender.lock().await;
            *sender_guard = new_messages_sender;
        }
        new_messages_receiver
    }

    // TODO: nuke this method
    async fn retry_send_messages(
        retrier: &Arc<Box<dyn Retry>>,
        task_params: &WriterPeriodicTaskParams,
        messages: Vec<MessageData>,
    ) -> YdbResult<()> {
        let mut attempt: usize = 0;
        let start = Instant::now();
        let mut reconnection_err: Option<YdbError> = None;

        loop {
            attempt += 1;

            let last_err = match reconnection_err {
                Some(err) => err,
                None => {
                    let send_result = {
                        let mut stream = task_params.stream.lock().await;
                        stream.send_nowait(stream_write_message::FromClient {
                            client_message: Some(ClientMessage::WriteRequest(WriteRequest {
                                messages: messages.to_owned(),
                                codec: 1,
                                tx: None,
                            })),
                        })
                    };

                    match send_result {
                        Ok(_) => return Ok(()),
                        Err(err) => YdbError::Transport(err.to_string()),
                    }
                }
            };

            let now = std::time::Instant::now();
            let retry_decision = retrier.wait_duration(RetryParams {
                attempt,
                time_from_start: now.duration_since(start),
            });
            if !retry_decision.allow_retry {
                return Err(last_err);
            }
            tokio::time::sleep(retry_decision.wait_timeout).await;

            match TopicWriter::reconnect(task_params).await {
                Ok(_) => {
                    trace!("Reconnect is successful, retrying send");
                    reconnection_err = None;
                }
                Err(err) => {
                    warn!("Reconnect has failed: {}", err);
                    reconnection_err = Some(err);
                }
            }
        }
    }

    async fn reconnect(task_params: &WriterPeriodicTaskParams) -> YdbResult<()> {
        let mut topic_service = task_params
            .connection_manager
            .get_auth_service(grpc_wrapper::raw_topic_service::client::RawTopicClient::new)
            .await?;

        let mut stream = topic_service
            .stream_write(task_params.init_request.clone())
            .await?;

        let _init_response =
            RawInitResponse::try_from(stream.receive::<RawServerMessage>().await?)?;

        // Update the shared stream so both write and receive loops use the new connection
        let mut shared_stream = task_params.stream.lock().await;
        *shared_stream = stream;

        Ok(())
    }

    pub async fn write(&mut self, message: TopicWriterMessage) -> YdbResult<()> {
        self.write_message(message, None).await?;
        Ok(())
    }

    pub async fn write_with_ack(
        &mut self,
        message: TopicWriterMessage,
    ) -> YdbResult<MessageWriteStatus> {
        let (tx, rx): (
            oneshot::Sender<MessageWriteStatus>,
            oneshot::Receiver<MessageWriteStatus>,
        ) = oneshot::channel();

        self.write_message(message, Some(tx)).await?;
        Ok(rx.await?)
    }

    pub async fn write_with_ack_future(
        &mut self,
        _message: TopicWriterMessage,
    ) -> YdbResult<AckFuture> {
        let (tx, rx): (
            oneshot::Sender<MessageWriteStatus>,
            oneshot::Receiver<MessageWriteStatus>,
        ) = oneshot::channel();

        self.write_message(_message, Some(tx)).await?;
        Ok(AckFuture { receiver: rx })
    }

    async fn write_message(
        &mut self,
        mut message: TopicWriterMessage,
        wait_ack: Option<oneshot::Sender<MessageWriteStatus>>,
    ) -> YdbResult<()> {
        self.is_cancelled().await?;

        let mut init_state = self.init_state.lock().await;
        if self.auto_set_seq_no {
            if message.seq_no.is_some() {
                return Err(YdbError::custom(
                    "force set message seqno possible only if auto_set_seq_no disabled",
                ));
            }
            message.seq_no = Some(init_state.last_seq_num_handled + 1);
        };

        let message_seqno = if let Some(mess_seqno) = message.seq_no {
            init_state.last_seq_num_handled = mess_seqno;
            mess_seqno
        } else {
            return Err(YdbError::custom("need to set message seq_no"));
        };

        self.writer_message_sender
            .lock()
            .await
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

        self.flush().await?;
        self.cancellation_token.cancel();

        self.reconnector_loop.await.map_err(|err| {
            YdbError::custom(format!(
                "stop: error while waiting for reconnector_loop to finish: {err}"
            ))
        })?; // TODO: handle error
        trace!("Reconnector loop stopped");

        Ok(())
    }
}

struct Reconnector {
    writer_loop: JoinHandle<()>,
    receive_messages_loop: JoinHandle<()>,
    cancellation_token: CancellationToken,
}

impl Reconnector {
    pub async fn new(
        writer_options: TopicWriterOptions,
        connection_manager: GrpcConnectionManager,
        producer_id: String,
        confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,
        messages_receiver: mpsc::Receiver<TopicWriterMessage>,
        want_reconnect_tx: oneshot::Sender<YdbError>,
        topic_writer_state: Arc<Mutex<TopicWriterState>>,
        init_state: Arc<TokioMutex<ConnectionInfo>>,
        messages: Arc<TokioMutex<Vec<MessageData>>>,
    ) -> YdbResult<Self> {
        let init_request_body = InitRequest {
            path: writer_options.topic_path.clone(),
            producer_id: producer_id.clone(),
            write_session_meta: writer_options.session_metadata.clone().unwrap_or_default(),
            get_last_seq_no: writer_options.auto_seq_no,
            partitioning: Some(Partitioning::MessageGroupId(producer_id.clone())),
        };

        let mut topic_service = connection_manager
            .get_auth_service(grpc_wrapper::raw_topic_service::client::RawTopicClient::new)
            .await?;

        let mut stream = topic_service
            .stream_write(init_request_body.clone())
            .await?;
        let init_response =
            RawInitResponse::try_from(stream.receive::<RawServerMessage>().await?)?;
        {
            let mut guard = init_state.lock().await;
            guard.partition_id = init_response.partition_id;
            guard.session_id = init_response.session_id;
            guard.last_seq_num_handled = init_response.last_seq_no;
            guard.codecs_from_server = init_response.supported_codecs;
        }

        let cancellation_token = CancellationToken::new();

        let writer_loop_cancellation_token = cancellation_token.clone();
        let writer_state_ref_writer_loop = topic_writer_state.clone();

        let message_receive_loop_cancellation_token = cancellation_token.clone();
        let writer_state_ref_message_receive_loop = topic_writer_state.clone();
        let message_loop_reception_queue = confirmation_reception_queue.clone();

        let shared_stream = Arc::new(TokioMutex::new(stream));

        let writer_loop_task_params = WriterPeriodicTaskParams {
            write_request_messages_chunk_size: writer_options.write_request_messages_chunk_size,
            write_request_send_messages_period: writer_options.write_request_send_messages_period,
            producer_id: Some(producer_id.clone()),
            stream: shared_stream.clone(),
            connection_manager: connection_manager.clone(),
            init_request: init_request_body,
        };

        let writer_loop_messages = messages.clone();

        let writer_loop = tokio::spawn(async move {
            let mut message_receiver = messages_receiver; // force move inside
            let task_params = writer_loop_task_params; // force move inside
            let mut want_reconnect_tx = Some(want_reconnect_tx); // force move inside + wrap in Option

            loop {
                if writer_loop_cancellation_token.is_cancelled() {
                    break;
                }

                let Err(writer_iteration_error) = Reconnector::write_loop_iteration(
                    &writer_loop_messages,
                    message_receiver.borrow_mut(),
                    task_params.borrow(),
                )
                .await
                else {
                    continue;
                };

                writer_loop_cancellation_token.cancel();
                let mut writer_state = writer_state_ref_writer_loop.lock().unwrap(); // TODO: handle error

                *writer_state = TopicWriterState::FinishedWithError(writer_iteration_error.clone());

                let Some(tx) = want_reconnect_tx.take() else {
                    continue;
                };

                if let Err(err) = tx.send(writer_iteration_error.clone()) {
                    *writer_state = TopicWriterState::FinishedWithError(
                        YdbError::custom(format!("can't send error to reconnector: {err} (original error: {writer_iteration_error})"))
                    );
                }
            }
        });

        let receive_messages_loop_stream = shared_stream.clone();
        let receive_messages_loop = tokio::spawn(async move {
            let mut reception_queue = message_loop_reception_queue; // force move inside

            loop {
                tokio::select! {
                    _ = message_receive_loop_cancellation_token.cancelled() => { return ; }
                    message_receive_it_res = async {
                        let mut stream = receive_messages_loop_stream.lock().await;
                        Reconnector::receive_messages_loop_iteration(
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
                                    writer_state_ref_message_receive_loop.lock().unwrap(); // TODO handle error
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
            let messages_len = {
                let messages_guard = messages.lock().await;
                messages_guard.len()
            };
            let messages_is_empty = messages_len == 0;

            if messages_len >= task_params.write_request_messages_chunk_size
                || !messages_is_empty && elapsed >= task_params.write_request_send_messages_period
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
            // - If write fails, then we put these unwirtten messages back WITH taking into account messages that might've been written into messages.
            //
            // The "clone, clear if success" approach is dangerous because we can lose messages that are appended in another coroutine.
            messages_guard.drain(..).collect::<Vec<MessageData>>()
        };

        if messages_to_send.is_empty() {
            return Ok(());
        }

        trace!("Sending topic message to grpc stream");
        let mut stream = task_params.stream.lock().await;
        let send_result = stream.send_nowait(stream_write_message::FromClient {
            client_message: Some(ClientMessage::WriteRequest(WriteRequest {
                messages: messages_to_send.clone(),
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
                let mut failed = messages_to_send;
                failed.append(&mut messages_guard.drain(..).collect());
                *messages_guard = failed;

                let err_message = err.borrow().to_string();
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
