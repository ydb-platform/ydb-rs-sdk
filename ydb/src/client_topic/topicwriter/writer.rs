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

pub(crate) enum TopicWriterMode {
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
    pub(crate) partition_id: i64,
    pub(crate) session_id: String,
    pub(crate) last_seq_num_handled: i64,
    pub(crate) write_request_messages_chunk_size: usize,
    pub(crate) write_request_send_messages_period: Duration,

    pub(crate) auto_set_seq_no: bool,
    pub(crate) codecs_from_server: RawSupportedCodecs,

    writer_message_sender: mpsc::Sender<TopicWriterMessage>,
    writer_loop: JoinHandle<()>,
    receive_messages_loop: JoinHandle<()>,

    cancellation_token: CancellationToken,
    writer_state: Arc<Mutex<TopicWriterMode>>,

    confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,

    pub(crate) connection_manager: GrpcConnectionManager,
}

#[allow(dead_code)]
pub struct AckFuture {
    receiver: tokio::sync::oneshot::Receiver<MessageWriteStatus>,
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
    request_stream: mpsc::UnboundedSender<stream_write_message::FromClient>,
}

impl TopicWriter {
    pub(crate) async fn new(
        writer_options: TopicWriterOptions,
        connection_manager: GrpcConnectionManager,
    ) -> YdbResult<Self> {
        //TODO: split to smaller functions

        let mut topic_service = connection_manager
            .get_auth_service(grpc_wrapper::raw_topic_service::client::RawTopicClient::new)
            .await?;

        let producer_id = if let Some(id) = writer_options.producer_id {
            id
        } else {
            uuid::Uuid::new_v4().to_string()
        };

        let init_request_body = InitRequest {
            path: writer_options.topic_path.clone(),
            producer_id: producer_id.clone(),
            write_session_meta: writer_options.session_metadata.clone().unwrap_or_default(),
            get_last_seq_no: writer_options.auto_seq_no,
            partitioning: Some(Partitioning::MessageGroupId(producer_id.clone())),
        };

        let mut stream = topic_service.stream_write(init_request_body).await?;
        let init_response = RawInitResponse::try_from(stream.receive::<RawServerMessage>().await?)?;

        let (messages_sender, messages_receiver): (
            mpsc::Sender<TopicWriterMessage>,
            mpsc::Receiver<TopicWriterMessage>,
        ) = mpsc::channel(32_usize);
        let cancellation_token = CancellationToken::new();
        let topic_writer_state = Arc::new(Mutex::new(TopicWriterMode::Working));
        let confirmation_reception_queue = Arc::new(Mutex::new(TopicWriterReceptionQueue::new()));

        let writer_loop_cancellation_token = cancellation_token.clone();
        let writer_state_ref_writer_loop = topic_writer_state.clone();

        let message_receive_loop_cancellation_token = cancellation_token.clone();
        let writer_state_ref_message_receive_loop = topic_writer_state.clone();
        let message_loop_reception_queue = confirmation_reception_queue.clone();

        let writer_loop_task_params = WriterPeriodicTaskParams {
            write_request_messages_chunk_size: writer_options.write_request_messages_chunk_size,
            write_request_send_messages_period: writer_options.write_request_send_messages_period,
            producer_id: Some(producer_id.clone()),
            request_stream: stream.clone_sender(),
        };
        let writer_loop = tokio::spawn(async move {
            let mut message_receiver = messages_receiver; // force move inside
            let task_params = writer_loop_task_params; // force move inside

            loop {
                match TopicWriter::write_loop_iteration(
                    message_receiver.borrow_mut(),
                    task_params.borrow(),
                )
                .await
                {
                    Ok(()) => {}
                    Err(writer_iteration_error) => {
                        writer_loop_cancellation_token.cancel();
                        let mut writer_state = writer_state_ref_writer_loop.lock().unwrap(); // TODO handle error
                        *writer_state = TopicWriterMode::FinishedWithError(writer_iteration_error);
                        return;
                    }
                }
                if writer_loop_cancellation_token.is_cancelled() {
                    break;
                }
            }
        });
        let receive_messages_loop = tokio::spawn(async move {
            let mut stream = stream; // force move inside
            let mut reception_queue = message_loop_reception_queue; // force move inside

            loop {
                tokio::select! {
                    _ = message_receive_loop_cancellation_token.cancelled() => { return ; }
                    message_receive_it_res = TopicWriter::receive_messages_loop_iteration(
                                                          stream.borrow_mut(),
                                                          reception_queue.borrow_mut()) => {
                        match message_receive_it_res {
                            Ok(_) => {}
                            Err(receive_message_iteration_error) => {
                                message_receive_loop_cancellation_token.cancel();
                                warn!("error receive message for topic writer receiver stream loop: {}", &receive_message_iteration_error);
                                let mut writer_state =
                                    writer_state_ref_message_receive_loop.lock().unwrap(); // TODO handle error
                                *writer_state =
                                    TopicWriterMode::FinishedWithError(receive_message_iteration_error);
                                return ;
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            path: writer_options.topic_path.clone(),
            producer_id: Some(producer_id.clone()),
            partition_id: init_response.partition_id,
            session_id: init_response.session_id,
            last_seq_num_handled: init_response.last_seq_no,
            write_request_messages_chunk_size: writer_options.write_request_messages_chunk_size,
            write_request_send_messages_period: writer_options.write_request_send_messages_period,
            auto_set_seq_no: writer_options.auto_seq_no,
            codecs_from_server: init_response.supported_codecs,
            writer_message_sender: messages_sender,
            writer_loop,
            receive_messages_loop,
            cancellation_token,
            writer_state: topic_writer_state,
            confirmation_reception_queue,
            connection_manager,
        })
    }

    async fn write_loop_iteration(
        messages_receiver: &mut Receiver<TopicWriterMessage>,
        task_params: &WriterPeriodicTaskParams,
    ) -> YdbResult<()> {
        let start = Instant::now();
        let mut messages = vec![];

        // wait messages loop
        'messages_loop: loop {
            let elapsed = start.elapsed();
            if messages.len() >= task_params.write_request_messages_chunk_size
                || !messages.is_empty() && elapsed >= task_params.write_request_send_messages_period
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
                    messages.push(MessageData {
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

        if !messages.is_empty() {
            trace!("Sending topic message to grpc stream...");
            task_params
                .request_stream
                .send(stream_write_message::FromClient {
                    client_message: Some(ClientMessage::WriteRequest(WriteRequest {
                        messages,
                        codec: 1,
                        tx: None,
                    })),
                })
                .unwrap(); // TODO: HANDLE ERROR
        }
        Ok(())
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

        self.flush().await?;
        self.cancellation_token.cancel();

        self.writer_loop.await.map_err(|err| {
            YdbError::custom(format!(
                "error while wait finish writer_loop on stop: {}",
                err
            ))
        })?; // TODO: handle ERROR
        trace!("Writer loop stopped");

        self.receive_messages_loop.await.map_err(|err| {
            YdbError::custom(format!(
                "error while wait finish receive_messages_loop on stop: {}",
                err
            ))
        })?; // TODO: handle ERROR
        trace!("Message receive stopped");
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
            tokio::sync::oneshot::Sender<MessageWriteStatus>,
            tokio::sync::oneshot::Receiver<MessageWriteStatus>,
        ) = tokio::sync::oneshot::channel();

        self.write_message(message, Some(tx)).await?;
        Ok(rx.await?)
    }

    pub async fn write_with_ack_future(
        &mut self,
        _message: TopicWriterMessage,
    ) -> YdbResult<AckFuture> {
        let (tx, rx): (
            tokio::sync::oneshot::Sender<MessageWriteStatus>,
            tokio::sync::oneshot::Receiver<MessageWriteStatus>,
        ) = tokio::sync::oneshot::channel();

        self.write_message(_message, Some(tx)).await?;
        Ok(AckFuture { receiver: rx })
    }

    async fn write_message(
        &mut self,
        mut message: TopicWriterMessage,
        wait_ack: Option<tokio::sync::oneshot::Sender<MessageWriteStatus>>,
    ) -> YdbResult<()> {
        self.is_cancelled().await?;

        if self.auto_set_seq_no {
            if message.seq_no.is_some() {
                return Err(YdbError::custom(
                    "force set message seqno possible only if auto_set_seq_no disabled",
                ));
            }
            message.seq_no = Some(self.last_seq_num_handled + 1);
        };

        let message_seqno = if let Some(mess_seqno) = message.seq_no {
            self.last_seq_num_handled = mess_seqno;
            mess_seqno
        } else {
            return Err(YdbError::custom("need to set message seq_no"));
        };

        self.writer_message_sender
            .borrow_mut()
            .send(message)
            .await
            .map_err(|err| {
                YdbError::custom(format!("can't send the message to channel: {}", err))
            })?;

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
            TopicWriterMode::Working => Ok(()),
            TopicWriterMode::FinishedWithError(err) => Err(err.clone()),
        }
    }
}
