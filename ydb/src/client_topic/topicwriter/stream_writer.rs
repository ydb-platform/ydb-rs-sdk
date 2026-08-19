use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::trace;

use ydb_grpc::ydb_proto::topic::stream_write_message;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::client_topic::compression::{
    CodecRegistry, CompressedChunk, CompressionWorker, Executor,
};
use crate::client_topic::list_types::Codec;
use crate::client_topic::task_supervisor::wait_child_tasks;
use crate::client_topic::topicwriter::message_write_status::WriteAck;
use crate::client_topic::topicwriter::queue::WriterRuntime;
use crate::client_topic::topicwriter::write_request::{
    PendingWriteRequest, TryAddMessage, WriteRequestSettings,
};
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_topic_service::stream_write::RawServerMessage;
use crate::{YdbError, YdbResult};

/// Manages the gRPC stream communications: write loop and receive-messages loop.
pub(crate) struct StreamWriter {
    supervisor: Option<JoinHandle<YdbResult<()>>>,
    cancellation_token: CancellationToken,
}

impl StreamWriter {
    pub(super) async fn new(
        writer_options: TopicWriterOptions,
        stream: AsyncGrpcStreamWrapper<
            stream_write_message::FromClient,
            stream_write_message::FromServer,
        >,
        runtime: WriterRuntime,
        server_codecs: Vec<Codec>,
        executor: Arc<dyn Executor>,
        write_request_settings: WriteRequestSettings,
    ) -> YdbResult<Self> {
        let cancellation_token = CancellationToken::new();

        let mut codec_registry = CodecRegistry::new();
        for enc in &writer_options.extra_encoders {
            codec_registry.register_encoder(enc.clone());
        }

        let worker = CompressionWorker::new(
            writer_options.codec_selector,
            Arc::new(codec_registry),
            executor,
            server_codecs,
        )?;

        let (batch_tx, batch_rx) = mpsc::unbounded_channel::<Vec<MessageData>>();
        let (compressed_tx, compressed_rx) =
            mpsc::unbounded_channel::<YdbResult<CompressedChunk>>();

        let request_stream = stream.clone_sender();

        let mut tasks = JoinSet::new();

        tasks.spawn(StreamWriter::write_messages_loop(
            cancellation_token.clone(),
            runtime.clone(),
            batch_tx,
        ));

        worker.spawn_into(&mut tasks, batch_rx, compressed_tx);

        tasks.spawn(StreamWriter::grpc_send_loop(
            cancellation_token.clone(),
            compressed_rx,
            request_stream,
            write_request_settings,
        ));

        tasks.spawn(StreamWriter::receive_messages_loop(
            cancellation_token.clone(),
            runtime,
            stream,
        ));

        let supervisor_cancellation = cancellation_token.clone();
        let supervisor = tokio::spawn(async move {
            wait_child_tasks(&supervisor_cancellation, tasks, "topic writer connection").await
        });

        Ok(Self {
            supervisor: Some(supervisor),
            cancellation_token,
        })
    }

    async fn write_messages_loop(
        cancellation_token: CancellationToken,
        runtime: WriterRuntime,
        batch_tx: mpsc::UnboundedSender<Vec<MessageData>>,
    ) -> YdbResult<()> {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => return Ok(()),
                result = runtime.get_messages_to_send() => {
                    let messages = result?;
                    if messages.is_empty() {
                        continue;
                    }
                    batch_tx
                        .send(messages)
                        .map_err(|_| YdbError::custom("compression worker input channel closed"))?;
                }
            }
        }
    }

    async fn grpc_send_loop(
        cancellation_token: CancellationToken,
        mut compressed_rx: mpsc::UnboundedReceiver<YdbResult<CompressedChunk>>,
        request_stream: mpsc::UnboundedSender<stream_write_message::FromClient>,
        write_request_settings: WriteRequestSettings,
    ) -> YdbResult<()> {
        let mut pending_request = None;

        loop {
            tokio::select! {
                biased;
                _ = cancellation_token.cancelled() => return Ok(()),
                next = compressed_rx.recv() => {
                    let chunk = next
                        .ok_or_else(|| YdbError::custom("compression worker output channel closed"))??;
                    StreamWriter::send_compressed_chunk(
                        &request_stream,
                        &write_request_settings,
                        &mut pending_request,
                        chunk,
                    )?;
                }
            }
        }
    }

    fn send_compressed_chunk(
        request_stream: &mpsc::UnboundedSender<stream_write_message::FromClient>,
        settings: &WriteRequestSettings,
        pending_request: &mut Option<PendingWriteRequest>,
        chunk: CompressedChunk,
    ) -> YdbResult<()> {
        let CompressedChunk {
            messages,
            codec,
            ends_batch,
        } = chunk;

        if let Some(request) = pending_request.as_ref()
            && request.codec() != codec
        {
            return Err(YdbError::custom(format!(
                "compression codec changed before the topic write batch ended: previous_codec={}, next_codec={}",
                request.codec().code,
                codec.code,
            )));
        }

        for message in messages {
            match pending_request.take() {
                None => {
                    *pending_request = Some(PendingWriteRequest::new(settings, codec, message)?);
                }
                Some(mut request) => match request.try_add(message) {
                    TryAddMessage::Added => {
                        *pending_request = Some(request);
                    }
                    TryAddMessage::RequestFull(message) => {
                        StreamWriter::send_write_request(request_stream, request)?;
                        *pending_request =
                            Some(PendingWriteRequest::new(settings, codec, message)?);
                    }
                },
            }
        }

        if ends_batch {
            let Some(request) = pending_request.take() else {
                return Err(YdbError::custom(
                    "compressed topic write batch ended without messages",
                ));
            };
            StreamWriter::send_write_request(request_stream, request)?;
        }

        Ok(())
    }

    fn send_write_request(
        request_stream: &mpsc::UnboundedSender<stream_write_message::FromClient>,
        request: PendingWriteRequest,
    ) -> YdbResult<()> {
        trace!("sending topic message to grpc stream");
        request_stream
            .send(request.into_grpc_message()?)
            .map_err(|err| YdbError::Transport(err.to_string()))
    }

    async fn receive_messages_loop(
        cancellation_token: CancellationToken,
        runtime: WriterRuntime,
        mut stream: AsyncGrpcStreamWrapper<
            stream_write_message::FromClient,
            stream_write_message::FromServer,
        >,
    ) -> YdbResult<()> {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => return Ok(()),
                result = StreamWriter::receive_messages_loop_iteration(
                    &runtime,
                    &mut stream,
                ) => {
                    result?;
                }
            }
        }
    }

    async fn receive_messages_loop_iteration(
        runtime: &WriterRuntime,
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
                        runtime.acknowledge_message(write_ack)?;
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

    pub(crate) async fn wait(&mut self) -> YdbResult<()> {
        let joined = match self.supervisor.as_mut() {
            Some(supervisor) => supervisor.await,
            None => {
                return Err(YdbError::custom(
                    "topic writer connection was already awaited",
                ));
            }
        };
        self.supervisor = None;
        joined.map_err(|err| YdbError::custom(format!("topic writer supervisor failed: {err}")))?
    }

    pub(crate) async fn stop(mut self) -> YdbResult<()> {
        trace!("stopping...");

        self.cancellation_token.cancel();

        if let Some(supervisor) = self.supervisor.take() {
            supervisor.await.map_err(|err| {
                YdbError::custom(format!("topic writer supervisor failed: {err}"))
            })??;
        }

        trace!("stream writer stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use tokio::sync::mpsc::error::TryRecvError;
    use ydb_grpc::ydb_proto::topic::stream_write_message::WriteRequest;
    use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage;

    use super::*;
    use crate::client_topic::topicwriter::write_request::WRITE_REQUEST_SIZE_RESERVE_BYTES;

    fn message(seq_no: i64, data_size: usize) -> MessageData {
        MessageData {
            seq_no,
            data: vec![0; data_size],
            ..Default::default()
        }
    }

    fn chunk(messages: Vec<MessageData>, ends_batch: bool) -> CompressedChunk {
        CompressedChunk {
            messages,
            codec: Codec::RAW,
            ends_batch,
        }
    }

    fn settings(max_write_request_size: usize) -> WriteRequestSettings {
        WriteRequestSettings::new(
            None,
            WRITE_REQUEST_SIZE_RESERVE_BYTES + max_write_request_size,
        )
        .unwrap()
    }

    fn write_request(message: stream_write_message::FromClient) -> WriteRequest {
        match message.client_message.unwrap() {
            ClientMessage::WriteRequest(request) => request,
            other => panic!("expected write request, got {other:?}"),
        }
    }

    fn encoded_size(messages: Vec<MessageData>) -> usize {
        stream_write_message::FromClient {
            client_message: Some(ClientMessage::WriteRequest(WriteRequest {
                messages,
                codec: Codec::RAW.code,
                tx: None,
            })),
        }
        .encoded_len()
    }

    #[test]
    fn combines_compression_chunks_until_batch_ends() {
        let (request_tx, mut request_rx) = mpsc::unbounded_channel();
        let mut pending = None;
        let settings = settings(1024);

        StreamWriter::send_compressed_chunk(
            &request_tx,
            &settings,
            &mut pending,
            chunk(vec![message(1, 8)], false),
        )
        .unwrap();
        assert!(matches!(request_rx.try_recv(), Err(TryRecvError::Empty)));

        StreamWriter::send_compressed_chunk(
            &request_tx,
            &settings,
            &mut pending,
            chunk(vec![message(2, 8)], true),
        )
        .unwrap();

        let request = write_request(request_rx.try_recv().unwrap());
        assert_eq!(
            request
                .messages
                .into_iter()
                .map(|message| message.seq_no)
                .collect::<Vec<_>>(),
            vec![1, 2],
        );
        assert!(pending.is_none());
        assert!(matches!(request_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn splits_logical_batch_at_encoded_size_limit() {
        let first = message(1, 8);
        let second = message(2, 8);
        let one_message_size = encoded_size(vec![first.clone()]);
        let (request_tx, mut request_rx) = mpsc::unbounded_channel();
        let mut pending = None;

        StreamWriter::send_compressed_chunk(
            &request_tx,
            &settings(one_message_size),
            &mut pending,
            chunk(vec![first, second], true),
        )
        .unwrap();

        let first_request = request_rx.try_recv().unwrap();
        let second_request = request_rx.try_recv().unwrap();
        assert_eq!(first_request.encoded_len(), one_message_size);
        assert_eq!(second_request.encoded_len(), one_message_size);
        assert_eq!(write_request(first_request).messages[0].seq_no, 1);
        assert_eq!(write_request(second_request).messages[0].seq_no, 2);
        assert!(pending.is_none());
        assert!(matches!(request_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn does_not_combine_separate_logical_batches() {
        let (request_tx, mut request_rx) = mpsc::unbounded_channel();
        let mut pending = None;
        let settings = settings(1024);

        StreamWriter::send_compressed_chunk(
            &request_tx,
            &settings,
            &mut pending,
            chunk(vec![message(1, 8)], true),
        )
        .unwrap();
        StreamWriter::send_compressed_chunk(
            &request_tx,
            &settings,
            &mut pending,
            chunk(vec![message(2, 8)], true),
        )
        .unwrap();

        assert_eq!(
            write_request(request_rx.try_recv().unwrap()).messages[0].seq_no,
            1,
        );
        assert_eq!(
            write_request(request_rx.try_recv().unwrap()).messages[0].seq_no,
            2,
        );
        assert!(matches!(request_rx.try_recv(), Err(TryRecvError::Empty)));
    }
}
