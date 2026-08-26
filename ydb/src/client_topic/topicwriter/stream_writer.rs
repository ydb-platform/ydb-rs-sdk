use std::convert::Infallible;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::trace;

use ydb_grpc::ydb_proto::topic::stream_write_message;
#[cfg(test)]
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::client_topic::compression::{
    CodecRegistry, CompressedChunk, CompressionBatch, CompressionWorker, Executor,
};
use crate::client_topic::list_types::Codec;
use crate::client_topic::topicwriter::message_write_status::WriteAck;
use crate::client_topic::topicwriter::state::WriterState;
use crate::client_topic::topicwriter::write_request::{
    PendingWriteRequest, TryAddMessage, WriteRequestSettings,
};
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_topic_service::stream_write::RawServerMessage;
use crate::{YdbError, YdbResult};

pub(super) fn spawn_connection_tasks(
    writer_options: TopicWriterOptions,
    stream: AsyncGrpcStreamWrapper<
        stream_write_message::FromClient,
        stream_write_message::FromServer,
    >,
    state: WriterState,
    epoch: usize,
    server_codecs: Vec<Codec>,
    executor: Arc<dyn Executor>,
    write_request_settings: WriteRequestSettings,
) -> YdbResult<JoinSet<YdbResult<Infallible>>> {
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

    let (batch_tx, batch_rx) = mpsc::unbounded_channel::<CompressionBatch>();
    let (compressed_tx, compressed_rx) = mpsc::unbounded_channel::<YdbResult<CompressedChunk>>();

    let request_stream = stream.clone_sender();

    let mut tasks = JoinSet::new();

    tasks.spawn(write_messages(state.clone(), epoch, batch_tx));

    worker.spawn_into(&mut tasks, batch_rx, compressed_tx);

    tasks.spawn(send_compressed_chunks(
        compressed_rx,
        request_stream,
        write_request_settings,
    ));

    tasks.spawn(receive_messages(state, epoch, stream));

    Ok(tasks)
}

async fn write_messages(
    state: WriterState,
    epoch: usize,
    batch_tx: mpsc::UnboundedSender<CompressionBatch>,
) -> YdbResult<Infallible> {
    loop {
        let batch = state.get_messages_to_send(epoch).await?;
        if batch.is_empty() {
            continue;
        }
        batch_tx.send(batch).map_err(|_| {
            YdbError::Transport("compression worker input channel closed".to_string())
        })?;
    }
}

async fn send_compressed_chunks(
    mut compressed_rx: mpsc::UnboundedReceiver<YdbResult<CompressedChunk>>,
    request_stream: mpsc::UnboundedSender<stream_write_message::FromClient>,
    write_request_settings: WriteRequestSettings,
) -> YdbResult<Infallible> {
    let mut pending_request = None;

    loop {
        let chunk = compressed_rx.recv().await.ok_or_else(|| {
            YdbError::Transport("compression worker output channel closed".to_string())
        })??;
        send_compressed_chunk(
            &request_stream,
            &write_request_settings,
            &mut pending_request,
            chunk,
        )?;
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
        transaction,
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
                *pending_request = Some(PendingWriteRequest::new(
                    settings,
                    codec,
                    message,
                    transaction.as_deref(),
                )?);
            }
            Some(mut request) => match request.try_add(message) {
                TryAddMessage::Added => {
                    *pending_request = Some(request);
                }
                TryAddMessage::RequestFull(message) => {
                    send_write_request(request_stream, request)?;
                    *pending_request = Some(PendingWriteRequest::new(
                        settings,
                        codec,
                        message,
                        transaction.as_deref(),
                    )?);
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
        send_write_request(request_stream, request)?;
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

async fn receive_messages(
    state: WriterState,
    epoch: usize,
    mut stream: AsyncGrpcStreamWrapper<
        stream_write_message::FromClient,
        stream_write_message::FromServer,
    >,
) -> YdbResult<Infallible> {
    loop {
        match stream.receive::<RawServerMessage>().await? {
            RawServerMessage::Init(_) => {
                return Err(YdbError::custom(
                    "unexpected message type in stream reader: init_response",
                ));
            }
            RawServerMessage::Write(write_response) => {
                for raw_ack in write_response.acks {
                    state.acknowledge_message(epoch, WriteAck::from(raw_ack))?;
                }
            }
            RawServerMessage::UpdateToken(_) => {}
        }
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
            transaction: None,
        }
    }

    fn settings(max_write_request_size: usize) -> WriteRequestSettings {
        WriteRequestSettings::new(WRITE_REQUEST_SIZE_RESERVE_BYTES + max_write_request_size)
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

        send_compressed_chunk(
            &request_tx,
            &settings,
            &mut pending,
            chunk(vec![message(1, 8)], false),
        )
        .unwrap();
        assert!(matches!(request_rx.try_recv(), Err(TryRecvError::Empty)));

        send_compressed_chunk(
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

        send_compressed_chunk(
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

        send_compressed_chunk(
            &request_tx,
            &settings,
            &mut pending,
            chunk(vec![message(1, 8)], true),
        )
        .unwrap();
        send_compressed_chunk(
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
