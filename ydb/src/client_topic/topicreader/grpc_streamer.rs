use std::collections::HashMap;
use std::convert::Infallible;

use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};
use ydb_grpc::ydb_proto::topic::stream_read_message::{FromClient, FromServer};

use crate::TopicReaderBatch;
use crate::{
    grpc_connection_manager::GrpcConnectionManager,
    grpc_wrapper::{
        grpc_stream_wrapper::AsyncGrpcStreamWrapper,
        raw_topic_service::{
            client::RawTopicClient,
            stream_read::messages::{
                RawFromClientOneOf, RawFromServer, RawInitRequest, RawReadRequest, RawReadResponse,
                RawStartPartitionSessionResponse, RawStopPartitionSessionRequest,
                RawStopPartitionSessionResponse,
            },
        },
    },
    TopicReaderOptions, YdbError, YdbResult,
};

use super::messages::ReaderEvent;
use super::partition_state::PartitionSession;
use super::reconnector;
use super::runtime;
use super::task_supervisor::wait_child_tasks;

const READER_BUFFER_SIZE: i64 = 1024 * 1024;

type GrpcStream = AsyncGrpcStreamWrapper<FromClient, FromServer>;

pub(super) struct GrpcStreamer {
    stream: GrpcStream,
    cancellation: CancellationToken,
    decompression_input_tx: mpsc::UnboundedSender<ReaderEvent>,
    client_message_rx: mpsc::UnboundedReceiver<RawFromClientOneOf>,
    runtime: runtime::RuntimeHandle,
    reader_id: usize,
    epoch: usize,
}

impl GrpcStreamer {
    pub(super) async fn new(
        attempt: &reconnector::ConnectionAttempt,
        decompression_input_tx: mpsc::UnboundedSender<ReaderEvent>,
        client_message_rx: mpsc::UnboundedReceiver<RawFromClientOneOf>,
        runtime: runtime::RuntimeHandle,
    ) -> YdbResult<Self> {
        let stream = grpc_connect(&attempt.manager, &attempt.options).await?;

        Ok(Self {
            stream,
            cancellation: attempt.cancellation_token.clone(),
            decompression_input_tx,
            client_message_rx,
            runtime,
            reader_id: attempt.reader_id,
            epoch: attempt.epoch,
        })
    }

    pub(super) async fn run(self) -> YdbResult<()> {
        let Self {
            stream,
            cancellation,
            decompression_input_tx,
            client_message_rx,
            runtime,
            reader_id,
            epoch,
        } = self;

        let client_message_tx = stream.clone_sender();
        let stream_cancellation = cancellation.child_token();

        let mut tasks: JoinSet<YdbResult<()>> = JoinSet::new();

        tasks.spawn(receive_loop(
            stream,
            runtime,
            decompression_input_tx,
            stream_cancellation.clone(),
            reader_id,
            epoch,
        ));

        tasks.spawn(send_loop(
            client_message_tx,
            client_message_rx,
            stream_cancellation.clone(),
        ));

        wait_child_tasks(&stream_cancellation, tasks, "topic reader grpc stream").await
    }
}

async fn grpc_connect(
    manager: &GrpcConnectionManager,
    options: &TopicReaderOptions,
) -> YdbResult<GrpcStream> {
    debug!(
        consumer = options.consumer,
        "starting topic reader grpc connection"
    );

    let mut topic_service = manager.get_auth_service(RawTopicClient::new).await?;

    let init_request = RawInitRequest {
        topics_read_settings: options.topic.clone().into_topics_read_settings(),
        consumer: options.consumer.clone(),
        reader_name: "".to_string(),
    };

    Ok(topic_service.stream_read(init_request).await?)
}

async fn receive_loop(
    stream: GrpcStream,
    runtime: runtime::RuntimeHandle,
    decompression_input_tx: mpsc::UnboundedSender<ReaderEvent>,
    cancellation: CancellationToken,
    reader_id: usize,
    epoch: usize,
) -> YdbResult<()> {
    select! {
        _ = cancellation.cancelled() => {
            debug!("topic reader grpc receive loop cancelled, stopping");
            Ok(())
        }
        result = receive_messages(stream, runtime, decompression_input_tx, reader_id, epoch) => {
            let Err(err) = result;
            Err(err)
        }
    }
}

async fn receive_messages(
    mut stream: GrpcStream,
    runtime: runtime::RuntimeHandle,
    decompression_input_tx: mpsc::UnboundedSender<ReaderEvent>,
    reader_id: usize,
    epoch: usize,
) -> YdbResult<Infallible> {
    let mut sessions: HashMap<i64, PartitionSession> = HashMap::new();

    loop {
        let message = stream.receive::<RawFromServer>().await?;

        match message {
            RawFromServer::ReadResponse(resp) => {
                handle_read_response(
                    resp,
                    &mut sessions,
                    &decompression_input_tx,
                    reader_id,
                    epoch,
                )?;
            }

            RawFromServer::InitResponse(_) => {
                debug!("topic reader initialized");
            }

            RawFromServer::CommitOffsetResponse(resp) => {
                let committed_iter = resp
                    .partitions_committed_offsets
                    .into_iter()
                    .map(|offset| (offset.partition_session_id, offset.committed_offset));

                runtime.ack_commits(committed_iter)?;
            }

            RawFromServer::StartPartitionSessionRequest(req) => {
                let partition_session = PartitionSession::from(req);
                let partition_session_id = partition_session.partition_session_id;
                sessions.insert(partition_session_id, partition_session);

                let response = RawFromClientOneOf::StartPartitionSessionResponse(
                    RawStartPartitionSessionResponse {
                        partition_session_id,
                    },
                );
                stream.send_nowait(response)?;
            }

            RawFromServer::StopPartitionSessionRequest(req) => {
                let RawStopPartitionSessionRequest {
                    partition_session_id,
                    graceful,
                    committed_offset,
                } = req;

                debug!(
                    partition_session_id,
                    graceful,
                    committed_offset,
                    "topic reader received stop partition session request"
                );

                if sessions.remove(&partition_session_id).is_some() {
                    // TODO: For graceful stops, delay response until buffered messages
                    // from this partition are processed and commits up to
                    // committed_offset are acknowledged.
                    runtime.stop_partition(
                        partition_session_id,
                        Some(committed_offset),
                        &YdbError::custom(format!(
                            "partition session {partition_session_id} stopped by server"
                        )),
                    )?;
                } else {
                    warn!(
                        partition_session_id,
                        "topic reader received stop for unknown partition session"
                    );
                }

                let response = RawFromClientOneOf::StopPartitionSessionResponse(
                    RawStopPartitionSessionResponse {
                        partition_session_id,
                    },
                );
                stream.send_nowait(response)?;
            }

            RawFromServer::EndPartitionSession(end) => {
                debug!(
                    partition_session_id = end.partition_session_id,
                    "topic reader received end partition session"
                );
                if sessions.remove(&end.partition_session_id).is_none() {
                    warn!(
                        partition_session_id = end.partition_session_id,
                        "topic reader received end for unknown partition session"
                    );
                }
                decompression_input_tx
                    .send(ReaderEvent::EndPartitionSession {
                        session_id: end.partition_session_id,
                        child_partition_ids: end.child_partition_ids,
                    })
                    .map_err(|_| {
                        YdbError::Transport(
                            "topic reader grpc -> decompressor channel closed".to_string(),
                        )
                    })?;
            }

            RawFromServer::UpdateTokenResponse(_) => {
                debug!("topic reader received update token response");
            }

            RawFromServer::UnsupportedMessage(mess) => {
                debug!("topic reader received unsupported message: {mess}");
            }
        }
    }
}

fn handle_read_response(
    resp: RawReadResponse,
    sessions: &mut HashMap<i64, PartitionSession>,
    decompression_input_tx: &mpsc::UnboundedSender<ReaderEvent>,
    reader_id: usize,
    epoch: usize,
) -> YdbResult<()> {
    for partition_data in resp.partition_data {
        let partition_session_id = partition_data.partition_session_id;
        let session = match sessions.get_mut(&partition_session_id) {
            Some(s) => s,
            None => {
                error!(
                    partition_session_id,
                    "read response for unknown partition session"
                );
                continue;
            }
        };

        for raw_batch in partition_data.batches {
            if raw_batch.message_data.is_empty() {
                continue;
            }

            let codec = raw_batch.codec.into();
            let batch_bytes = raw_batch.get_read_session_size();
            let batch = TopicReaderBatch::new(raw_batch, session, reader_id, epoch);
            let mut messages = batch.messages;
            if let Some(last) = messages.last_mut() {
                last.bytes_to_release = batch_bytes;
            }

            decompression_input_tx
                .send(ReaderEvent::Messages { messages, codec })
                .map_err(|_| {
                    YdbError::Transport(
                        "topic reader grpc -> decompressor channel closed".to_string(),
                    )
                })?;
        }
    }

    Ok(())
}

async fn send_loop(
    client_message_tx: mpsc::UnboundedSender<FromClient>,
    mut client_message_rx: mpsc::UnboundedReceiver<RawFromClientOneOf>,
    cancellation: CancellationToken,
) -> YdbResult<()> {
    select! {
        _ = cancellation.cancelled() => {
            debug!("topic reader grpc send loop cancelled, stopping");
            Ok(())
        }
        result = send_messages(&client_message_tx, &mut client_message_rx) => {
            let Err(e) = result;
            Err(e)
        }
    }
}

async fn send_messages(
    client_message_tx: &mpsc::UnboundedSender<FromClient>,
    client_message_rx: &mut mpsc::UnboundedReceiver<RawFromClientOneOf>,
) -> YdbResult<Infallible> {
    send_client_message(
        client_message_tx,
        RawFromClientOneOf::ReadRequest(RawReadRequest {
            bytes_size: READER_BUFFER_SIZE,
        }),
    )?;

    loop {
        let message = client_message_rx.recv().await.ok_or(YdbError::Transport(
            "topic reader grpc send queue closed".into(),
        ))?;

        send_client_message(client_message_tx, message)?;
    }
}

fn send_client_message(
    sender: &mpsc::UnboundedSender<FromClient>,
    msg: RawFromClientOneOf,
) -> YdbResult<()> {
    let from_client: FromClient = msg.into();
    sender
        .send(from_client)
        .map_err(|err| YdbError::Transport(format!("topic reader send failed: {err}")))
}
