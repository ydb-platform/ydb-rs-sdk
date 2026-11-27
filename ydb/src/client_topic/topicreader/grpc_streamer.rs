use std::collections::HashMap;
use std::convert::Infallible;

use metrics::Counter;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use ydb_grpc::ydb_proto::topic::stream_read_message::{FromClient, FromServer};

use crate::client_metrics::names::TopicReaderMetrics;
use crate::{
    TopicReaderOptions, YdbError, YdbResult,
    grpc_connection_manager::GrpcConnectionManager,
    grpc_wrapper::{
        grpc_stream_wrapper::AsyncGrpcStreamWrapper,
        raw_topic_service::{
            client::RawTopicClient,
            stream_read::messages::{
                RawFromClientOneOf, RawFromServer, RawInitRequest, RawReadRequest,
            },
        },
    },
};

use super::ids::PartitionSessionId;
use super::reconnector;
use super::task_supervisor::wait_child_tasks;

const READER_BUFFER_SIZE: i64 = 1024 * 1024;

type GrpcStream = AsyncGrpcStreamWrapper<FromClient, FromServer>;

pub(super) struct GrpcStreamer {
    stream: GrpcStream,
    cancellation: CancellationToken,
    decompression_input_tx: mpsc::UnboundedSender<RawFromServer>,
    client_message_rx: mpsc::UnboundedReceiver<RawFromClientOneOf>,
}

impl GrpcStreamer {
    pub(super) async fn new(
        attempt: &reconnector::ConnectionAttempt,
        decompression_input_tx: mpsc::UnboundedSender<RawFromServer>,
        client_message_rx: mpsc::UnboundedReceiver<RawFromClientOneOf>,
    ) -> YdbResult<Self> {
        let mut stream = grpc_connect(&attempt.manager, &attempt.options).await?;
        handle_init_response(stream.receive::<RawFromServer>().await?)?;

        Ok(Self {
            stream,
            cancellation: attempt.cancellation_token.clone(),
            decompression_input_tx,
            client_message_rx,
        })
    }

    pub(super) async fn run(self, metrics: TopicReaderMetrics) -> YdbResult<()> {
        let Self {
            stream,
            cancellation,
            decompression_input_tx,
            client_message_rx,
        } = self;

        let client_message_tx = stream.clone_sender();
        let stream_cancellation = cancellation.child_token();

        let mut tasks: JoinSet<YdbResult<()>> = JoinSet::new();

        tasks.spawn(receive_loop(
            stream,
            decompression_input_tx,
            metrics,
            stream_cancellation.clone(),
        ));

        tasks.spawn(send_loop(
            client_message_tx,
            client_message_rx,
            stream_cancellation.clone(),
        ));

        wait_child_tasks(&stream_cancellation, tasks, "topic reader grpc stream").await
    }
}

fn handle_init_response(message: RawFromServer) -> YdbResult<()> {
    match message {
        RawFromServer::InitResponse(response) => {
            debug!(?response, "topic reader initialized");
            Ok(())
        }
        message => Err(YdbError::custom(format!(
            "topic reader expected init response, got: {message:?}"
        ))),
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
        reader_name: options.reader_name.clone().unwrap_or_default(),
        auto_partitioning_support: options.auto_partitioning_support,
    };

    Ok(topic_service.stream_read(init_request).await?)
}

async fn receive_loop(
    stream: GrpcStream,
    decompression_input_tx: mpsc::UnboundedSender<RawFromServer>,
    metrics: TopicReaderMetrics,
    cancellation: CancellationToken,
) -> YdbResult<()> {
    select! {
        _ = cancellation.cancelled() => {
            debug!("topic reader grpc receive loop cancelled, stopping");
            Ok(())
        }
        result = receive_messages(stream, decompression_input_tx, metrics) => {
            let Err(err) = result;
            Err(err)
        }
    }
}

async fn receive_messages(
    mut stream: GrpcStream,
    decompression_input_tx: mpsc::UnboundedSender<RawFromServer>,
    metrics: TopicReaderMetrics,
) -> YdbResult<Infallible> {
    // Session ids belong to this stream. Follow wire order even when the
    // decompressor is still handling an earlier control message.
    let mut sessions = HashMap::new();
    loop {
        let message = stream.receive::<RawFromServer>().await?;
        record_received(&metrics, &mut sessions, &message);
        decompression_input_tx.send(message).map_err(|_| {
            YdbError::Transport("topic reader grpc -> decompressor channel closed".to_string())
        })?;
    }
}

fn record_received(
    metrics: &TopicReaderMetrics,
    sessions: &mut HashMap<PartitionSessionId, Counter>,
    message: &RawFromServer,
) {
    match message {
        RawFromServer::StartPartitionSessionRequest(request) => {
            let session = &request.partition_session;
            sessions
                .entry(session.partition_session_id)
                .or_insert_with(|| metrics.received_messages(&session.path));
        }
        RawFromServer::StopPartitionSessionRequest(request) => {
            sessions.remove(&request.partition_session_id);
        }
        RawFromServer::EndPartitionSession(request) => {
            sessions.remove(&request.partition_session_id);
        }
        RawFromServer::ReadResponse(response) => {
            match u64::try_from(response.bytes_size) {
                Ok(bytes) => metrics.received_bytes.increment(bytes),
                Err(_) => warn!(
                    bytes_size = response.bytes_size,
                    "topic reader received a negative response size"
                ),
            }
            for partition in &response.partition_data {
                if let Some(counter) = sessions.get(&partition.partition_session_id) {
                    let messages = partition
                        .batches
                        .iter()
                        .map(|batch| batch.message_data.len() as u64)
                        .sum();
                    counter.increment(messages);
                }
            }
        }
        _ => {}
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc_wrapper::raw_topic_service::stream_read::messages::{
        RawInitResponse, RawReadResponse,
    };
    use ydb_grpc::ydb_proto::topic::stream_read_message;

    #[test]
    fn accepts_init_response_as_first_server_message() {
        let response = stream_read_message::InitResponse {
            session_id: "test-session".to_string(),
        };

        assert!(
            handle_init_response(RawFromServer::InitResponse(RawInitResponse::from(response)))
                .is_ok()
        );
    }

    #[test]
    fn rejects_non_init_response_as_first_server_message() {
        assert!(
            handle_init_response(RawFromServer::ReadResponse(RawReadResponse {
                bytes_size: 0,
                partition_data: Vec::new(),
            }))
            .is_err()
        );
    }
}
