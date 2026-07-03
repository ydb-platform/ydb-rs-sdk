use std::convert::Infallible;

use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use ydb_grpc::ydb_proto::topic::stream_read_message::{FromClient, FromServer};

use crate::{
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
    TopicReaderOptions, YdbError, YdbResult,
};

use super::reconnector;
use super::task_supervisor::wait_child_tasks;

// NOTE: The receive_loop → channel → decompressor hop adds one extra allocation
// and channel round-trip per server message. A follow-up refactor can eliminate
// grpc_streamer entirely and have the decompressor read directly from the gRPC
// stream, reducing RTT and removing this indirection.

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
        let stream = grpc_connect(&attempt.manager, &attempt.options).await?;

        Ok(Self {
            stream,
            cancellation: attempt.cancellation_token.clone(),
            decompression_input_tx,
            client_message_rx,
        })
    }

    pub(super) async fn run(self) -> YdbResult<()> {
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

pub(super) async fn grpc_connect(
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
    decompression_input_tx: mpsc::UnboundedSender<RawFromServer>,
    cancellation: CancellationToken,
) -> YdbResult<()> {
    select! {
        _ = cancellation.cancelled() => {
            debug!("topic reader grpc receive loop cancelled, stopping");
            Ok(())
        }
        result = receive_messages(stream, decompression_input_tx) => {
            let Err(err) = result;
            Err(err)
        }
    }
}

async fn receive_messages(
    mut stream: GrpcStream,
    decompression_input_tx: mpsc::UnboundedSender<RawFromServer>,
) -> YdbResult<Infallible> {
    loop {
        let message = stream.receive::<RawFromServer>().await?;
        decompression_input_tx.send(message).map_err(|_| {
            YdbError::Transport("topic reader grpc -> decompressor channel closed".to_string())
        })?;
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
