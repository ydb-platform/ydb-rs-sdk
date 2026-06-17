use crate::client_common::TokenCache;
use crate::client_topic::topicreader::cancelation_token::YdbCancellationToken;
use crate::client_topic::topicreader::messages::{TopicReaderBatch, TopicReaderMessage};
use crate::client_topic::topicreader::partition_state::PartitionSession;
use crate::client_topic::topicreader::reader_options::{
    TopicReaderOptions, TopicReaderOptionsBuilder,
};
use crate::errors::NeedRetry;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_topic_service::client::RawTopicClient;
use crate::grpc_wrapper::raw_topic_service::common::partition::RawOffsetsRange;
use crate::grpc_wrapper::raw_topic_service::common::update_token::RawUpdateTokenRequest;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::{
    PartitionCommitOffset, RawCommitOffsetRequest, RawFromClientOneOf, RawFromServer,
    RawInitRequest, RawReadRequest, RawReadResponse, RawStartPartitionSessionResponse,
    RawStopPartitionSessionResponse, RawTopicReadSettings,
};
use crate::grpc_wrapper::raw_topic_service::update_offsets_in_transaction::{
    RawPartitionOffsets, RawTopicOffsets, RawTransactionIdentity,
    RawUpdateOffsetsInTransactionRequest,
};
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::transaction::{Transaction, TransactionInfo};
use crate::{YdbError, YdbResult};
use secrecy::ExposeSecret;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::select;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{oneshot, Notify};
use tracing::{debug, error, info};
use ydb_grpc::ydb_proto::topic::stream_read_message::{FromClient, FromServer};

const READER_SHARED_STATE_POISONED: &str = "topic reader shared state mutex poisoned";
const READER_BUFFER_SIZE: i64 = 1024 * 1024; // 1MB
const UPDATE_TOKEN_INTERVAL: time::Duration = Duration::from_secs(3600);

const RETRY_BACKOFF_SLOT: Duration = Duration::from_millis(100);
const RETRY_BACKOFF_CEILING: u32 = 6;
const RECONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);

type PartitionSessionId = i64;
type GrpcStream = AsyncGrpcStreamWrapper<FromClient, FromServer>;

#[derive(Default)]
struct PendingCommits {
    // NOTE: Reverse keeps all offsets covered by a server ack in the right side
    // of split_off(&Reverse(committed_offset)): real end_offset <= committed_offset.
    sessions: HashMap<PartitionSessionId, BTreeMap<std::cmp::Reverse<i64>, oneshot::Sender<()>>>,
}

impl PendingCommits {
    fn push(
        &mut self,
        partition_session_id: PartitionSessionId,
        committed_offset: i64,
    ) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();

        let session = self.sessions.entry(partition_session_id).or_default();

        session.insert(std::cmp::Reverse(committed_offset), sender);

        receiver
    }

    fn ack(&mut self, committed_offsets: impl IntoIterator<Item = (PartitionSessionId, i64)>) {
        for (partition_session_id, committed_offset) in committed_offsets {
            self.ack_partition(partition_session_id, committed_offset);
        }
    }

    fn fail_all(&mut self) {
        self.sessions.clear();
    }

    fn fail_session(&mut self, partition_session_id: PartitionSessionId) {
        self.sessions.remove(&partition_session_id);
    }

    fn stop(&mut self, partition_session_id: PartitionSessionId, committed_offset: Option<i64>) {
        if let Some(committed_offset) = committed_offset {
            self.ack_partition(partition_session_id, committed_offset);
        }
        self.fail_session(partition_session_id);
    }

    fn ack_partition(&mut self, partition_session_id: PartitionSessionId, committed_offset: i64) {
        let Some(session) = self.sessions.get_mut(&partition_session_id) else {
            return;
        };

        let acked = session.split_off(&std::cmp::Reverse(committed_offset));
        Self::ack_commits(acked);

        if session.is_empty() {
            self.sessions.remove(&partition_session_id);
        }
    }

    fn ack_commits(commits: BTreeMap<std::cmp::Reverse<i64>, oneshot::Sender<()>>) {
        commits.into_values().for_each(|sender| {
            let _ = sender.send(());
        });
    }
}

#[derive(Default)]
struct SharedState {
    buffer: VecDeque<TopicReaderMessage>,
    pending_commits: PendingCommits,
}

pub(crate) struct ReaderShared {
    state: Mutex<YdbResult<SharedState>>,
    notify: Notify,
}

impl ReaderShared {
    fn new() -> Self {
        Self {
            state: Mutex::new(Ok(SharedState::default())),
            notify: Notify::new(),
        }
    }

    fn fail(&self, err: YdbError) {
        let mut state = self.lock_state();

        if let Ok(state) = &mut *state {
            state.pending_commits.fail_all();
        }
        *state = Err(err);

        self.notify.notify_one();
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, YdbResult<SharedState>> {
        self.state.lock().expect(READER_SHARED_STATE_POISONED)
    }
}

struct TopicReaderContext {
    manager: GrpcConnectionManager,
    options: TopicReaderOptions,
    token_cache: TokenCache,
}

pub struct TopicReader {
    context: TopicReaderContext,
    epoch: usize,

    reader: StreamReader,
}

type TopicCommitHandler = tokio::sync::oneshot::Receiver<()>;

impl TopicReader {
    pub(crate) async fn new(
        options: TopicReaderOptions,
        manager: GrpcConnectionManager,
        token_cache: TokenCache,
    ) -> YdbResult<Self> {
        let context = TopicReaderContext {
            manager,
            options,
            token_cache,
        };
        let epoch = 0;
        let reader = StreamReader::new(&context, epoch).await?;

        Ok(Self {
            context,
            epoch,
            reader,
        })
    }

    pub async fn read_batch(&mut self) -> YdbResult<TopicReaderBatch> {
        loop {
            match self.reader.read_batch().await {
                Ok(batch) => return Ok(batch),
                Err(err) => self.try_reconnect_on_err(err).await?,
            }
        }
    }

    pub async fn pop_batch_in_tx(
        &mut self,
        tx: &mut Box<dyn Transaction>,
    ) -> YdbResult<TopicReaderBatch> {
        let tx_info = tx.transaction_info().await?;
        let batch = self.read_batch().await?;
        self.reader
            .update_offsets_in_transaction(&batch, &tx_info)
            .await?;
        Ok(batch)
    }

    pub fn commit(&mut self, commit_marker: TopicReaderCommitMarker) -> TopicCommitHandler {
        self.reader
            .commit(commit_marker)
            .unwrap_or_else(|_| StreamReader::cancelled_commit_handle())
    }

    async fn try_reconnect_on_err(&mut self, err: YdbError) -> YdbResult<()> {
        self.ensure_retriable(err)?;

        self.reader.cancel().await;
        self.epoch += 1;

        let mut attempts: usize = 0;
        let start = std::time::Instant::now();

        let reader = loop {
            attempts += 1;

            match tokio::time::timeout(
                RECONNECT_ATTEMPT_TIMEOUT,
                StreamReader::new(&self.context, self.epoch),
            )
            .await
            {
                Ok(Ok(reader)) => break reader,
                Ok(Err(err)) => self.ensure_retriable(err)?,
                Err(_) => {
                    debug!(
                        consumer = self.context.options.consumer,
                        epoch = self.epoch,
                        attempt = attempts,
                        "topic reader reconnect attempt timed out"
                    );
                }
            };

            tokio::time::sleep(topic_reader_retry_backoff(attempts)).await;
        };

        info!(
            consumer = self.context.options.consumer,
            epoch = self.epoch,
            elapsed = ?start.elapsed(),
            attempts = attempts,
            "topic reader reconnected"
        );

        self.reader = reader;

        Ok(())
    }

    /// Converts a retriable error into `Ok(())` and returns non-retriable errors unchanged.
    fn ensure_retriable(&self, err: YdbError) -> YdbResult<()> {
        match err.need_retry() {
            NeedRetry::True | NeedRetry::IdempotentOnly => {
                info!(
                    consumer = self.context.options.consumer,
                    epoch = self.epoch,
                    err = %err,
                    "topic reader error is retriable, reconnecting"
                );
                Ok(())
            }
            NeedRetry::False => {
                error!(
                    consumer = self.context.options.consumer,
                    epoch = self.epoch,
                    err = %err,
                    "topic reader error is non-retriable"
                );
                Err(err)
            }
        }
    }
}

struct StreamReader {
    stream_sender: UnboundedSender<FromClient>,
    topic_service: RawTopicClient,
    shared: Arc<ReaderShared>,

    consumer: String,
    stop_background_work_token: YdbCancellationToken,
    batch_size: usize,
    epoch: usize,

    background: tokio::task::JoinSet<()>,
}

impl StreamReader {
    async fn read_batch(&mut self) -> YdbResult<TopicReaderBatch> {
        self.read_batch_private().await
    }

    async fn read_batch_private(&mut self) -> YdbResult<TopicReaderBatch> {
        loop {
            // Register waiter BEFORE checking the buffer so that any notify_one()
            // between the check and .await either wakes us or leaves a permit.
            let notified = self.shared.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let prefix = match &mut *self.shared.lock_state() {
                Ok(state) => cut_prefix(&mut state.buffer, self.batch_size),
                Err(err) => return Err(err.clone()),
            };

            if let Some((messages, bytes_to_release)) = prefix {
                if bytes_to_release > 0 {
                    self.send_read_request(bytes_to_release)?;
                }
                return Ok(TopicReaderBatch::from_messages(messages));
            }

            notified.await;
        }
    }

    async fn update_offsets_in_transaction(
        &mut self,
        batch: &TopicReaderBatch,
        tx_info: &TransactionInfo,
    ) -> YdbResult<()> {
        let commit_marker = batch.get_commit_marker();

        let request = RawUpdateOffsetsInTransactionRequest {
            operation_params: RawOperationParams::new_with_timeouts(
                Duration::from_secs(30),
                Duration::from_secs(60),
            ),
            tx: RawTransactionIdentity {
                id: tx_info.transaction_id.clone(),
                session: tx_info.session_id.clone(),
            },
            topics: vec![RawTopicOffsets {
                path: commit_marker.topic.clone(),
                partitions: vec![RawPartitionOffsets {
                    partition_id: commit_marker.partition_id,
                    partition_offsets: vec![RawOffsetsRange {
                        start: commit_marker.start_offset,
                        end: commit_marker.end_offset,
                    }],
                }],
            }],
            consumer: self.consumer.clone(),
        };

        self.topic_service
            .update_offsets_in_transaction(request)
            .await?;

        Ok(())
    }

    fn cancelled_commit_handle() -> TopicCommitHandler {
        let (_sender, reciever) = oneshot::channel();
        reciever
    }

    fn commit(&mut self, commit_marker: TopicReaderCommitMarker) -> YdbResult<TopicCommitHandler> {
        if self.epoch != commit_marker.epoch {
            return Ok(Self::cancelled_commit_handle());
        }

        let receiver = {
            let mut state_guard = self.shared.lock_state();

            let state = match &mut *state_guard {
                Ok(state) => state,
                Err(err) => {
                    return Err(err.clone());
                }
            };

            state
                .pending_commits
                .push(commit_marker.partition_session_id, commit_marker.end_offset)
        };

        let commit_message = RawFromClientOneOf::CommitOffsetRequest(RawCommitOffsetRequest {
            commit_offsets: vec![PartitionCommitOffset {
                partition_session_id: commit_marker.partition_session_id,
                offsets: vec![RawOffsetsRange {
                    start: commit_marker.start_offset,
                    end: commit_marker.end_offset,
                }],
            }],
        });

        if let Err(err) = send_on_stream(&self.stream_sender, commit_message) {
            self.shared.fail(err.clone());

            Err(err)
        } else {
            Ok(receiver)
        }
    }

    pub(crate) async fn new(context: &TopicReaderContext, epoch: usize) -> YdbResult<Self> {
        let (stream, topic_service) =
            Self::grpc_connect(&context.manager, &context.options).await?;

        let mut stream_reader = StreamReader {
            stream_sender: stream.clone_sender(),
            shared: Arc::new(ReaderShared::new()),
            stop_background_work_token: YdbCancellationToken::new(),
            epoch,
            background: Default::default(),
            consumer: context.options.consumer.clone(),
            batch_size: context.options.batch_size,
            topic_service,
        };

        stream_reader.start_background_jobs(stream, context.token_cache.clone());

        debug!(
            consumer = stream_reader.consumer,
            epoch = stream_reader.epoch,
            "topic stream reader created"
        );

        Ok(stream_reader)
    }

    fn start_background_jobs(&mut self, stream: GrpcStream, token_cache: TokenCache) {
        self.background.spawn(receive_loop(
            stream,
            self.shared.clone(),
            self.stop_background_work_token.clone(),
            self.epoch,
        ));

        self.background.spawn(Self::update_token_loop(
            self.stop_background_work_token.clone(),
            self.stream_sender.clone(),
            self.shared.clone(),
            token_cache,
        ));
    }

    fn send_read_request(&self, size: i64) -> YdbResult<()> {
        send_on_stream(
            &self.stream_sender,
            RawFromClientOneOf::ReadRequest(RawReadRequest { bytes_size: size }),
        )
    }

    async fn update_token_loop(
        cancellation_token: YdbCancellationToken,
        send: UnboundedSender<FromClient>,
        shared: Arc<ReaderShared>,
        auth_token: TokenCache,
    ) {
        let mut interval = tokio::time::interval(UPDATE_TOKEN_INTERVAL);
        interval.tick().await;

        let tokio_cancellation = cancellation_token.to_tokio_token();

        loop {
            select! {
                _ = tokio_cancellation.cancelled() => {
                    debug!("update_token_loop cancelled, stopping");
                    break;
                }
                _ = interval.tick() => {
                    if let Err(err) = Self::send_update_token(&send, &auth_token).await {
                        shared.fail(err);
                        break;
                    }
                }
            }
        }
    }

    async fn send_update_token(
        send: &UnboundedSender<FromClient>,
        auth_token: &TokenCache,
    ) -> YdbResult<()> {
        let token = auth_token.token();
        debug!("sending update token request from topic reader");

        let update_request = RawFromClientOneOf::UpdateTokenRequest(RawUpdateTokenRequest {
            token: token.expose_secret().to_string(),
        })
        .into();

        send.send(update_request).map_err(|err| {
            YdbError::Transport(format!("topic reader update token send failed: {err}"))
        })
    }

    async fn grpc_connect(
        manager: &GrpcConnectionManager,
        options: &TopicReaderOptions,
    ) -> YdbResult<(GrpcStream, RawTopicClient)> {
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

        let stream = topic_service.stream_read(init_request).await?;

        stream
            .send(RawFromClientOneOf::ReadRequest(RawReadRequest {
                bytes_size: READER_BUFFER_SIZE,
            }))
            .await?;

        let topic_client = manager.get_auth_service(RawTopicClient::new).await?;

        Ok((stream, topic_client))
    }

    async fn cancel(&mut self) {
        self.stop_background_work_token.cancel();
        self.background.shutdown().await;
    }
}

impl Drop for StreamReader {
    fn drop(&mut self) {
        self.stop_background_work_token.cancel();
        self.background.abort_all();
    }
}

/// If `buffer` is empty, returns [`None`].
///
/// Otherwise, takes up to `cap` messages from the front of `buffer`, all sharing the same
/// `partition_session_id`. Returns the messages and the total `bytes_to_release`
/// for the flow-control ReadRequest.
pub(crate) fn cut_prefix(
    buffer: &mut VecDeque<TopicReaderMessage>,
    cap: usize,
) -> Option<(Vec<TopicReaderMessage>, i64)> {
    let session_id = buffer.front()?.commit_marker.partition_session_id;

    let mut out = Vec::new();
    let mut bytes: i64 = 0;

    while out.len() < cap {
        match buffer.front() {
            Some(m) if m.commit_marker.partition_session_id == session_id => {
                let m = buffer.pop_front().expect("front() returned Some");
                bytes += m.bytes_to_release;
                out.push(m);
            }
            _ => break,
        }
    }

    Some((out, bytes))
}

/// Background task: reads from the grpc stream, dispatches incoming messages,
/// and pushes parsed TopicReaderMessages into the shared buffer.
async fn receive_loop(
    mut stream: AsyncGrpcStreamWrapper<FromClient, FromServer>,
    shared: Arc<ReaderShared>,
    stop: YdbCancellationToken,
    epoch: usize,
) {
    let mut sessions: HashMap<i64, PartitionSession> = HashMap::new();
    let tokio_stop = stop.to_tokio_token();

    let sender_for_responses = stream.clone_sender();

    let err: Option<YdbError> = loop {
        select! {
            _ = tokio_stop.cancelled() => {
                debug!("topic reader receive_loop cancelled, stopping");
                break None;
            }
            res = stream.receive::<RawFromServer>() => {
                match res {
                    Err(e) => break Some(e.into()),
                    Ok(msg) => {
                        if let Err(e) = handle_incoming(
                            msg,
                            &mut sessions,
                            &sender_for_responses,
                            &shared,
                            epoch,
                        ) {
                            break Some(e);
                        }
                    }
                }
            }
        }
    };

    if let Some(err) = err {
        if !tokio_stop.is_cancelled() {
            close_with_error(&shared, Some(err));
        }
    }
}

fn topic_reader_retry_backoff(attempt: usize) -> Duration {
    let multiplier = 1u32 << (attempt as u32).min(RETRY_BACKOFF_CEILING);
    RETRY_BACKOFF_SLOT * multiplier
}

fn handle_incoming(
    msg: RawFromServer,
    sessions: &mut HashMap<i64, PartitionSession>,
    sender: &UnboundedSender<FromClient>,
    shared: &ReaderShared,
    epoch: usize,
) -> YdbResult<()> {
    match msg {
        RawFromServer::ReadResponse(resp) => handle_read_response(resp, sessions, shared, epoch)?,
        RawFromServer::InitResponse(_) => debug!("topic reader initialized"),
        RawFromServer::CommitOffsetResponse(resp) => {
            debug!("commit offset response for topic reader: {:?}", resp);
            let mut state = shared.lock_state();
            if let Ok(state) = &mut *state {
                state.pending_commits.ack(
                    resp.partitions_committed_offsets
                        .into_iter()
                        .map(|offset| (offset.partition_session_id, offset.committed_offset)),
                );
            }
        }
        RawFromServer::UpdateTokenResponse(_) => {}
        RawFromServer::StartPartitionSessionRequest(request) => {
            sessions.insert(
                request.partition_session.partition_session_id,
                PartitionSession {
                    partition_session_id: request.partition_session.partition_session_id,
                    partition_id: request.partition_session.partition_id,
                    topic: request.partition_session.path,
                    next_commit_offset_start: request.committed_offset,
                },
            );
            send_on_stream(
                sender,
                RawFromClientOneOf::StartPartitionSessionResponse(
                    RawStartPartitionSessionResponse {
                        partition_session_id: request.partition_session.partition_session_id,
                    },
                ),
            )?;
        }
        RawFromServer::StopPartitionSessionRequest(request) => {
            sessions.remove(&request.partition_session_id);
            {
                let mut state = shared.lock_state();
                if let Ok(state) = &mut *state {
                    state
                        .pending_commits
                        .stop(request.partition_session_id, None);
                }
            }
            send_on_stream(
                sender,
                RawFromClientOneOf::StopPartitionSessionResponse(RawStopPartitionSessionResponse {
                    partition_session_id: request.partition_session_id,
                }),
            )?;
        }
        RawFromServer::UnsupportedMessage(mess) => {
            debug!("topic reader received unsupported message: {}", mess)
        }
    }
    Ok(())
}

/// Parses a RawReadResponse into TopicReaderMessages and appends them to the
/// shared buffer in FIFO order. The `bytes_to_release` tag is set by
/// `RawBatch::get_read_session_size()` — non-zero only on the very last message
/// of the entire response (see `From<ReadResponse> for RawReadResponse`).
pub(crate) fn handle_read_response(
    resp: RawReadResponse,
    sessions: &mut HashMap<i64, PartitionSession>,
    shared: &ReaderShared,
    epoch: usize,
) -> YdbResult<()> {
    for partition_data in resp.partition_data {
        let partition_session_id = partition_data.partition_session_id;
        let session = match sessions.get_mut(&partition_session_id) {
            Some(s) => s,
            None => {
                error!(
                    "read_response for unknown partition_session_id: {}",
                    partition_session_id
                );
                continue;
            }
        };
        for raw_batch in partition_data.batches {
            if raw_batch.message_data.is_empty() {
                continue;
            }
            let batch_bytes = raw_batch.get_read_session_size();
            let batch = TopicReaderBatch::new(raw_batch, session, epoch);
            let mut messages = batch.messages;
            if let Some(last) = messages.last_mut() {
                last.bytes_to_release = batch_bytes;
            }

            {
                let mut state = shared.lock_state();
                match &mut *state {
                    Ok(state) => state.buffer.extend(messages),
                    Err(err) => return Err(err.clone()),
                }
            }
            // push-then-notify: no lost wakeups
            shared.notify.notify_one();
        }
    }
    Ok(())
}

pub(crate) fn close_with_error(shared: &ReaderShared, err: Option<YdbError>) {
    shared.fail(err.unwrap_or_else(|| YdbError::custom("topic read stream closed")));
}

fn send_on_stream(
    sender: &UnboundedSender<FromClient>,
    message: RawFromClientOneOf,
) -> YdbResult<()> {
    let from_client: FromClient = message.into();
    sender
        .send(from_client)
        .map_err(|err| YdbError::Transport(format!("topic reader send failed: {err}")))
}

#[derive(Clone)]
pub struct TopicSelectors(pub Vec<TopicSelector>);

impl TopicSelectors {
    pub(crate) fn into_topics_read_settings(self) -> Vec<RawTopicReadSettings> {
        self.0
            .into_iter()
            .map(|selector| selector.into_raw_topic_read_setting())
            .collect()
    }
}

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
#[derive(Clone)]
pub struct TopicSelector {
    pub path: String,
    pub partition_ids: Option<Vec<i64>>,
    pub read_from: Option<SystemTime>,
}

impl TopicSelector {
    pub(crate) fn into_raw_topic_read_setting(self) -> RawTopicReadSettings {
        RawTopicReadSettings {
            path: self.path,
            partition_ids: self.partition_ids.unwrap_or_default(),
            read_from: self.read_from.map(|time| time.into()),
            max_lag: None,
        }
    }
}

impl From<String> for TopicSelectors {
    fn from(path: String) -> Self {
        TopicSelectors(vec![TopicSelector {
            path,
            partition_ids: None,
            read_from: Some(UNIX_EPOCH),
        }])
    }
}

impl From<&str> for TopicSelectors {
    fn from(path: &str) -> Self {
        path.to_owned().into()
    }
}

impl TopicReaderOptionsBuilder {
    pub fn from_consumer_topic(
        consumer: impl Into<String>,
        topic: impl Into<TopicSelectors>,
    ) -> Self {
        let mut b = TopicReaderOptionsBuilder::default();
        b.consumer(consumer.into()).topic(topic.into());
        b
    }
}

#[derive(Clone, Debug)]
pub struct TopicReaderCommitMarker {
    pub(crate) partition_session_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) start_offset: i64,
    pub(crate) end_offset: i64,
    pub(crate) topic: String,
    pub(crate) epoch: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_topic::topicreader::messages::TopicReaderBatch;
    use crate::grpc_wrapper::raw_topic_service::common::codecs::RawCodec;
    use crate::grpc_wrapper::raw_topic_service::stream_read::messages::{
        RawBatch, RawMessageData, RawPartitionData,
    };
    use crate::grpc_wrapper::raw_topic_service::update_offsets_in_transaction::*;
    use std::time::Duration;
    use tokio::sync::oneshot::error::TryRecvError;

    fn assert_commit_ack(mut receiver: oneshot::Receiver<()>) {
        assert_eq!(receiver.try_recv(), Ok(()));
    }

    fn assert_commit_pending(mut receiver: oneshot::Receiver<()>) -> oneshot::Receiver<()> {
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
        receiver
    }

    fn assert_commit_aborted(mut receiver: oneshot::Receiver<()>) {
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
    }

    #[test]
    fn topic_reader_retry_backoff_uses_middle_go_like_policy() {
        assert_eq!(topic_reader_retry_backoff(1), Duration::from_millis(200));
        assert_eq!(topic_reader_retry_backoff(6), Duration::from_millis(6400));
        assert_eq!(topic_reader_retry_backoff(7), Duration::from_millis(6400));
    }

    #[test]
    fn pending_commits_push_returns_pending_receiver() {
        let mut commits = PendingCommits::default();

        let receiver = commits.push(1, 10);

        let _receiver = assert_commit_pending(receiver);
    }

    #[test]
    fn pending_commits_ack_confirms_offsets_up_to_committed_offset() {
        let mut commits = PendingCommits::default();
        let offset_10 = commits.push(1, 10);
        let offset_20 = commits.push(1, 20);
        let offset_30 = commits.push(1, 30);

        commits.ack([(1, 20)]);

        assert_commit_ack(offset_10);
        assert_commit_ack(offset_20);
        let _offset_30 = assert_commit_pending(offset_30);
    }

    #[test]
    fn pending_commits_ack_ignores_other_sessions() {
        let mut commits = PendingCommits::default();
        let session_1 = commits.push(1, 10);
        let session_2 = commits.push(2, 10);

        commits.ack([(1, 10)]);

        assert_commit_ack(session_1);
        let _session_2 = assert_commit_pending(session_2);
    }

    #[test]
    fn pending_commits_push_replaces_waiter_for_same_offset() {
        let mut commits = PendingCommits::default();
        let replaced = commits.push(1, 10);
        let current = commits.push(1, 10);

        commits.ack([(1, 10)]);

        assert_commit_aborted(replaced);
        assert_commit_ack(current);
    }

    #[test]
    fn pending_commits_fail_all_aborts_all_receivers() {
        let mut commits = PendingCommits::default();
        let session_1 = commits.push(1, 10);
        let session_2 = commits.push(2, 20);

        commits.fail_all();

        assert_commit_aborted(session_1);
        assert_commit_aborted(session_2);
    }

    #[test]
    fn pending_commits_fail_session_aborts_only_requested_session() {
        let mut commits = PendingCommits::default();
        let session_1 = commits.push(1, 10);
        let session_2 = commits.push(2, 10);

        commits.fail_session(1);

        assert_commit_aborted(session_1);
        let _session_2 = assert_commit_pending(session_2);
    }

    #[test]
    fn pending_commits_stop_acks_covered_offsets_and_aborts_rest() {
        let mut commits = PendingCommits::default();
        let covered = commits.push(1, 10);
        let uncovered = commits.push(1, 20);

        commits.stop(1, Some(10));

        assert_commit_ack(covered);
        assert_commit_aborted(uncovered);
    }

    #[test]
    fn pending_commits_stop_without_committed_offset_aborts_session() {
        let mut commits = PendingCommits::default();
        let receiver = commits.push(1, 10);

        commits.stop(1, None);

        assert_commit_aborted(receiver);
    }

    #[test]
    fn transaction_topic_reading_integration() {
        let commit_marker = TopicReaderCommitMarker {
            partition_session_id: 456,
            partition_id: 789,
            start_offset: 1000,
            end_offset: 1100,
            topic: "integration-test-topic".to_string(),
            epoch: 0,
        };

        let raw_request = RawUpdateOffsetsInTransactionRequest {
            operation_params: RawOperationParams::new_with_timeouts(
                Duration::from_secs(30),
                Duration::from_secs(60),
            ),
            tx: RawTransactionIdentity {
                id: "integration_tx_id".to_string(),
                session: "integration_session_id".to_string(),
            },
            topics: vec![RawTopicOffsets {
                path: commit_marker.topic.clone(),
                partitions: vec![RawPartitionOffsets {
                    partition_id: commit_marker.partition_id,
                    partition_offsets: vec![RawOffsetsRange {
                        start: commit_marker.start_offset,
                        end: commit_marker.end_offset,
                    }],
                }],
            }],
            consumer: "integration-consumer".to_string(),
        };

        use ydb_grpc::ydb_proto::topic::UpdateOffsetsInTransactionRequest;
        let proto_request: UpdateOffsetsInTransactionRequest = raw_request.into();

        assert!(proto_request.operation_params.is_some());
        assert!(proto_request.tx.is_some());
        assert_eq!(proto_request.consumer, "integration-consumer");
        assert_eq!(proto_request.topics.len(), 1);

        let proto_tx = proto_request.tx.unwrap();
        assert_eq!(proto_tx.id, "integration_tx_id");
        assert_eq!(proto_tx.session, "integration_session_id");

        let proto_topic = &proto_request.topics[0];
        assert_eq!(proto_topic.path, "integration-test-topic");
        assert_eq!(proto_topic.partitions.len(), 1);

        let proto_partition = &proto_topic.partitions[0];
        assert_eq!(proto_partition.partition_id, 789);
        assert_eq!(proto_partition.partition_offsets.len(), 1);

        let proto_offsets = &proto_partition.partition_offsets[0];
        assert_eq!(proto_offsets.start, 1000);
        assert_eq!(proto_offsets.end, 1100);
    }

    // ---- test helpers ----

    fn make_session(
        partition_session_id: i64,
        partition_id: i64,
        topic: &str,
        start_offset: i64,
    ) -> PartitionSession {
        PartitionSession {
            partition_session_id,
            partition_id,
            topic: topic.to_string(),
            next_commit_offset_start: start_offset,
        }
    }

    fn make_raw_batch(start_offset: i64, count: usize) -> RawBatch {
        let message_data = (0..count)
            .map(|i| RawMessageData {
                offset: start_offset + i as i64,
                seq_no: (start_offset + i as i64) + 1,
                created_at: None,
                uncompressed_size: 0,
                data: vec![],
                read_session_size_bytes: 0,
            })
            .collect();
        RawBatch {
            producer_id: "p".to_string(),
            write_session_meta: HashMap::new(),
            codec: RawCodec { code: 1 },
            written_at: SystemTime::UNIX_EPOCH.into(),
            message_data,
        }
    }

    fn message_for_session(
        session: &mut PartitionSession,
        offset: i64,
        bytes_to_release: i64,
    ) -> TopicReaderMessage {
        let raw = make_raw_batch(offset, 1);
        let batch = TopicReaderBatch::new(raw, session, 0);
        let mut m = batch.messages.into_iter().next().unwrap();
        m.bytes_to_release = bytes_to_release;
        m
    }

    // ---- cut_prefix ----

    #[test]
    fn cut_prefix_hard_limit_within_one_session() {
        let mut session = make_session(1, 11, "t", 0);
        let mut buf: VecDeque<TopicReaderMessage> = VecDeque::new();
        for offset in 0..1500i64 {
            let bytes = if offset == 1499 { 12345 } else { 0 };
            buf.push_back(message_for_session(&mut session, offset, bytes));
        }

        let (first, first_bytes) = cut_prefix(&mut buf, 1000).unwrap();
        assert_eq!(first.len(), 1000);
        assert_eq!(first.first().unwrap().offset, 0);
        assert_eq!(first.last().unwrap().offset, 999);
        assert_eq!(first_bytes, 0);

        let (second, second_bytes) = cut_prefix(&mut buf, 1000).unwrap();
        assert_eq!(second.len(), 500);
        assert_eq!(second.first().unwrap().offset, 1000);
        assert_eq!(second.last().unwrap().offset, 1499);
        assert_eq!(second_bytes, 12345);

        assert!(buf.is_empty());
    }

    #[test]
    fn cut_prefix_returns_none_for_empty_buffer() {
        let mut buf = VecDeque::new();

        assert!(cut_prefix(&mut buf, 1000).is_none());
    }

    #[test]
    fn cut_prefix_stops_at_session_boundary() {
        let mut a1 = make_session(1, 11, "t", 0);
        let mut b = make_session(2, 22, "t", 0);
        let mut a2 = make_session(3, 33, "t", 0);

        let mut buf: VecDeque<TopicReaderMessage> = VecDeque::new();
        for offset in 0..200 {
            buf.push_back(message_for_session(&mut a1, offset, 0));
        }
        for offset in 0..200 {
            buf.push_back(message_for_session(&mut b, offset, 0));
        }
        for offset in 0..100 {
            buf.push_back(message_for_session(&mut a2, offset, 0));
        }

        let (first, _) = cut_prefix(&mut buf, 1000).unwrap();
        assert_eq!(first.len(), 200);
        assert!(first
            .iter()
            .all(|m| m.commit_marker.partition_session_id == 1));

        let (second, _) = cut_prefix(&mut buf, 1000).unwrap();
        assert_eq!(second.len(), 200);
        assert!(second
            .iter()
            .all(|m| m.commit_marker.partition_session_id == 2));

        let (third, _) = cut_prefix(&mut buf, 1000).unwrap();
        assert_eq!(third.len(), 100);
        assert!(third
            .iter()
            .all(|m| m.commit_marker.partition_session_id == 3));

        assert!(buf.is_empty());
    }

    #[test]
    fn cut_prefix_aggregates_bytes() {
        let mut session = make_session(1, 11, "t", 0);
        let mut buf: VecDeque<TopicReaderMessage> = VecDeque::new();
        for offset in 0..5 {
            let bytes = if offset == 4 { 1234 } else { 0 };
            buf.push_back(message_for_session(&mut session, offset, bytes));
        }

        let (msgs, bytes) = cut_prefix(&mut buf, 10).unwrap();
        assert_eq!(msgs.len(), 5);
        assert_eq!(bytes, 1234);
        assert!(buf.is_empty());
    }

    #[test]
    fn cut_prefix_hard_limit_leaves_bytes_tag_on_remainder() {
        let mut session = make_session(1, 11, "t", 0);
        let mut buf: VecDeque<TopicReaderMessage> = VecDeque::new();
        for offset in 0..5 {
            let bytes = if offset == 4 { 1234 } else { 0 };
            buf.push_back(message_for_session(&mut session, offset, bytes));
        }

        let (first, bytes_first) = cut_prefix(&mut buf, 3).unwrap();
        assert_eq!(first.len(), 3);
        assert_eq!(bytes_first, 0);

        let (second, bytes_second) = cut_prefix(&mut buf, 10).unwrap();
        assert_eq!(second.len(), 2);
        assert_eq!(bytes_second, 1234);
    }

    // ---- handle_read_response ----

    fn make_raw_partition_data(
        partition_session_id: i64,
        batches: Vec<RawBatch>,
    ) -> RawPartitionData {
        RawPartitionData {
            partition_session_id,
            batches: batches.into_iter().collect(),
        }
    }

    /// Mimics server behavior: bytes_size is stamped on the last message
    /// of the last batch of the last partition_data.
    fn make_raw_read_response(
        bytes_size: i64,
        partition_data: Vec<RawPartitionData>,
    ) -> RawReadResponse {
        let mut resp = RawReadResponse {
            bytes_size,
            partition_data,
        };

        if let Some(last_pd) = resp.partition_data.last_mut() {
            if let Some(last_batch) = last_pd.batches.back_mut() {
                if let Some(last_msg) = last_batch.message_data.last_mut() {
                    last_msg.read_session_size_bytes = bytes_size;
                }
            }
        }
        resp
    }

    #[test]
    fn handle_read_response_preserves_fifo_across_partition_data() {
        let shared = ReaderShared::new();
        let mut sessions: HashMap<i64, PartitionSession> = HashMap::new();
        sessions.insert(1, make_session(1, 11, "t-a", 0));
        sessions.insert(2, make_session(2, 22, "t-b", 0));
        sessions.insert(3, make_session(3, 33, "t-a2", 0));

        let pd_a1 = make_raw_partition_data(1, vec![make_raw_batch(0, 2), make_raw_batch(2, 2)]);
        let pd_b = make_raw_partition_data(2, vec![make_raw_batch(0, 3)]);
        let pd_a2 = make_raw_partition_data(3, vec![make_raw_batch(0, 2)]);

        let resp = make_raw_read_response(9999, vec![pd_a1, pd_b, pd_a2]);
        handle_read_response(resp, &mut sessions, &shared, 0).unwrap();

        let state = shared.lock_state();
        let buf = &state.as_ref().unwrap().buffer;
        assert_eq!(buf.len(), 9);

        let session_sequence: Vec<i64> = buf
            .iter()
            .map(|m| m.commit_marker.partition_session_id)
            .collect();
        assert_eq!(session_sequence, vec![1, 1, 1, 1, 2, 2, 2, 3, 3]);

        let non_zero: Vec<i64> = buf
            .iter()
            .map(|m| m.bytes_to_release)
            .filter(|b| *b != 0)
            .collect();
        assert_eq!(non_zero, vec![9999]);
    }

    #[test]
    fn handle_read_response_skips_empty_batches() {
        let shared = ReaderShared::new();
        let mut sessions: HashMap<i64, PartitionSession> = HashMap::new();
        sessions.insert(1, make_session(1, 11, "t", 0));

        let empty_batch = RawBatch {
            producer_id: "p".to_string(),
            write_session_meta: HashMap::new(),
            codec: RawCodec { code: 1 },
            written_at: SystemTime::UNIX_EPOCH.into(),
            message_data: vec![],
        };
        let pd = make_raw_partition_data(1, vec![empty_batch, make_raw_batch(0, 2)]);
        let resp = make_raw_read_response(500, vec![pd]);

        handle_read_response(resp, &mut sessions, &shared, 0).unwrap();

        let state = shared.lock_state();
        let buf = &state.as_ref().unwrap().buffer;
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0].offset, 0);
        assert_eq!(buf[1].offset, 1);
        assert_eq!(buf[1].bytes_to_release, 500);
    }

    #[test]
    fn handle_read_response_advances_next_commit_offset_start() {
        let shared = ReaderShared::new();
        let mut sessions: HashMap<i64, PartitionSession> = HashMap::new();
        sessions.insert(1, make_session(1, 11, "t", 100));

        let pd = make_raw_partition_data(1, vec![make_raw_batch(100, 3)]);
        let resp = make_raw_read_response(10, vec![pd]);

        handle_read_response(resp, &mut sessions, &shared, 0).unwrap();

        assert_eq!(sessions.get(&1).unwrap().next_commit_offset_start, 103);
    }

    #[test]
    fn handle_read_response_drops_data_for_unknown_session() {
        let shared = ReaderShared::new();
        let mut sessions: HashMap<i64, PartitionSession> = HashMap::new();
        sessions.insert(1, make_session(1, 11, "t", 0));

        let pd_unknown = make_raw_partition_data(2, vec![make_raw_batch(0, 3)]);
        let pd_known = make_raw_partition_data(1, vec![make_raw_batch(0, 2)]);
        let resp = make_raw_read_response(123, vec![pd_unknown, pd_known]);

        handle_read_response(resp, &mut sessions, &shared, 0).unwrap();

        let state = shared.lock_state();
        let buf = &state.as_ref().unwrap().buffer;
        assert_eq!(buf.len(), 2);
        assert!(buf
            .iter()
            .all(|m| m.commit_marker.partition_session_id == 1));
    }

    #[tokio::test]
    async fn handle_read_response_notifies_after_push() {
        let shared = Arc::new(ReaderShared::new());
        let mut sessions: HashMap<i64, PartitionSession> = HashMap::new();
        sessions.insert(1, make_session(1, 11, "t", 0));

        let notified = shared.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let pd = make_raw_partition_data(1, vec![make_raw_batch(0, 2)]);
        let resp = make_raw_read_response(100, vec![pd]);
        handle_read_response(resp, &mut sessions, shared.as_ref(), 0).unwrap();

        tokio::time::timeout(Duration::from_millis(100), notified)
            .await
            .expect("waiter not notified after push");
    }

    // ---- read_batch_private (via TestReader) ----

    struct TestReader {
        sender: UnboundedSender<FromClient>,
        shared: Arc<ReaderShared>,
        batch_size: usize,
    }

    impl TestReader {
        fn new(
            batch_size: usize,
        ) -> (
            Self,
            tokio::sync::mpsc::UnboundedReceiver<FromClient>,
            Arc<ReaderShared>,
        ) {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let shared = Arc::new(ReaderShared::new());
            (
                Self {
                    sender: tx,
                    shared: shared.clone(),
                    batch_size,
                },
                rx,
                shared,
            )
        }

        async fn read_batch_private(&self) -> YdbResult<TopicReaderBatch> {
            loop {
                let notified = self.shared.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();

                let prefix = match &mut *self.shared.lock_state() {
                    Ok(state) => cut_prefix(&mut state.buffer, self.batch_size),
                    Err(err) => return Err(err.clone()),
                };

                if let Some((messages, bytes_to_release)) = prefix {
                    if bytes_to_release > 0 {
                        send_on_stream(
                            &self.sender,
                            RawFromClientOneOf::ReadRequest(RawReadRequest {
                                bytes_size: bytes_to_release,
                            }),
                        )?;
                    }
                    return Ok(TopicReaderBatch::from_messages(messages));
                }

                notified.await;
            }
        }
    }

    #[tokio::test]
    async fn read_batch_private_returns_data_already_in_buffer() {
        let (reader, mut rx, shared) = TestReader::new(1000);
        let mut session = make_session(1, 11, "t", 0);
        {
            let mut state = shared.lock_state();
            let buf = &mut state.as_mut().unwrap().buffer;
            for offset in 0..300i64 {
                let bytes = if offset == 299 { 7777 } else { 0 };
                buf.push_back(message_for_session(&mut session, offset, bytes));
            }
        }

        let batch = reader.read_batch_private().await.unwrap();
        assert_eq!(batch.messages.len(), 300);

        let sent = rx.try_recv().expect("ReadRequest must be sent");
        match sent.client_message.unwrap() {
            ydb_grpc::ydb_proto::topic::stream_read_message::from_client::ClientMessage::ReadRequest(r) => {
                assert_eq!(r.bytes_size, 7777);
            }
            other => panic!("unexpected client message: {:?}", other),
        }
        assert!(rx.try_recv().is_err(), "only one ReadRequest expected");
    }

    #[tokio::test]
    async fn read_batch_private_awaits_notify_then_reads() {
        let (reader, _rx, shared) = TestReader::new(1000);

        let handle =
            tokio::spawn(
                async move { reader.read_batch_private().await.map(|b| b.messages.len()) },
            );

        tokio::time::sleep(Duration::from_millis(20)).await;

        {
            let mut session = make_session(1, 11, "t", 0);
            let mut state = shared.lock_state();
            let buf = &mut state.as_mut().unwrap().buffer;
            buf.push_back(message_for_session(&mut session, 0, 0));
            buf.push_back(message_for_session(&mut session, 1, 0));
        }
        shared.notify.notify_one();

        let res = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("should complete after notify")
            .unwrap()
            .unwrap();
        assert_eq!(res, 2);
    }

    #[tokio::test]
    async fn read_batch_private_returns_error_when_closed_with_notify() {
        let (reader, _rx, shared) = TestReader::new(1000);
        {
            let mut state = shared.lock_state();
            *state = Err(YdbError::custom("boom"));
        }
        shared.notify.notify_one();

        let res = reader.read_batch_private().await;
        match res {
            Err(YdbError::Custom(s)) => assert_eq!(s, "boom"),
            other => panic!("expected Err(Custom(\"boom\")), got {:?}", other.err()),
        }
    }

    #[tokio::test]
    async fn read_batch_private_returns_error_when_closed_without_notify() {
        let (reader, _rx, shared) = TestReader::new(1000);
        {
            let mut state = shared.lock_state();
            *state = Err(YdbError::custom("topic read stream closed"));
        }

        let res = tokio::time::timeout(Duration::from_millis(200), reader.read_batch_private())
            .await
            .expect("should not hang on closed=true without notify");
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn read_batch_private_returns_error_when_closed_even_with_data() {
        let (reader, _rx, shared) = TestReader::new(1000);
        let mut session = make_session(1, 11, "t", 0);
        {
            let mut state = shared.lock_state();
            let buf = &mut state.as_mut().unwrap().buffer;
            for offset in 0..10i64 {
                buf.push_back(message_for_session(&mut session, offset, 0));
            }
        }
        {
            let mut state = shared.lock_state();
            *state = Err(YdbError::custom("topic read stream closed"));
        }

        let res = reader.read_batch_private().await;
        assert!(
            res.is_err(),
            "closed reader must return error even if buffer has data"
        );
    }

    // ---- options ----

    #[test]
    fn topic_reader_options_default_batch_size_is_1000() {
        let opts =
            crate::client_topic::topicreader::reader_options::TopicReaderOptionsBuilder::default()
                .consumer("c".to_string())
                .topic(TopicSelectors::from("t"))
                .build()
                .unwrap();
        assert_eq!(opts.batch_size, 1000);
    }

    #[test]
    fn topic_reader_options_custom_batch_size() {
        let opts =
            crate::client_topic::topicreader::reader_options::TopicReaderOptionsBuilder::default()
                .consumer("c".to_string())
                .topic(TopicSelectors::from("t"))
                .batch_size(42)
                .build()
                .unwrap();
        assert_eq!(opts.batch_size, 42);
    }
}
