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
use futures_util::Future;
use secrecy::ExposeSecret;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
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

type TopicCommitAckSender = tokio::sync::oneshot::Sender<YdbResult<()>>;
type TopicCommitAckReceiver = tokio::sync::oneshot::Receiver<YdbResult<()>>;

type PartitionPendingCommits = BTreeMap<std::cmp::Reverse<i64>, TopicCommitAckSender>;

#[derive(Default)]
struct PendingCommits {
    // NOTE: Reverse keeps all offsets covered by a server ack in the right side
    // of split_off(&Reverse(committed_offset)): real end_offset <= committed_offset.
    sessions: HashMap<PartitionSessionId, PartitionPendingCommits>,
}

impl PendingCommits {
    fn push(
        &mut self,
        partition_session_id: PartitionSessionId,
        committed_offset: i64,
    ) -> TopicCommitAckReceiver {
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

    fn fail_all(&mut self, err: &YdbError) {
        let sessions = std::mem::take(&mut self.sessions);

        for session in sessions.into_values() {
            Self::fail_commits(session, err);
        }
    }

    fn fail_session(&mut self, partition_session_id: PartitionSessionId, err: &YdbError) {
        if let Some(session) = self.sessions.remove(&partition_session_id) {
            Self::fail_commits(session, err);
        }
    }

    fn stop(
        &mut self,
        partition_session_id: PartitionSessionId,
        committed_offset: Option<i64>,
        err: &YdbError,
    ) {
        if let Some(committed_offset) = committed_offset {
            self.ack_partition(partition_session_id, committed_offset);
        }

        self.fail_session(partition_session_id, err);
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

    fn ack_commits(commits: PartitionPendingCommits) {
        for sender in commits.into_values() {
            let _ = sender.send(Ok(()));
        }
    }

    fn fail_commits(commits: PartitionPendingCommits, err: &YdbError) {
        for sender in commits.into_values() {
            let _ = sender.send(Err(err.clone()));
        }
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
            state.pending_commits.fail_all(&err);
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

    epoch: usize,
    reader_id: usize,
}

pub struct TopicReader {
    context: TopicReaderContext,

    reader: StreamReader,
}

static READER_IDS: AtomicUsize = AtomicUsize::new(0);

impl TopicReader {
    fn new_reader_id() -> usize {
        READER_IDS.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) async fn new(
        options: TopicReaderOptions,
        manager: GrpcConnectionManager,
        token_cache: TokenCache,
    ) -> YdbResult<Self> {
        let context = TopicReaderContext {
            manager,
            options,
            token_cache,
            epoch: 0,
            reader_id: Self::new_reader_id(),
        };

        let reader = StreamReader::new(&context).await?;

        Ok(Self { context, reader })
    }

    pub async fn read_batch(&mut self) -> YdbResult<TopicReaderBatch> {
        loop {
            match self.reader.read_batch().await {
                Ok(batch) => return Ok(batch),
                Err(err) => self.try_reconnect_on_err(err).await?,
            }
        }
    }

    /// WARN: DO NOT USE IN PRODUCTION
    ///
    /// Read a batch of messages within a transaction context.
    /// The TopicReaderBatch from the result will be committed within the `tx` transaction.
    /// This is an EXAMPLE of the interface. IT IS NOT PRODUCTION READY.
    /// The reader will fail consistently on ANY error, including TLI.
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

    /// Sends a commit for the given [`TopicReaderCommitMarker`] to the server.
    ///
    /// Returns as soon as the commit message has been queued for sending; it does
    /// not wait for the server to acknowledge the commit. Use
    /// [`commit_with_ack`](Self::commit_with_ack) if you need that guarantee.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit message could not be queued (for example,
    /// the reader has been closed).
    pub fn commit(&mut self, commit_marker: TopicReaderCommitMarker) -> YdbResult<()> {
        self.reader.commit(commit_marker).map(|_| ())
    }

    /// Sends a commit for the given [`TopicReaderCommitMarker`] and returns a
    /// handle that resolves once the server acknowledges it.
    ///
    /// `.await` the returned handle to wait for the acknowledgement; it
    /// resolves to `Ok(())` when the server acks the commit.
    ///
    /// # Errors
    ///
    /// The handle resolves to an error if the acknowledgement will never
    /// arrive — either because the initial send failed, or because the reader
    /// was closed before the server replied.
    pub fn commit_with_ack(
        &mut self,
        commit_marker: TopicReaderCommitMarker,
    ) -> impl Future<Output = YdbResult<()>> {
        let handler = self.reader.commit(commit_marker);

        async {
            let handler = handler?;

            match handler.await {
                Ok(res) => res,
                Err(_) => Err(YdbError::Custom(
                    "commit channel was closed without err msg".to_string(),
                )),
            }
        }
    }

    async fn try_reconnect_on_err(&mut self, err: YdbError) -> YdbResult<()> {
        self.ensure_retriable(err)?;

        self.reader.cancel().await;
        self.context.epoch += 1;

        let mut attempts: usize = 0;
        let start = std::time::Instant::now();

        let reader = loop {
            attempts += 1;

            match tokio::time::timeout(RECONNECT_ATTEMPT_TIMEOUT, StreamReader::new(&self.context))
                .await
            {
                Ok(Ok(reader)) => break reader,
                Ok(Err(err)) => self.ensure_retriable(err)?,
                Err(_) => {
                    debug!(
                        consumer = self.context.options.consumer,
                        epoch = self.context.epoch,
                        attempt = attempts,
                        "topic reader reconnect attempt timed out"
                    );
                }
            };

            tokio::time::sleep(topic_reader_retry_backoff(attempts)).await;
        };

        info!(
            consumer = self.context.options.consumer,
            epoch = self.context.epoch,
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
                    epoch = self.context.epoch,
                    err = %err,
                    "topic reader error is retriable, reconnecting"
                );
                Ok(())
            }
            NeedRetry::False => {
                error!(
                    consumer = self.context.options.consumer,
                    epoch = self.context.epoch,
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
    reader_id: usize,
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

    fn commit(
        &mut self,
        commit_marker: TopicReaderCommitMarker,
    ) -> YdbResult<TopicCommitAckReceiver> {
        if self.epoch != commit_marker.epoch {
            return Err(YdbError::Custom(
                "commit belongs to previous connection".to_string(),
            ));
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

    pub(crate) async fn new(context: &TopicReaderContext) -> YdbResult<Self> {
        let (stream, topic_service) =
            Self::grpc_connect(&context.manager, &context.options).await?;

        let mut stream_reader = StreamReader {
            stream_sender: stream.clone_sender(),
            shared: Arc::new(ReaderShared::new()),
            stop_background_work_token: YdbCancellationToken::new(),
            reader_id: context.reader_id,
            epoch: context.epoch,
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
            self.reader_id,
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
    reader_id: usize,
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
                            reader_id,
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
    reader_id: usize,
    epoch: usize,
) -> YdbResult<()> {
    match msg {
        RawFromServer::ReadResponse(resp) => {
            handle_read_response(resp, sessions, shared, reader_id, epoch)?
        }
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
                    state.pending_commits.stop(
                        request.partition_session_id,
                        None,
                        &YdbError::Custom(format!(
                            "partition session {} stopped by server",
                            request.partition_session_id,
                        )),
                    );
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
    reader_id: usize,
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
            let batch = TopicReaderBatch::new(raw_batch, session, reader_id, epoch);
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

impl TopicSelector {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            partition_ids: None,
            read_from: Some(UNIX_EPOCH),
        }
    }
}

impl<S: Into<String>> From<S> for TopicSelector {
    fn from(path: S) -> Self {
        Self::new(path)
    }
}

impl<S: Into<TopicSelector>> From<S> for TopicSelectors {
    fn from(s: S) -> Self {
        Self(vec![s.into()])
    }
}

impl<S: Into<TopicSelector>> FromIterator<S> for TopicSelectors {
    fn from_iter<I: IntoIterator<Item = S>>(iter: I) -> Self {
        Self(iter.into_iter().map(Into::into).collect())
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
    use oneshot::error::TryRecvError;

    use super::*;

    #[test]
    fn pending_commits_guarantees() {
        let mut pending = PendingCommits::default();

        let mut ack0_0 = pending.push(0, 0);
        let mut ack0_1 = pending.push(0, 1);
        let mut ack0_2 = pending.push(0, 2);

        let mut ack1_0 = pending.push(1, 0);

        // NOTE: Here after grpc messages are precessed, messages contains inclusive end offset,
        // which matches with commit end offset.
        pending.ack([(0, 1)]);

        assert!(ack0_0.try_recv().is_ok());
        assert!(ack0_1.try_recv().is_ok());

        assert!(matches!(ack0_2.try_recv(), Err(TryRecvError::Empty)));

        assert!(matches!(ack1_0.try_recv(), Err(TryRecvError::Empty)));

        pending.fail_session(1, &YdbError::custom("fail"));

        assert!(matches!(ack1_0.try_recv(), Ok(Err(_))));

        drop(pending);

        assert!(matches!(ack0_2.try_recv(), Err(TryRecvError::Closed)));
    }
}
