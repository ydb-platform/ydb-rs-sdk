use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};

use futures_util::FutureExt;
use tokio::sync::{Notify, oneshot};
use tokio::time::{Instant, sleep_until};
use ydb_grpc::ydb_proto::topic::TransactionIdentity;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::client_topic::compression::CompressionBatch;
use crate::client_topic::topicwriter::capacity_limiter::{AdmittedMessage, CapacityLimiter};
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::message_queue::{
    AppendMessageToSendBufferResult, MessageQueue, QueuedMessage,
};
use crate::client_topic::topicwriter::message_write_status::{
    MessageWriteStatus, WriteAck, accept_any_write_status, expect_transactional_write_status,
};
use crate::client_topic::topicwriter::reception_queue::{ReceptionQueue, ReceptionTicket};
use crate::client_topic::topicwriter::writer_options::{AutoFlushSettings, WriterFlowControl};
use crate::{YdbError, YdbResult};

const WRITER_STATE_MUTEX_POISONED: &str = "topic writer state mutex poisoned";

/// Shared logical writer state that outlives individual gRPC stream attempts.
///
/// Each buffer epoch belongs to one connection task set. A failed transactional connection advances
/// the epoch and retains its error until the Query transaction finishes, preventing stale tasks or
/// later writes from bypassing transaction cleanup. Terminal writer failure stores the error
/// returned by every subsequent operation.
#[derive(Clone)]
pub(crate) struct WriterState {
    inner: Arc<WriterStateInner>,
}

struct WriterStateInner {
    buffer_state: Mutex<WriterBufferState>,
    capacity_limiter: CapacityLimiter,
    auto_flush: AutoFlushSettings,
    new_message_added: Notify,
    flush_requested: Notify,
    transaction_finished: Notify,
}

enum WriterBufferState {
    Active(WriterBuffer),
    Failed(YdbError),
}

struct TransactionBinding {
    identity: Arc<TransactionIdentity>,
    phase: TransactionPhase,
}

enum TransactionPhase {
    /// Transactional messages may still be enqueued.
    Writing,
    /// Message admission is closed while accepted messages flush and the Query transaction finalizes.
    Committing,
    /// StreamWrite failed while bound; transaction completion must clear the empty replacement.
    Failed(Box<YdbError>),
}

impl WriterBufferState {
    fn buffer(&self) -> YdbResult<&WriterBuffer> {
        match self {
            Self::Active(buffer) => Ok(buffer),
            Self::Failed(error) => Err(error.clone()),
        }
    }

    fn buffer_mut(&mut self) -> YdbResult<&mut WriterBuffer> {
        match self {
            Self::Active(buffer) => Ok(buffer),
            Self::Failed(error) => Err(error.clone()),
        }
    }

    fn user_buffer(
        &mut self,
        requested: Option<&Arc<TransactionIdentity>>,
    ) -> YdbResult<&mut WriterBuffer> {
        let buffer = self.buffer_mut()?;

        let Some(requested) = requested else {
            return match &buffer.transaction {
                None => Ok(buffer),
                Some(active) => Err(ordinary_write_disabled_error(&active.identity)),
            };
        };
        let Some(active) = &buffer.transaction else {
            return Err(transaction_inactive_error(requested));
        };
        if !Arc::ptr_eq(&active.identity, requested) {
            return Err(transaction_mismatch_error(&active.identity, requested));
        }
        match &active.phase {
            TransactionPhase::Writing => Ok(buffer),
            TransactionPhase::Committing => Err(transaction_committing_error(requested)),
            TransactionPhase::Failed(error) => Err(error.as_ref().clone()),
        }
    }

    fn transaction_buffer(
        &mut self,
        requested: &Arc<TransactionIdentity>,
    ) -> YdbResult<&mut WriterBuffer> {
        let buffer = self.buffer_mut()?;

        let Some(active) = &buffer.transaction else {
            return Err(transaction_inactive_error(requested));
        };
        if !Arc::ptr_eq(&active.identity, requested) {
            return Err(transaction_mismatch_error(&active.identity, requested));
        }
        if let TransactionPhase::Failed(error) = &active.phase {
            return Err(error.as_ref().clone());
        }

        Ok(buffer)
    }

    fn connection_buffer(&mut self, epoch: usize) -> YdbResult<&mut WriterBuffer> {
        let buffer = self.buffer_mut()?;
        if buffer.epoch != epoch {
            return Err(stale_connection_error(epoch, buffer.epoch));
        }
        Ok(buffer)
    }
}

impl WriterState {
    pub(crate) fn new(auto_seq_no: bool, flow_control: WriterFlowControl) -> YdbResult<Self> {
        let inflight = flow_control.inflight();
        Ok(Self {
            inner: Arc::new(WriterStateInner {
                buffer_state: Mutex::new(WriterBufferState::Active(WriterBuffer::new(
                    0,
                    auto_seq_no,
                    None,
                ))),
                capacity_limiter: CapacityLimiter::new(inflight.messages(), inflight.bytes())?,
                auto_flush: flow_control.auto_flush(),
                new_message_added: Notify::new(),
                flush_requested: Notify::new(),
                transaction_finished: Notify::new(),
            }),
        })
    }

    fn lock_buffer_state(&self) -> YdbResult<MutexGuard<'_, WriterBufferState>> {
        self.inner
            .buffer_state
            .lock()
            .map_err(|_| YdbError::custom(WRITER_STATE_MUTEX_POISONED))
    }

    pub(crate) fn epoch(&self) -> YdbResult<usize> {
        Ok(self.lock_buffer_state()?.buffer()?.epoch)
    }

    pub(crate) fn initialize_last_seq_no(&self, last_seq_no: i64) -> YdbResult<()> {
        let mut state = self.lock_buffer_state()?;
        let buffer = state.buffer_mut()?;
        if buffer.last_seq_no_assigned.is_some() {
            return Err(YdbError::custom(
                "message queue last sequence number is already initialized",
            ));
        }
        buffer.last_seq_no_assigned = Some(last_seq_no);
        Ok(())
    }

    pub(crate) fn add_message(
        &self,
        message: TopicWriterMessage,
        ack_sender: Option<oneshot::Sender<YdbResult<MessageWriteStatus>>>,
    ) -> impl Future<Output = YdbResult<()>> + '_ {
        self.add_message_inner(message, ack_sender, None)
    }

    pub(crate) fn add_transactional_message<'a>(
        &'a self,
        message: TopicWriterMessage,
        ack_sender: Option<oneshot::Sender<YdbResult<MessageWriteStatus>>>,
        transaction: &'a Arc<TransactionIdentity>,
    ) -> impl Future<Output = YdbResult<()>> + 'a {
        self.add_message_inner(message, ack_sender, Some(transaction))
    }

    async fn add_message_inner(
        &self,
        message: TopicWriterMessage,
        ack_sender: Option<oneshot::Sender<YdbResult<MessageWriteStatus>>>,
        transaction: Option<&Arc<TransactionIdentity>>,
    ) -> YdbResult<()> {
        // Reject the wrong writer mode before waiting for flow-control capacity. The binding is
        // checked again after admission because it may change while this future is suspended.
        self.lock_buffer_state()?.user_buffer(transaction)?;
        let admission = self.inner.capacity_limiter.admit(message);
        tokio::pin!(admission);
        let (message, was_blocked) = match admission.as_mut().now_or_never() {
            Some(Ok(message)) => (message, false),
            Some(Err(err)) => return Err(self.failure_or(err, transaction)),
            None => {
                self.inner.flush_requested.notify_one();
                match admission.await {
                    Ok(message) => (message, true),
                    Err(err) => return Err(self.failure_or(err, transaction)),
                }
            }
        };
        {
            let mut state = self.lock_buffer_state()?;
            state
                .user_buffer(transaction)?
                .add_message(message, ack_sender)?;
        }
        self.inner.new_message_added.notify_one();
        if was_blocked {
            // Send this message while later capacity waiters are still blocked.
            self.inner.flush_requested.notify_one();
        }
        Ok(())
    }

    pub(crate) fn bind_transaction(&self, transaction: Arc<TransactionIdentity>) -> YdbResult<()> {
        let mut state = self.lock_buffer_state()?;
        let buffer = state.buffer_mut()?;
        if let Some(bound) = &buffer.transaction {
            return Err(YdbError::custom(format!(
                "topic writer is already bound to transaction: transaction_id={}",
                bound.identity.id,
            )));
        }
        if !buffer.is_empty() {
            return Err(YdbError::custom(
                "cannot bind topic writer to a transaction while messages are pending",
            ));
        }
        buffer.transaction = Some(TransactionBinding {
            identity: transaction,
            phase: TransactionPhase::Writing,
        });
        Ok(())
    }

    pub(crate) fn finish_committed_transaction(
        &self,
        transaction: &Arc<TransactionIdentity>,
    ) -> YdbResult<()> {
        {
            let mut state = self.lock_buffer_state()?;
            let buffer = state.buffer_mut()?;
            if let Some(active) = &buffer.transaction {
                if !Arc::ptr_eq(&active.identity, transaction) {
                    return Err(transaction_mismatch_error(&active.identity, transaction));
                }
                match &active.phase {
                    TransactionPhase::Writing => {
                        return Err(YdbError::custom(format!(
                            "topic writer transaction committed before its commit flush: transaction_id={}",
                            transaction.id,
                        )));
                    }
                    TransactionPhase::Committing if !buffer.is_empty() => {
                        return Err(YdbError::custom(format!(
                            "committed topic transaction still has pending messages: transaction_id={}",
                            transaction.id,
                        )));
                    }
                    TransactionPhase::Committing => {}
                    TransactionPhase::Failed(_) => {
                        // Query commit is dispatched only after this hook's flush succeeds. A
                        // committed transaction can therefore race only with a later StreamWrite
                        // failure, after every transactional message was already acknowledged.
                    }
                }
                buffer.transaction = None;
            }
        }
        self.inner.transaction_finished.notify_one();
        Ok(())
    }

    pub(crate) fn finish_aborted_transaction(
        &self,
        transaction: &Arc<TransactionIdentity>,
        error: YdbError,
    ) -> YdbResult<()> {
        let epoch_advanced = {
            let mut state = self.lock_buffer_state()?;
            let buffer = state.buffer_mut()?;
            if let Some(active) = &buffer.transaction {
                if !Arc::ptr_eq(&active.identity, transaction) {
                    return Err(transaction_mismatch_error(&active.identity, transaction));
                }
                match &active.phase {
                    TransactionPhase::Failed(_) => {
                        buffer.transaction = None;
                        false
                    }
                    TransactionPhase::Writing | TransactionPhase::Committing => {
                        buffer.replace_after_transaction(error);
                        true
                    }
                }
            } else {
                false
            }
        };

        if epoch_advanced {
            self.notify_epoch_advanced();
        }
        self.inner.transaction_finished.notify_one();
        Ok(())
    }

    fn notify_epoch_advanced(&self) {
        self.inner.new_message_added.notify_one();
        self.inner.flush_requested.notify_one();
    }

    pub(crate) fn acknowledge_message(&self, epoch: usize, write_ack: WriteAck) -> YdbResult<()> {
        let transaction_failed = {
            let mut state = self.lock_buffer_state()?;
            let buffer = state.connection_buffer(epoch)?;
            let validator = if buffer.transaction.is_some() {
                expect_transactional_write_status
            } else {
                accept_any_write_status
            };
            match buffer.acknowledge_message(write_ack, validator) {
                Ok(()) => return Ok(()),
                Err(error) => {
                    if buffer.transaction.is_none() {
                        return Err(error);
                    }
                    buffer.replace_after_transaction_failure(error.clone())?;
                    error
                }
            }
        };
        self.notify_epoch_advanced();
        Err(YdbError::Transport(format!(
            "transactional topic write failed: {transaction_failed}",
        )))
    }

    fn append_message_to_send_buffer(
        &self,
        epoch: usize,
        send_buffer: &mut CompressionBatch,
        send_buffer_bytes: &mut usize,
    ) -> YdbResult<AppendMessageToSendBufferResult> {
        let mut state = self.lock_buffer_state()?;
        let buffer = state.connection_buffer(epoch)?;
        let batch_was_empty = send_buffer.messages.is_empty();
        let result = buffer.message_queue.append_message_to_send_buffer(
            &mut send_buffer.messages,
            send_buffer_bytes,
            self.inner.auto_flush,
        );
        if batch_was_empty && !send_buffer.messages.is_empty() {
            send_buffer.transaction = buffer
                .transaction
                .as_ref()
                .map(|binding| binding.identity.clone());
        }
        Ok(result)
    }

    pub(crate) async fn get_messages_to_send(&self, epoch: usize) -> YdbResult<CompressionBatch> {
        let mut batch = CompressionBatch::new();
        let mut message_bytes = 0;

        let timeout = Instant::now() + self.inner.auto_flush.interval();
        loop {
            // Append while we can
            loop {
                match self.append_message_to_send_buffer(epoch, &mut batch, &mut message_bytes)? {
                    AppendMessageToSendBufferResult::Full => return Ok(batch),
                    AppendMessageToSendBufferResult::CouldNotGetMessage => break,
                    AppendMessageToSendBufferResult::UnderThreshold => {}
                }
            }

            // Wait for new messages or timeout
            tokio::select! {
                biased;
                _ = self.inner.flush_requested.notified() => break,
                _ = self.inner.new_message_added.notified() => {}
                _ = sleep_until(timeout) => break,
            }
        }

        Ok(batch)
    }

    pub(crate) fn fail(&self, error: YdbError) -> YdbResult<()> {
        {
            let mut state = self.lock_buffer_state()?;
            match &mut *state {
                WriterBufferState::Active(buffer) => buffer.fail(error.clone()),
                WriterBufferState::Failed(writer_error) => return Err(writer_error.clone()),
            }
            *state = WriterBufferState::Failed(error);
        }
        self.inner.capacity_limiter.close();
        self.inner.new_message_added.notify_waiters();
        self.inner.flush_requested.notify_waiters();
        Ok(())
    }

    /// Resolve buffer ownership after a connection task fails.
    ///
    /// Returns the error only when ordinary writer retry policy must decide whether to reconnect.
    /// Transaction errors remain in [`TransactionPhase::Failed`], while stale errors belong to
    /// an already-abandoned buffer epoch.
    pub(crate) fn handle_connection_failure(
        &self,
        epoch: usize,
        error: YdbError,
    ) -> YdbResult<Option<YdbError>> {
        let retry_error = {
            let mut state = self.lock_buffer_state()?;
            let buffer = state.buffer_mut()?;
            if buffer.epoch != epoch {
                return Ok(None);
            }
            if buffer.transaction.is_none() {
                buffer.message_queue.reset_progress();
                buffer.epoch = buffer.epoch.wrapping_add(1);
                Some(error)
            } else {
                buffer.replace_after_transaction_failure(error)?;
                None
            }
        };
        self.notify_epoch_advanced();
        Ok(retry_error)
    }

    pub(crate) async fn wait_for_failed_transaction_cleanup(&self) -> YdbResult<()> {
        loop {
            let transaction_finished = self.inner.transaction_finished.notified();
            let waiting =
                {
                    let state = self.lock_buffer_state()?;
                    state.buffer()?.transaction.as_ref().is_some_and(|binding| {
                        matches!(&binding.phase, TransactionPhase::Failed(_))
                    })
                };
            if !waiting {
                return Ok(());
            }
            transaction_finished.await;
        }
    }

    pub(crate) async fn flush(&self) -> YdbResult<()> {
        let flush_result_rx = {
            let mut state = self.lock_buffer_state()?;
            state.user_buffer(None)?.reception_queue.init_flush()?
        };
        self.inner.flush_requested.notify_one();
        flush_result_rx.await.map_err(YdbError::from)?
    }

    pub(crate) async fn begin_commit_and_flush(
        &self,
        transaction: &Arc<TransactionIdentity>,
    ) -> YdbResult<()> {
        let flush_result_rx = {
            let mut state = self.lock_buffer_state()?;
            let buffer = state.transaction_buffer(transaction)?;
            let binding = buffer.transaction.as_mut().ok_or_else(|| {
                YdbError::InternalError(format!(
                    "cannot begin topic transaction commit without a transaction binding: transaction_id={}",
                    transaction.id,
                ))
            })?;
            binding.phase = TransactionPhase::Committing;
            buffer.reception_queue.init_flush()?
        };
        self.inner.flush_requested.notify_one();
        flush_result_rx.await.map_err(YdbError::from)?
    }

    pub(crate) fn ensure_not_failed(&self) -> YdbResult<()> {
        self.lock_buffer_state()?.buffer().map(|_| ())
    }

    fn failure_or(
        &self,
        fallback: YdbError,
        transaction: Option<&Arc<TransactionIdentity>>,
    ) -> YdbError {
        let mut state = match self.lock_buffer_state() {
            Ok(state) => state,
            Err(error) => return error,
        };
        match state.user_buffer(transaction) {
            Ok(_) => fallback,
            Err(error) => error,
        }
    }
}

fn ordinary_write_disabled_error(transaction: &Arc<TransactionIdentity>) -> YdbError {
    YdbError::custom(format!(
        "ordinary topic writes are disabled while writer is bound to transaction: transaction_id={}",
        transaction.id,
    ))
}

fn transaction_inactive_error(transaction: &Arc<TransactionIdentity>) -> YdbError {
    YdbError::custom(format!(
        "topic writer transaction is no longer active: transaction_id={}",
        transaction.id,
    ))
}

fn transaction_committing_error(transaction: &Arc<TransactionIdentity>) -> YdbError {
    YdbError::custom(format!(
        "topic writer transaction is already committing: transaction_id={}",
        transaction.id,
    ))
}

fn transaction_mismatch_error(
    active: &Arc<TransactionIdentity>,
    requested: &Arc<TransactionIdentity>,
) -> YdbError {
    YdbError::custom(format!(
        "topic writer is bound to another transaction: active_transaction_id={}, requested_transaction_id={}",
        active.id, requested.id,
    ))
}

fn stale_connection_error(task_epoch: usize, current_epoch: usize) -> YdbError {
    YdbError::Transport(format!(
        "stale topic writer connection epoch: task_epoch={task_epoch}, current_epoch={current_epoch}",
    ))
}

#[cfg(test)]
const TEST_INFLIGHT_MESSAGES: usize = 1000;
#[cfg(test)]
const TEST_INFLIGHT_BYTES: usize = 20 * crate::byte_units::MiB;

#[cfg(test)]
impl WriterState {
    pub(crate) fn for_test() -> Self {
        Self::with_flow_control(
            false,
            TEST_INFLIGHT_MESSAGES,
            TEST_INFLIGHT_BYTES,
            10,
            TEST_INFLIGHT_BYTES,
            std::time::Duration::from_millis(20),
        )
    }

    fn with_flow_control(
        auto_seq_no: bool,
        inflight_messages: usize,
        inflight_bytes: usize,
        auto_flush_messages: usize,
        auto_flush_bytes: usize,
        auto_flush_interval: std::time::Duration,
    ) -> Self {
        Self::new(
            auto_seq_no,
            crate::client_topic::topicwriter::test_helpers::writer_flow_control(
                inflight_messages,
                inflight_bytes,
                auto_flush_messages,
                auto_flush_bytes,
                auto_flush_interval,
            ),
        )
        .unwrap()
    }
}

struct WriterBuffer {
    epoch: usize,
    message_queue: MessageQueue,
    reception_queue: ReceptionQueue,
    auto_seq_no: bool,
    last_seq_no_assigned: Option<i64>,
    transaction: Option<TransactionBinding>,
}

impl WriterBuffer {
    fn new(epoch: usize, auto_seq_no: bool, last_seq_no_assigned: Option<i64>) -> Self {
        Self {
            epoch,
            message_queue: MessageQueue::new(),
            reception_queue: ReceptionQueue::new(),
            auto_seq_no,
            last_seq_no_assigned,
            transaction: None,
        }
    }

    fn replace_after_transaction(&mut self, error: YdbError) {
        let next_epoch = self.epoch.wrapping_add(1);
        let auto_seq_no = self.auto_seq_no;
        let last_seq_no_assigned = self.last_seq_no_assigned;
        self.fail(error);
        *self = Self::new(next_epoch, auto_seq_no, last_seq_no_assigned);
    }

    fn replace_after_transaction_failure(&mut self, error: YdbError) -> YdbResult<()> {
        let Some(transaction) = self.transaction.as_ref() else {
            return Err(YdbError::InternalError(
                "cannot fail transactional topic writes without a transaction binding".to_string(),
            ));
        };
        if matches!(&transaction.phase, TransactionPhase::Failed(_)) {
            return Err(YdbError::InternalError(format!(
                "transactional topic writes are already failed: transaction_id={}",
                transaction.identity.id,
            )));
        }
        let identity = transaction.identity.clone();
        let next_epoch = self.epoch.wrapping_add(1);
        let auto_seq_no = self.auto_seq_no;
        let last_seq_no_assigned = self.last_seq_no_assigned;
        self.fail(error.clone());
        *self = Self::new(next_epoch, auto_seq_no, last_seq_no_assigned);
        self.transaction = Some(TransactionBinding {
            identity,
            phase: TransactionPhase::Failed(Box::new(error)),
        });
        Ok(())
    }

    fn add_message(
        &mut self,
        mut admitted: AdmittedMessage,
        ack_sender: Option<oneshot::Sender<YdbResult<MessageWriteStatus>>>,
    ) -> YdbResult<()> {
        let message = admitted.message_mut();
        let message_seq_no = match (self.auto_seq_no, message.seq_no) {
            (true, Some(_)) => Err(YdbError::custom(
                "explicitly specifying message.seq_no is only allowed if auto_seq_no is disabled",
            )),
            (true, None) => self
                .last_seq_no_assigned
                .ok_or_else(|| {
                    YdbError::custom("message queue last sequence number is not initialized")
                })?
                .checked_add(1)
                .ok_or_else(|| YdbError::custom("message sequence number overflow")),
            (false, Some(seq_no)) => Ok(seq_no),
            (false, None) => Err(YdbError::custom("empty message seq_no is provided")),
        }?;
        message.seq_no = Some(message_seq_no);

        let (message, capacity) = admitted.into_parts();
        let message: MessageData = message.try_into()?;
        let seq_no = message.seq_no;
        self.message_queue
            .add_message(QueuedMessage::new(message, capacity))?;
        self.reception_queue
            .add_ticket(ReceptionTicket::new(seq_no, ack_sender));
        self.last_seq_no_assigned = Some(message_seq_no);

        Ok(())
    }

    fn acknowledge_message(
        &mut self,
        write_ack: WriteAck,
        status_validator: fn(MessageWriteStatus) -> YdbResult<MessageWriteStatus>,
    ) -> YdbResult<()> {
        let Some(ticket_seq_no) = self.reception_queue.peek_ticket_seq_no() else {
            return Err(YdbError::custom(
                "expected reception ticket to be actually present",
            ));
        };
        let ack_seq_no = write_ack.seq_no;

        if ticket_seq_no != ack_seq_no {
            return Err(YdbError::custom(format!(
                "reception ticket and write ack seq_no mismatch: ack_seq_no: {ack_seq_no}, ticket_seq_no: {ticket_seq_no}",
            )));
        }

        self.message_queue.acknowledge_message(ticket_seq_no)?;

        let Some(ticket) = self.reception_queue.pop_ticket() else {
            return Err(YdbError::custom(
                "reception ticket is missing after message queue ack",
            ));
        };
        let status_result = status_validator(write_ack.status);
        let flush_error = status_result.as_ref().err().cloned();
        let result = status_result.as_ref().map(|_| ()).map_err(Clone::clone);
        ticket.send_result_if_needed(status_result);
        self.reception_queue
            .notify_ticket_processed(ticket_seq_no, flush_error);

        result
    }

    fn fail(&mut self, error: YdbError) {
        self.reception_queue.send_error_to_tickets_and_clear(error);
    }

    fn is_empty(&self) -> bool {
        self.message_queue.is_empty() && self.reception_queue.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures_util::FutureExt;
    use tokio::time::timeout;

    use super::*;
    use crate::client_topic::topicwriter::test_helpers::write_ack;

    fn create_message(seq_no: i64, data: Vec<u8>) -> TopicWriterMessage {
        TopicWriterMessage::builder()
            .data(data)
            .seq_no(seq_no)
            .build()
    }

    fn transaction() -> Arc<TransactionIdentity> {
        Arc::new(TransactionIdentity {
            id: "transaction".to_string(),
            session: "session".to_string(),
        })
    }

    #[tokio::test]
    async fn capacity_is_held_until_acknowledgement() {
        let q = WriterState::with_flow_control(false, 2, 5, 1, 5, Duration::ZERO);
        q.add_message(create_message(1, vec![0; 3]), None)
            .await
            .unwrap();
        let messages = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();
        assert_eq!(messages.messages.len(), 1);

        let mut blocked_write = Box::pin(q.add_message(create_message(2, vec![0; 3]), None));
        assert!(blocked_write.as_mut().now_or_never().is_none());

        q.acknowledge_message(q.epoch().unwrap(), write_ack(1))
            .unwrap();
        blocked_write.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn capacity_waiter_flushes_partial_batches() {
        let q = WriterState::with_flow_control(false, 10, 10, 10, 10, Duration::from_secs(3600));
        q.add_message(create_message(1, vec![0; 7]), None)
            .await
            .unwrap();

        let mut blocked_write = Box::pin(q.add_message(create_message(2, vec![0; 4]), None));
        assert!(blocked_write.as_mut().now_or_never().is_none());

        let first_batch = timeout(
            Duration::from_millis(100),
            q.get_messages_to_send(q.epoch().unwrap()),
        )
        .await
        .expect("capacity pressure must flush the buffered partial batch")
        .expect("queue must remain available");
        assert_eq!(first_batch.messages.len(), 1);
        assert_eq!(first_batch.messages[0].seq_no, 1);

        q.acknowledge_message(q.epoch().unwrap(), write_ack(1))
            .unwrap();
        blocked_write.await.unwrap();

        let second_batch = timeout(
            Duration::from_millis(100),
            q.get_messages_to_send(q.epoch().unwrap()),
        )
        .await
        .expect("the admitted capacity waiter must flush without waiting for the interval")
        .expect("queue must remain available");
        assert_eq!(second_batch.messages.len(), 1);
        assert_eq!(second_batch.messages[0].seq_no, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn capacity_waiters_keep_flushing_partial_batches() {
        let q = WriterState::with_flow_control(false, 10, 10, 10, 10, Duration::from_secs(3600));
        q.add_message(create_message(1, vec![0; 10]), None)
            .await
            .unwrap();

        let first_batch = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();
        assert_eq!(first_batch.messages.len(), 1);
        assert_eq!(first_batch.messages[0].seq_no, 1);

        let mut second_write = Box::pin(q.add_message(create_message(2, vec![0; 7]), None));
        assert!(second_write.as_mut().now_or_never().is_none());
        let mut third_write = Box::pin(q.add_message(create_message(3, vec![0; 7]), None));
        assert!(third_write.as_mut().now_or_never().is_none());

        let empty_batch = timeout(
            Duration::from_millis(100),
            q.get_messages_to_send(q.epoch().unwrap()),
        )
        .await
        .expect("capacity waiters must request a flush")
        .expect("queue must remain available");
        assert!(empty_batch.is_empty());

        q.acknowledge_message(q.epoch().unwrap(), write_ack(1))
            .unwrap();
        second_write.await.unwrap();

        let second_batch = timeout(
            Duration::from_millis(100),
            q.get_messages_to_send(q.epoch().unwrap()),
        )
        .await
        .expect("the admitted capacity waiter must request another flush")
        .expect("queue must remain available");
        assert_eq!(second_batch.messages.len(), 1);
        assert_eq!(second_batch.messages[0].seq_no, 2);

        assert!(third_write.as_mut().now_or_never().is_none());
        q.acknowledge_message(q.epoch().unwrap(), write_ack(2))
            .unwrap();
        third_write.await.unwrap();
    }

    #[tokio::test]
    async fn get_messages_to_send_flushes_at_byte_threshold() {
        let q = WriterState::with_flow_control(false, 10, 10, 10, 5, Duration::from_secs(3600));
        let q_collect = q.clone();
        let collect_handle = tokio::spawn(async move {
            q_collect
                .get_messages_to_send(q_collect.epoch().unwrap())
                .await
                .unwrap()
        });

        q.add_message(create_message(1, vec![0; 2]), None)
            .await
            .unwrap();
        q.add_message(create_message(2, vec![0; 3]), None)
            .await
            .unwrap();

        let messages = timeout(Duration::from_millis(100), collect_handle)
            .await
            .expect("byte threshold must flush without waiting for the interval")
            .expect("collector task must not panic");
        assert_eq!(messages.messages.len(), 2);
        assert_eq!(messages.messages[0].seq_no, 1);
        assert_eq!(messages.messages[1].seq_no, 2);
    }

    #[tokio::test]
    async fn get_messages_to_send_moves_batch_to_sent_and_can_ack() {
        let q = Arc::new(WriterState::for_test());

        let q_collect = Arc::clone(&q);
        let collect_handle = tokio::spawn(async move {
            q_collect
                .get_messages_to_send(q_collect.epoch().unwrap())
                .await
                .unwrap()
        });
        q.add_message(create_message(1, vec![10]), None)
            .await
            .unwrap();
        q.add_message(create_message(2, vec![20]), None)
            .await
            .unwrap();

        let batch = collect_handle.await.unwrap();
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.messages[0].seq_no, 1);
        assert_eq!(batch.messages[1].seq_no, 2);

        q.acknowledge_message(q.epoch().unwrap(), write_ack(1))
            .unwrap();
        q.acknowledge_message(q.epoch().unwrap(), write_ack(2))
            .unwrap();
    }

    #[tokio::test]
    async fn get_messages_to_send_empty_queue_times_out_empty() {
        let q = WriterState::for_test();
        let msgs = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();
        assert!(msgs.is_empty());
    }

    #[tokio::test]
    async fn get_messages_to_send_drains_messages_added_before_call() {
        let q = WriterState::for_test();
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        q.add_message(create_message(2, vec![]), None)
            .await
            .unwrap();

        let msgs = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();

        assert_eq!(msgs.messages.len(), 2);
        assert_eq!(msgs.messages[0].seq_no, 1);
        assert_eq!(msgs.messages[1].seq_no, 2);
    }

    #[tokio::test]
    async fn get_messages_to_send_with_zero_duration_drains_messages() {
        let q = WriterState::with_flow_control(
            false,
            TEST_INFLIGHT_MESSAGES,
            TEST_INFLIGHT_BYTES,
            10,
            TEST_INFLIGHT_BYTES,
            Duration::ZERO,
        );
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();

        let msgs = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();

        assert_eq!(msgs.messages.len(), 1);
        assert_eq!(msgs.messages[0].seq_no, 1);
    }

    #[tokio::test]
    async fn get_messages_to_send_collects_messages_added_during_call() {
        let q = Arc::new(WriterState::for_test());
        let q_collect = Arc::clone(&q);
        let collect_handle = tokio::spawn(async move {
            q_collect
                .get_messages_to_send(q_collect.epoch().unwrap())
                .await
                .unwrap()
        });
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();

        let msgs = collect_handle.await.unwrap();
        assert_eq!(msgs.messages.len(), 1);
        assert_eq!(msgs.messages[0].seq_no, 1);
    }

    #[tokio::test]
    async fn get_messages_to_send_respects_threshold() {
        let q = Arc::new(WriterState::with_flow_control(
            false,
            TEST_INFLIGHT_MESSAGES,
            TEST_INFLIGHT_BYTES,
            2,
            TEST_INFLIGHT_BYTES,
            Duration::from_millis(50),
        ));
        let q_collect = Arc::clone(&q);
        let collect_handle = tokio::spawn(async move {
            q_collect
                .get_messages_to_send(q_collect.epoch().unwrap())
                .await
                .unwrap()
        });
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        q.add_message(create_message(2, vec![]), None)
            .await
            .unwrap();
        q.add_message(create_message(3, vec![]), None)
            .await
            .unwrap();

        let msgs = collect_handle.await.unwrap();
        assert_eq!(msgs.messages.len(), 2);
        assert_eq!(msgs.messages[0].seq_no, 1);
        assert_eq!(msgs.messages[1].seq_no, 2);
    }

    #[tokio::test]
    async fn get_messages_to_send_second_call_drains_remaining() {
        let q = Arc::new(WriterState::with_flow_control(
            false,
            TEST_INFLIGHT_MESSAGES,
            TEST_INFLIGHT_BYTES,
            2,
            TEST_INFLIGHT_BYTES,
            Duration::from_millis(50),
        ));
        let q1 = Arc::clone(&q);
        let h1 =
            tokio::spawn(
                async move { q1.get_messages_to_send(q1.epoch().unwrap()).await.unwrap() },
            );
        q.add_message(create_message(11, vec![]), None)
            .await
            .unwrap();
        q.add_message(create_message(12, vec![]), None)
            .await
            .unwrap();
        q.add_message(create_message(13, vec![]), None)
            .await
            .unwrap();
        let first = h1.await.unwrap();
        assert_eq!(first.messages.len(), 2);

        let q2 = Arc::clone(&q);
        let h2 =
            tokio::spawn(
                async move { q2.get_messages_to_send(q2.epoch().unwrap()).await.unwrap() },
            );
        let second = h2.await.unwrap();
        assert_eq!(second.messages.len(), 1);
        assert_eq!(second.messages[0].seq_no, 13);
    }

    #[tokio::test]
    async fn acknowledge_message_returns_error_when_reception_ticket_not_present() {
        let q = WriterState::for_test();

        let err = q
            .acknowledge_message(q.epoch().unwrap(), write_ack(8))
            .unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("expected reception ticket to be actually present"));
    }

    #[tokio::test]
    async fn acknowledge_message_errors_when_seq_no_mismatches() {
        let q = WriterState::for_test();
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        let messages = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();
        assert_eq!(messages.messages.len(), 1);

        let err = q
            .acknowledge_message(q.epoch().unwrap(), write_ack(99))
            .unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("reception ticket and write ack seq_no mismatch"));
        assert!(err_msg.contains("ack_seq_no: 99"));
        assert!(err_msg.contains("ticket_seq_no: 1"));
    }

    #[tokio::test]
    async fn reset_progress_restores_sent_messages_to_pending() {
        let q = WriterState::for_test();
        let failed_epoch = q.epoch().unwrap();
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        q.add_message(create_message(2, vec![]), None)
            .await
            .unwrap();
        let first_batch = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();
        assert_eq!(first_batch.messages.len(), 2);

        q.handle_connection_failure(
            failed_epoch,
            YdbError::Transport("test failure".to_string()),
        )
        .unwrap();
        assert_eq!(q.epoch().unwrap(), failed_epoch + 1);

        let msgs = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();
        assert_eq!(msgs.messages.len(), 2);
        assert_eq!(msgs.messages[0].seq_no, 1);
        assert_eq!(msgs.messages[1].seq_no, 2);
    }

    #[tokio::test]
    async fn flush_returns_status_validation_error_observed_before_flush() {
        let q = WriterState::for_test();
        let transaction = transaction();
        q.bind_transaction(transaction.clone()).unwrap();
        q.add_transactional_message(create_message(1, vec![]), None, &transaction)
            .await
            .unwrap();
        let messages = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();
        assert_eq!(messages.messages.len(), 1);

        q.acknowledge_message(q.epoch().unwrap(), write_ack(1))
            .unwrap_err();

        assert!(q.begin_commit_and_flush(&transaction).await.is_err());
    }

    #[tokio::test]
    async fn flush_returns_status_validation_error_observed_during_flush() {
        let q = WriterState::for_test();
        let transaction = transaction();
        q.bind_transaction(transaction.clone()).unwrap();
        q.add_transactional_message(create_message(1, vec![]), None, &transaction)
            .await
            .unwrap();
        let messages = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();
        assert_eq!(messages.messages.len(), 1);

        let mut flush = Box::pin(q.begin_commit_and_flush(&transaction));
        assert!(flush.as_mut().now_or_never().is_none());

        q.acknowledge_message(q.epoch().unwrap(), write_ack(1))
            .unwrap_err();

        assert!(flush.await.is_err());
    }

    #[tokio::test]
    async fn flush_returns_error_when_reception_tickets_fail_during_wait() {
        let q = WriterState::for_test();
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        let messages = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();
        assert_eq!(messages.messages.len(), 1);

        let mut flush = Box::pin(q.flush());
        assert!(flush.as_mut().now_or_never().is_none());

        q.fail(YdbError::custom("fatal writer error")).unwrap();

        let result = timeout(Duration::from_millis(100), flush)
            .await
            .expect("flush must finish after reception tickets fail");

        let err = result.unwrap_err();
        assert!(err.to_string().contains("fatal writer error"));
    }

    #[tokio::test]
    async fn terminal_failure_is_returned_by_all_operations() {
        let q = WriterState::for_test();
        let (ack_tx, ack_rx) = oneshot::channel();
        q.add_message(create_message(1, vec![]), Some(ack_tx))
            .await
            .unwrap();

        q.fail(YdbError::custom("terminal writer error")).unwrap();

        let write_err = q
            .add_message(create_message(2, vec![]), None)
            .await
            .unwrap_err();
        let flush_err = q.flush().await.unwrap_err();
        let ack_err = ack_rx.await.unwrap().unwrap_err();

        for err in [write_err, flush_err, ack_err] {
            assert!(err.to_string().contains("terminal writer error"));
        }
    }

    #[tokio::test]
    async fn terminal_failure_wakes_capacity_waiters_with_stored_error() {
        let q = WriterState::with_flow_control(false, 1, 1, 1, 1, Duration::ZERO);
        q.add_message(create_message(1, vec![0]), None)
            .await
            .unwrap();

        let mut waiting = Box::pin(q.add_message(create_message(2, vec![0]), None));
        assert!(waiting.as_mut().now_or_never().is_none());
        q.fail(YdbError::custom("terminal writer error")).unwrap();

        let err = waiting.await.unwrap_err();
        assert!(err.to_string().contains("terminal writer error"));
    }

    #[tokio::test]
    async fn transaction_binding_rejects_ordinary_messages() {
        let q = WriterState::for_test();
        let transaction = transaction();
        q.bind_transaction(transaction.clone()).unwrap();

        let error = q
            .add_message(create_message(1, vec![]), None)
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("ordinary topic writes are disabled")
        );
        assert!(
            q.get_messages_to_send(q.epoch().unwrap())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn transaction_binding_rejects_messages_from_another_identity() {
        let q = WriterState::for_test();
        let transaction = transaction();
        let another_transaction = Arc::new(TransactionIdentity {
            id: "another transaction".to_string(),
            session: "another session".to_string(),
        });
        q.bind_transaction(transaction.clone()).unwrap();

        let error = q
            .add_transactional_message(create_message(1, vec![]), None, &another_transaction)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("bound to another transaction"));
        assert!(
            q.get_messages_to_send(q.epoch().unwrap())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn transaction_flush_closes_message_admission() {
        let q = WriterState::for_test();
        let transaction = transaction();
        q.bind_transaction(transaction.clone()).unwrap();

        q.begin_commit_and_flush(&transaction).await.unwrap();
        let error = q
            .add_transactional_message(create_message(1, vec![]), None, &transaction)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("already committing"));
    }

    #[tokio::test]
    async fn transaction_cleanup_replaces_buffer_immediately() {
        let q = WriterState::with_flow_control(false, 1, 1, 1, 1, Duration::ZERO);
        let transaction = transaction();
        q.bind_transaction(transaction.clone()).unwrap();
        let transaction_epoch = q.epoch().unwrap();

        let (ack_tx, ack_rx) = oneshot::channel();
        q.add_transactional_message(create_message(1, vec![0]), Some(ack_tx), &transaction)
            .await
            .unwrap();
        let batch = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();
        assert!(Arc::ptr_eq(
            batch.transaction.as_ref().unwrap(),
            &transaction,
        ));

        q.finish_aborted_transaction(
            &transaction,
            YdbError::custom("transaction attempt aborted"),
        )
        .unwrap();
        let ack_error = ack_rx.await.unwrap().unwrap_err();
        assert!(
            ack_error
                .to_string()
                .contains("transaction attempt aborted")
        );

        q.add_message(create_message(2, vec![0]), None)
            .await
            .unwrap();
        assert_eq!(q.epoch().unwrap(), transaction_epoch + 1);

        let ordinary_batch = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();
        assert!(ordinary_batch.transaction.is_none());
        assert_eq!(ordinary_batch.messages[0].seq_no, 2);
    }

    #[tokio::test]
    async fn transaction_cleanup_wakes_stale_message_job() {
        let q = Arc::new(WriterState::with_flow_control(
            false,
            TEST_INFLIGHT_MESSAGES,
            TEST_INFLIGHT_BYTES,
            10,
            TEST_INFLIGHT_BYTES,
            Duration::from_secs(3600),
        ));
        let transaction = transaction();
        q.bind_transaction(transaction.clone()).unwrap();
        let stale_epoch = q.epoch().unwrap();
        let worker_state = Arc::clone(&q);
        let message_job = tokio::spawn(async move {
            loop {
                let batch = worker_state.get_messages_to_send(stale_epoch).await?;
                if !batch.is_empty() {
                    return Ok::<(), YdbError>(());
                }
            }
        });
        tokio::task::yield_now().await;

        q.finish_aborted_transaction(
            &transaction,
            YdbError::custom("transaction attempt aborted"),
        )
        .unwrap();

        let result = timeout(Duration::from_millis(100), message_job)
            .await
            .expect("transaction cleanup must wake the stale message job")
            .expect("message job must not panic");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn transaction_connection_failure_is_preserved_until_cleanup() {
        let q = WriterState::for_test();
        let transaction = transaction();
        q.bind_transaction(transaction.clone()).unwrap();
        let failed_epoch = q.epoch().unwrap();
        q.add_transactional_message(create_message(1, vec![]), None, &transaction)
            .await
            .unwrap();
        q.get_messages_to_send(failed_epoch).await.unwrap();
        q.add_transactional_message(create_message(2, vec![]), None, &transaction)
            .await
            .unwrap();

        q.handle_connection_failure(
            failed_epoch,
            YdbError::Transport("stream failed".to_string()),
        )
        .unwrap();

        let write_error = q
            .add_transactional_message(create_message(3, vec![]), None, &transaction)
            .await
            .unwrap_err();
        assert!(write_error.to_string().contains("stream failed"));
        let flush_error = q.begin_commit_and_flush(&transaction).await.unwrap_err();
        assert!(flush_error.to_string().contains("stream failed"));

        let mut failed_transaction_cleanup = Box::pin(q.wait_for_failed_transaction_cleanup());
        assert!(failed_transaction_cleanup.as_mut().now_or_never().is_none());
        q.finish_aborted_transaction(
            &transaction,
            YdbError::custom("transaction attempt aborted"),
        )
        .unwrap();
        failed_transaction_cleanup.await.unwrap();
        assert_eq!(q.epoch().unwrap(), failed_epoch + 1);

        let empty = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();
        assert!(empty.is_empty());

        q.add_message(create_message(4, vec![]), None)
            .await
            .unwrap();
        let batch = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();
        assert!(batch.transaction.is_none());
        assert_eq!(batch.messages[0].seq_no, 4);
    }

    #[tokio::test]
    async fn committed_transaction_releases_failed_writer() {
        let q = WriterState::for_test();
        let transaction = transaction();
        q.bind_transaction(transaction.clone()).unwrap();
        q.add_transactional_message(create_message(1, vec![]), None, &transaction)
            .await
            .unwrap();
        q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();
        q.acknowledge_message(
            q.epoch().unwrap(),
            WriteAck {
                seq_no: 1,
                status: MessageWriteStatus::WrittenInTx(
                    crate::client_topic::topicwriter::message_write_status::MessageWriteInTxInfo {},
                ),
            },
        )
        .unwrap();
        q.begin_commit_and_flush(&transaction).await.unwrap();
        let failed_epoch = q.epoch().unwrap();

        q.handle_connection_failure(
            failed_epoch,
            YdbError::Transport("stream failed".to_string()),
        )
        .unwrap();

        let mut failed_transaction_cleanup = Box::pin(q.wait_for_failed_transaction_cleanup());
        assert!(failed_transaction_cleanup.as_mut().now_or_never().is_none());

        q.finish_committed_transaction(&transaction).unwrap();
        failed_transaction_cleanup.await.unwrap();

        q.add_message(create_message(2, vec![]), None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn stale_connection_cannot_dequeue_or_ack_replacement_buffer() {
        let q = WriterState::for_test();
        let transaction = transaction();
        q.bind_transaction(transaction.clone()).unwrap();
        let stale_epoch = q.epoch().unwrap();
        q.finish_aborted_transaction(
            &transaction,
            YdbError::custom("transaction attempt aborted"),
        )
        .unwrap();

        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        let current_epoch = q.epoch().unwrap();
        q.handle_connection_failure(
            stale_epoch,
            YdbError::Transport("stale stream failed".to_string()),
        )
        .unwrap();
        assert_eq!(q.epoch().unwrap(), current_epoch);

        let dequeue_error = match q.get_messages_to_send(stale_epoch).await {
            Ok(_) => panic!("stale connection must not dequeue replacement messages"),
            Err(error) => error,
        };
        assert!(
            dequeue_error
                .to_string()
                .contains("stale topic writer connection epoch")
        );

        let batch = q.get_messages_to_send(current_epoch).await.unwrap();
        assert_eq!(batch.messages.len(), 1);
        let ack_error = q
            .acknowledge_message(stale_epoch, write_ack(1))
            .unwrap_err();
        assert!(
            ack_error
                .to_string()
                .contains("stale topic writer connection epoch")
        );
        q.acknowledge_message(current_epoch, write_ack(1)).unwrap();
    }

    #[tokio::test]
    async fn cleanup_rejects_another_transaction_without_changing_it() {
        let q = WriterState::for_test();
        let old_transaction = transaction();
        let new_transaction = Arc::new(TransactionIdentity {
            id: "new transaction".to_string(),
            session: "new session".to_string(),
        });
        q.bind_transaction(old_transaction.clone()).unwrap();
        q.finish_aborted_transaction(
            &old_transaction,
            YdbError::custom("transaction attempt aborted"),
        )
        .unwrap();
        q.bind_transaction(new_transaction.clone()).unwrap();

        assert!(
            q.finish_aborted_transaction(
                &old_transaction,
                YdbError::custom("late transaction cleanup"),
            )
            .is_err()
        );
        q.add_transactional_message(create_message(1, vec![]), None, &new_transaction)
            .await
            .unwrap();

        let batch = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();
        assert!(Arc::ptr_eq(
            batch.transaction.as_ref().unwrap(),
            &new_transaction,
        ));
    }

    #[tokio::test]
    async fn committed_transaction_buffer_becomes_ordinary() {
        let q = WriterState::for_test();
        let transaction = transaction();
        q.bind_transaction(transaction.clone()).unwrap();

        let error = q.finish_committed_transaction(&transaction).unwrap_err();
        assert!(error.to_string().contains("before its commit flush"));

        q.begin_commit_and_flush(&transaction).await.unwrap();
        q.finish_committed_transaction(&transaction).unwrap();

        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        let batch = q.get_messages_to_send(q.epoch().unwrap()).await.unwrap();

        assert!(batch.transaction.is_none());
    }
}
