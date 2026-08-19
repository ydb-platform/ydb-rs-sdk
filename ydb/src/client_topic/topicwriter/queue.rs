use std::sync::{Arc, Mutex, MutexGuard};

use futures_util::FutureExt;
use tokio::sync::{Notify, oneshot};
use tokio::time::{Instant, sleep_until};
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::client_topic::topicwriter::capacity_limiter::{AdmittedMessage, CapacityLimiter};
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::message_queue::{
    AppendMessageToSendBufferResult, MessageQueue, QueuedMessage,
};
use crate::client_topic::topicwriter::message_write_status::{
    MessageWriteStatus, MessageWriteStatusValidator, WriteAck,
};
use crate::client_topic::topicwriter::reception_queue::{ReceptionQueue, ReceptionTicket};
use crate::client_topic::topicwriter::writer_options::{AutoFlushSettings, WriterFlowControl};
use crate::{YdbError, YdbResult};

const WRITER_RUNTIME_MUTEX_POISONED: &str = "topic writer runtime mutex poisoned";

/// Shared logical writer state that outlives individual gRPC stream attempts.
///
/// Reconnecting resets send progress but preserves the buffer. Terminal failure stores the error
/// returned by every subsequent operation.
#[derive(Clone)]
pub(crate) struct WriterRuntime {
    inner: Arc<RuntimeInner>,
}

struct RuntimeInner {
    state: Mutex<RuntimeState>,
    capacity_limiter: CapacityLimiter,
    auto_flush: AutoFlushSettings,
    new_message_added: Notify,
    flush_requested: Notify,
}

enum RuntimeState {
    Active(WriterBuffer),
    Failed(YdbError),
}

impl RuntimeState {
    fn buffer_mut(&mut self) -> YdbResult<&mut WriterBuffer> {
        match self {
            Self::Active(buffer) => Ok(buffer),
            Self::Failed(err) => Err(err.clone()),
        }
    }
}

impl WriterRuntime {
    pub(crate) fn new_with_status_validator(
        status_validator: MessageWriteStatusValidator,
        auto_seq_no: bool,
        flow_control: WriterFlowControl,
    ) -> YdbResult<Self> {
        let inflight = flow_control.inflight();
        Ok(Self {
            inner: Arc::new(RuntimeInner {
                state: Mutex::new(RuntimeState::Active(WriterBuffer::new(
                    status_validator,
                    auto_seq_no,
                ))),
                capacity_limiter: CapacityLimiter::new(inflight.messages(), inflight.bytes())?,
                auto_flush: flow_control.auto_flush(),
                new_message_added: Notify::new(),
                flush_requested: Notify::new(),
            }),
        })
    }

    fn lock_state(&self) -> YdbResult<MutexGuard<'_, RuntimeState>> {
        self.inner
            .state
            .lock()
            .map_err(|_| YdbError::custom(WRITER_RUNTIME_MUTEX_POISONED))
    }

    pub(crate) fn initialize_last_seq_no(&self, last_seq_no: i64) -> YdbResult<()> {
        let mut state = self.lock_state()?;
        let buffer = state.buffer_mut()?;
        if buffer.last_seq_no_assigned.is_some() {
            return Err(YdbError::custom(
                "message queue last sequence number is already initialized",
            ));
        }
        buffer.last_seq_no_assigned = Some(last_seq_no);
        Ok(())
    }

    pub(crate) async fn add_message(
        &self,
        message: TopicWriterMessage,
        ack_sender: Option<oneshot::Sender<YdbResult<MessageWriteStatus>>>,
    ) -> YdbResult<()> {
        self.ensure_available()?;
        let admission = self.inner.capacity_limiter.admit(message);
        tokio::pin!(admission);
        let (message, was_blocked) = match admission.as_mut().now_or_never() {
            Some(Ok(message)) => (message, false),
            Some(Err(err)) => return Err(self.runtime_error_or(err)),
            None => {
                self.inner.flush_requested.notify_one();
                match admission.await {
                    Ok(message) => (message, true),
                    Err(err) => return Err(self.runtime_error_or(err)),
                }
            }
        };
        let mut state = self.lock_state()?;
        state.buffer_mut()?.add_message(message, ack_sender)?;
        self.inner.new_message_added.notify_one();
        if was_blocked {
            // Send this message while later capacity waiters are still blocked.
            self.inner.flush_requested.notify_one();
        }
        Ok(())
    }

    pub(crate) fn acknowledge_message(&self, write_ack: WriteAck) -> YdbResult<()> {
        let mut state = self.lock_state()?;
        state.buffer_mut()?.acknowledge_message(write_ack)
    }

    fn append_message_to_send_buffer(
        &self,
        send_buffer: &mut Vec<MessageData>,
        send_buffer_bytes: &mut usize,
    ) -> YdbResult<AppendMessageToSendBufferResult> {
        let mut state = self.lock_state()?;
        Ok(state
            .buffer_mut()?
            .message_queue
            .append_message_to_send_buffer(send_buffer, send_buffer_bytes, self.inner.auto_flush))
    }

    pub(crate) async fn get_messages_to_send(&self) -> YdbResult<Vec<MessageData>> {
        let mut messages = Vec::new();
        let mut message_bytes = 0;

        let timeout = Instant::now() + self.inner.auto_flush.interval();
        loop {
            // Append while we can
            loop {
                match self.append_message_to_send_buffer(&mut messages, &mut message_bytes)? {
                    AppendMessageToSendBufferResult::Full => return Ok(messages),
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

        Ok(messages)
    }

    pub(crate) fn fail(&self, error: YdbError) -> YdbResult<()> {
        {
            let mut state = self.lock_state()?;
            state
                .buffer_mut()?
                .reception_queue
                .send_error_to_tickets_and_clear(error.clone());
            *state = RuntimeState::Failed(error);
        }
        self.inner.capacity_limiter.close();
        self.inner.new_message_added.notify_waiters();
        self.inner.flush_requested.notify_waiters();
        Ok(())
    }

    pub(crate) fn reset_progress(&self) -> YdbResult<()> {
        let mut state = self.lock_state()?;
        state.buffer_mut()?.message_queue.reset_progress();
        Ok(())
    }

    pub(crate) async fn flush(&self) -> YdbResult<()> {
        let flush_result_rx = {
            let mut state = self.lock_state()?;
            state.buffer_mut()?.reception_queue.init_flush()?
        };
        self.inner.flush_requested.notify_one();
        flush_result_rx.await.map_err(YdbError::from)?
    }

    pub(crate) fn ensure_available(&self) -> YdbResult<()> {
        match &*self.lock_state()? {
            RuntimeState::Active(_) => Ok(()),
            RuntimeState::Failed(err) => Err(err.clone()),
        }
    }

    fn runtime_error_or(&self, fallback: YdbError) -> YdbError {
        match self.ensure_available() {
            Ok(()) => fallback,
            Err(err) => err,
        }
    }
}

#[cfg(test)]
const TEST_INFLIGHT_MESSAGES: usize = 1000;
#[cfg(test)]
const TEST_INFLIGHT_BYTES: usize = 20 * crate::byte_units::MiB;

#[cfg(test)]
impl WriterRuntime {
    fn new() -> Self {
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
        Self::new_with_status_validator(
            crate::client_topic::topicwriter::message_write_status::accept_any_write_status,
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
    message_queue: MessageQueue,
    reception_queue: ReceptionQueue,
    status_validator: MessageWriteStatusValidator,
    auto_seq_no: bool,
    last_seq_no_assigned: Option<i64>,
}

impl WriterBuffer {
    fn new(status_validator: MessageWriteStatusValidator, auto_seq_no: bool) -> Self {
        Self {
            message_queue: MessageQueue::new(),
            reception_queue: ReceptionQueue::new(),
            status_validator,
            auto_seq_no,
            last_seq_no_assigned: None,
        }
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

    fn acknowledge_message(&mut self, write_ack: WriteAck) -> YdbResult<()> {
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
        let status_result = (self.status_validator)(write_ack.status);
        let flush_error = status_result.as_ref().err().cloned();
        ticket.send_result_if_needed(status_result);
        self.reception_queue
            .notify_ticket_processed(ticket_seq_no, flush_error);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures_util::FutureExt;
    use tokio::time::timeout;

    use super::*;
    use crate::client_topic::topicwriter::message_write_status::expect_transactional_write_status;
    use crate::client_topic::topicwriter::test_helpers::{write_ack, writer_flow_control};

    fn create_message(seq_no: i64, data: Vec<u8>) -> TopicWriterMessage {
        TopicWriterMessage::builder()
            .data(data)
            .seq_no(seq_no)
            .build()
    }

    #[tokio::test]
    async fn capacity_is_held_until_acknowledgement() {
        let q = WriterRuntime::with_flow_control(false, 2, 5, 1, 5, Duration::ZERO);
        q.add_message(create_message(1, vec![0; 3]), None)
            .await
            .unwrap();
        let messages = q.get_messages_to_send().await.unwrap();
        assert_eq!(messages.len(), 1);

        let mut blocked_write = Box::pin(q.add_message(create_message(2, vec![0; 3]), None));
        assert!(blocked_write.as_mut().now_or_never().is_none());

        q.acknowledge_message(write_ack(1)).unwrap();
        blocked_write.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn capacity_waiter_flushes_partial_batches() {
        let q = WriterRuntime::with_flow_control(false, 10, 10, 10, 10, Duration::from_secs(3600));
        q.add_message(create_message(1, vec![0; 7]), None)
            .await
            .unwrap();

        let mut blocked_write = Box::pin(q.add_message(create_message(2, vec![0; 4]), None));
        assert!(blocked_write.as_mut().now_or_never().is_none());

        let first_batch = timeout(Duration::from_millis(100), q.get_messages_to_send())
            .await
            .expect("capacity pressure must flush the buffered partial batch")
            .expect("queue must remain available");
        assert_eq!(first_batch.len(), 1);
        assert_eq!(first_batch[0].seq_no, 1);

        q.acknowledge_message(write_ack(1)).unwrap();
        blocked_write.await.unwrap();

        let second_batch = timeout(Duration::from_millis(100), q.get_messages_to_send())
            .await
            .expect("the admitted capacity waiter must flush without waiting for the interval")
            .expect("queue must remain available");
        assert_eq!(second_batch.len(), 1);
        assert_eq!(second_batch[0].seq_no, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn capacity_waiters_keep_flushing_partial_batches() {
        let q = WriterRuntime::with_flow_control(false, 10, 10, 10, 10, Duration::from_secs(3600));
        q.add_message(create_message(1, vec![0; 10]), None)
            .await
            .unwrap();

        let first_batch = q.get_messages_to_send().await.unwrap();
        assert_eq!(first_batch.len(), 1);
        assert_eq!(first_batch[0].seq_no, 1);

        let mut second_write = Box::pin(q.add_message(create_message(2, vec![0; 7]), None));
        assert!(second_write.as_mut().now_or_never().is_none());
        let mut third_write = Box::pin(q.add_message(create_message(3, vec![0; 7]), None));
        assert!(third_write.as_mut().now_or_never().is_none());

        let empty_batch = timeout(Duration::from_millis(100), q.get_messages_to_send())
            .await
            .expect("capacity waiters must request a flush")
            .expect("queue must remain available");
        assert!(empty_batch.is_empty());

        q.acknowledge_message(write_ack(1)).unwrap();
        second_write.await.unwrap();

        let second_batch = timeout(Duration::from_millis(100), q.get_messages_to_send())
            .await
            .expect("the admitted capacity waiter must request another flush")
            .expect("queue must remain available");
        assert_eq!(second_batch.len(), 1);
        assert_eq!(second_batch[0].seq_no, 2);

        assert!(third_write.as_mut().now_or_never().is_none());
        q.acknowledge_message(write_ack(2)).unwrap();
        third_write.await.unwrap();
    }

    #[tokio::test]
    async fn get_messages_to_send_flushes_at_byte_threshold() {
        let q = WriterRuntime::with_flow_control(false, 10, 10, 10, 5, Duration::from_secs(3600));
        let q_collect = q.clone();
        let collect_handle =
            tokio::spawn(async move { q_collect.get_messages_to_send().await.unwrap() });

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
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].seq_no, 1);
        assert_eq!(messages[1].seq_no, 2);
    }

    #[tokio::test]
    async fn get_messages_to_send_moves_batch_to_sent_and_can_ack() {
        let q = Arc::new(WriterRuntime::new());

        let q_collect = Arc::clone(&q);
        let collect_handle =
            tokio::spawn(async move { q_collect.get_messages_to_send().await.unwrap() });
        q.add_message(create_message(1, vec![10]), None)
            .await
            .unwrap();
        q.add_message(create_message(2, vec![20]), None)
            .await
            .unwrap();

        let batch = collect_handle.await.unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].seq_no, 1);
        assert_eq!(batch[1].seq_no, 2);

        q.acknowledge_message(write_ack(1)).unwrap();
        q.acknowledge_message(write_ack(2)).unwrap();
    }

    #[tokio::test]
    async fn get_messages_to_send_empty_queue_times_out_empty() {
        let q = WriterRuntime::new();
        let msgs = q.get_messages_to_send().await.unwrap();
        assert!(msgs.is_empty());
    }

    #[tokio::test]
    async fn get_messages_to_send_drains_messages_added_before_call() {
        let q = WriterRuntime::new();
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        q.add_message(create_message(2, vec![]), None)
            .await
            .unwrap();

        let msgs = q.get_messages_to_send().await.unwrap();

        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].seq_no, 1);
        assert_eq!(msgs[1].seq_no, 2);
    }

    #[tokio::test]
    async fn get_messages_to_send_with_zero_duration_drains_messages() {
        let q = WriterRuntime::with_flow_control(
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

        let msgs = q.get_messages_to_send().await.unwrap();

        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].seq_no, 1);
    }

    #[tokio::test]
    async fn get_messages_to_send_collects_messages_added_during_call() {
        let q = Arc::new(WriterRuntime::new());
        let q_collect = Arc::clone(&q);
        let collect_handle =
            tokio::spawn(async move { q_collect.get_messages_to_send().await.unwrap() });
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();

        let msgs = collect_handle.await.unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].seq_no, 1);
    }

    #[tokio::test]
    async fn get_messages_to_send_respects_threshold() {
        let q = Arc::new(WriterRuntime::with_flow_control(
            false,
            TEST_INFLIGHT_MESSAGES,
            TEST_INFLIGHT_BYTES,
            2,
            TEST_INFLIGHT_BYTES,
            Duration::from_millis(50),
        ));
        let q_collect = Arc::clone(&q);
        let collect_handle =
            tokio::spawn(async move { q_collect.get_messages_to_send().await.unwrap() });
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
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].seq_no, 1);
        assert_eq!(msgs[1].seq_no, 2);
    }

    #[tokio::test]
    async fn get_messages_to_send_second_call_drains_remaining() {
        let q = Arc::new(WriterRuntime::with_flow_control(
            false,
            TEST_INFLIGHT_MESSAGES,
            TEST_INFLIGHT_BYTES,
            2,
            TEST_INFLIGHT_BYTES,
            Duration::from_millis(50),
        ));
        let q1 = Arc::clone(&q);
        let h1 = tokio::spawn(async move { q1.get_messages_to_send().await.unwrap() });
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
        assert_eq!(first.len(), 2);

        let q2 = Arc::clone(&q);
        let h2 = tokio::spawn(async move { q2.get_messages_to_send().await.unwrap() });
        let second = h2.await.unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].seq_no, 13);
    }

    #[tokio::test]
    async fn acknowledge_message_returns_error_when_reception_ticket_not_present() {
        let q = WriterRuntime::new();

        let err = q.acknowledge_message(write_ack(8)).unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("expected reception ticket to be actually present"));
    }

    #[tokio::test]
    async fn acknowledge_message_errors_when_seq_no_mismatches() {
        let q = WriterRuntime::new();
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        let messages = q.get_messages_to_send().await.unwrap();
        assert_eq!(messages.len(), 1);

        let err = q.acknowledge_message(write_ack(99)).unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("reception ticket and write ack seq_no mismatch"));
        assert!(err_msg.contains("ack_seq_no: 99"));
        assert!(err_msg.contains("ticket_seq_no: 1"));
    }

    #[tokio::test]
    async fn reset_progress_restores_sent_messages_to_pending() {
        let q = WriterRuntime::new();
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        q.add_message(create_message(2, vec![]), None)
            .await
            .unwrap();
        let first_batch = q.get_messages_to_send().await.unwrap();
        assert_eq!(first_batch.len(), 2);

        q.reset_progress().unwrap();

        let msgs = q.get_messages_to_send().await.unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].seq_no, 1);
        assert_eq!(msgs[1].seq_no, 2);
    }

    #[tokio::test]
    async fn flush_returns_status_validation_error_observed_before_flush() {
        let q = WriterRuntime::new_with_status_validator(
            expect_transactional_write_status,
            false,
            writer_flow_control(
                TEST_INFLIGHT_MESSAGES,
                TEST_INFLIGHT_BYTES,
                10,
                TEST_INFLIGHT_BYTES,
                Duration::from_millis(20),
            ),
        )
        .unwrap();
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        let messages = q.get_messages_to_send().await.unwrap();
        assert_eq!(messages.len(), 1);

        q.acknowledge_message(write_ack(1)).unwrap();

        assert!(q.flush().await.is_err());
    }

    #[tokio::test]
    async fn flush_returns_status_validation_error_observed_during_flush() {
        let q = Arc::new(
            WriterRuntime::new_with_status_validator(
                expect_transactional_write_status,
                false,
                writer_flow_control(
                    TEST_INFLIGHT_MESSAGES,
                    TEST_INFLIGHT_BYTES,
                    10,
                    TEST_INFLIGHT_BYTES,
                    Duration::from_millis(20),
                ),
            )
            .unwrap(),
        );
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        let messages = q.get_messages_to_send().await.unwrap();
        assert_eq!(messages.len(), 1);

        let q_flush = Arc::clone(&q);
        let flush_handle = tokio::spawn(async move { q_flush.flush().await });

        q.acknowledge_message(write_ack(1)).unwrap();

        assert!(
            flush_handle
                .await
                .expect("flush task must complete")
                .is_err()
        );
    }

    #[tokio::test]
    async fn flush_returns_error_when_reception_tickets_fail_during_wait() {
        let q = Arc::new(WriterRuntime::new());
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        let messages = q.get_messages_to_send().await.unwrap();
        assert_eq!(messages.len(), 1);

        let q_flush = Arc::clone(&q);
        let flush_handle = tokio::spawn(async move { q_flush.flush().await });
        tokio::task::yield_now().await;

        q.fail(YdbError::custom("fatal writer error")).unwrap();

        let result = timeout(Duration::from_millis(100), flush_handle)
            .await
            .expect("flush must finish after reception tickets fail")
            .expect("flush task must not panic");

        let err = result.unwrap_err();
        assert!(err.to_string().contains("fatal writer error"));
    }

    #[tokio::test]
    async fn terminal_failure_is_returned_by_all_runtime_operations() {
        let q = WriterRuntime::new();
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
    async fn terminal_failure_wakes_capacity_waiters_with_runtime_error() {
        let q = WriterRuntime::with_flow_control(false, 1, 1, 1, 1, Duration::ZERO);
        q.add_message(create_message(1, vec![0]), None)
            .await
            .unwrap();

        let mut waiting = Box::pin(q.add_message(create_message(2, vec![0]), None));
        assert!(waiting.as_mut().now_or_never().is_none());
        q.fail(YdbError::custom("terminal writer error")).unwrap();

        let err = waiting.await.unwrap_err();
        assert!(err.to_string().contains("terminal writer error"));
    }
}
