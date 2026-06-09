use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{oneshot, Mutex as TokioMutex, Notify, RwLock};
use tokio::time::{sleep_until, Instant};
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::client_topic::topicwriter::message_queue::{
    AppendMessageToSendBufferResult, MessageQueueInner,
};
use crate::client_topic::topicwriter::message_write_status::{MessageWriteStatus, WriteAck};
use crate::client_topic::topicwriter::writer_reception_queue::TopicWriterReceptionQueue;
use crate::client_topic::topicwriter::writer_reception_queue::TopicWriterReceptionTicket;
use crate::client_topic::topicwriter::writer_reception_queue::TopicWriterReceptionType;
use crate::YdbError;
use crate::YdbResult;

#[derive(Clone)]
pub(crate) struct Queue {
    inner: Arc<TokioMutex<QueueInner>>,

    new_message_added: Arc<Notify>,
    last_acknowledged_seq_no: Arc<RwLock<Option<i64>>>,
    message_acknowledged: Arc<Notify>,
}

impl Queue {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(TokioMutex::new(QueueInner::new())),
            new_message_added: Arc::new(Notify::new()),
            last_acknowledged_seq_no: Arc::new(RwLock::new(None)),
            message_acknowledged: Arc::new(Notify::new()),
        }
    }

    pub(crate) async fn add_message(
        &self,
        message: MessageData,
        ack: Option<oneshot::Sender<YdbResult<MessageWriteStatus>>>,
    ) -> YdbResult<()> {
        let reception_type = ack.map_or(
            TopicWriterReceptionType::NoConfirmationExpected,
            TopicWriterReceptionType::AwaitingConfirmation,
        );

        let mut inner = self.inner.lock().await;
        inner.add_message(message, reception_type).await?;
        self.new_message_added.notify_one();
        Ok(())
    }

    pub(crate) async fn acknowledge_message(&self, write_ack: WriteAck) -> YdbResult<()> {
        let mut inner = self.inner.lock().await;
        let seq_no = write_ack.seq_no;
        inner.acknowledge_message(write_ack).await?;

        *self.last_acknowledged_seq_no.write().await = Some(seq_no);
        self.message_acknowledged.notify_one();

        Ok(())
    }

    // Waits for the last message to be acknowledged.
    // Note that the "last message" is the last message in the queue at the start of the method call.
    // If more messages are added during the wait, they will not be waited for here.
    //
    // Is used for the flush operation.
    pub(crate) async fn wait_for_messages_to_be_acknowledged(&self) {
        let last_seq_no = {
            let inner = self.inner.lock().await;
            inner.last_added_seq_no()
        };
        let Some(last_seq_no) = last_seq_no else {
            return;
        };

        loop {
            tokio::select! {
                _ = self.message_acknowledged.notified() => {
                    match *self.last_acknowledged_seq_no.read().await {
                        Some(last_acknowledged_seq_no) if last_acknowledged_seq_no >= last_seq_no => break,
                        _ => continue,
                    }
                }
            }
        }
    }

    async fn append_message_to_send_buffer(
        &self,
        send_buffer: &mut Vec<MessageData>,
        threshold: usize,
    ) -> AppendMessageToSendBufferResult {
        let mut inner = self.inner.lock().await;
        inner.append_message_to_send_buffer(send_buffer, threshold)
    }

    pub(crate) async fn get_messages_to_send(
        &self,
        threshold: usize,
        duration: Duration,
    ) -> Vec<MessageData> {
        let mut messages = Vec::new();
        if threshold == 0 {
            return messages;
        }

        let timeout = Instant::now() + duration;
        loop {
            // Append while we can
            loop {
                match self
                    .append_message_to_send_buffer(&mut messages, threshold)
                    .await
                {
                    AppendMessageToSendBufferResult::Full => return messages,
                    // Looks like there are no messages
                    AppendMessageToSendBufferResult::CouldNotGetMessage => break,
                    AppendMessageToSendBufferResult::UnderThreshold => {}
                }
            }

            // Wait for new messages or timeout
            tokio::select! {
                biased;
                _ = self.new_message_added.notified() => {}
                _ = sleep_until(timeout) => break,
            }
        }

        messages
    }

    pub(crate) async fn notify_reception_tickets(&mut self, error: YdbError) {
        let mut inner = self.inner.lock().await;
        inner.notify_reception_tickets(error)
    }

    pub(crate) async fn close_for_new_messages(&self) {
        let mut inner = self.inner.lock().await;
        inner.close_for_new_messages()
    }

    pub(crate) async fn reset_progress(&self) -> () {
        let mut inner = self.inner.lock().await;
        inner.reset_progress()
    }

    pub(crate) async fn flush(&self) -> YdbResult<()> {
        let flush_op_completed = {
            let mut inner = self.inner.lock().await;
            inner.init_flush()?
        };

        self.wait_for_messages_to_be_acknowledged().await;

        Ok(flush_op_completed.await?)
    }
}

struct QueueInner {
    message_queue: MessageQueueInner,
    reception_queue: TopicWriterReceptionQueue,
    is_open_for_new_messages: bool,
    last_added_seq_no: Option<i64>,
}

impl QueueInner {
    fn new() -> Self {
        Self {
            message_queue: MessageQueueInner::new(),
            reception_queue: TopicWriterReceptionQueue::new(),
            is_open_for_new_messages: true,
            last_added_seq_no: None,
        }
    }

    async fn add_message(
        &mut self,
        message: MessageData,
        reception_type: TopicWriterReceptionType,
    ) -> YdbResult<()> {
        if !self.is_open_for_new_messages {
            return Err(YdbError::custom("message queue is closed for new messages"));
        }

        let seq_no = message.seq_no;

        self.message_queue.add_message(message)?;

        self.reception_queue
            .add_ticket(TopicWriterReceptionTicket::new(seq_no, reception_type));

        self.last_added_seq_no = Some(seq_no);

        Ok(())
    }

    async fn acknowledge_message(&mut self, write_ack: WriteAck) -> YdbResult<()> {
        let expected_seq_no = self.reception_queue.peek_ticket_seq_no();

        let Some(expected_seq_no) = expected_seq_no else {
            return Err(YdbError::custom(
                "expected reception ticket to be actually present",
            ));
        };
        if write_ack.seq_no != expected_seq_no {
            return Err(YdbError::custom(format!(
                "reception ticket and write ack seq_no mismatch: expected_seq_no: {}, actual_seq_no: {}",
                write_ack.seq_no, expected_seq_no,
            )));
        }

        self.message_queue.acknowledge_message(write_ack.seq_no)?;

        let ticket = self.reception_queue.try_get_ticket()?;
        let Some(ticket) = ticket else {
            return Err(YdbError::custom(
                "reception ticket is missing after message queue ack",
            ));
        };
        ticket.send_confirmation_if_needed(write_ack.status);

        Ok(())
    }

    fn append_message_to_send_buffer(
        &mut self,
        send_buffer: &mut Vec<MessageData>,
        threshold: usize,
    ) -> AppendMessageToSendBufferResult {
        self.message_queue
            .append_message_to_send_buffer(send_buffer, threshold)
    }

    fn last_added_seq_no(&self) -> Option<i64> {
        self.last_added_seq_no
    }

    fn notify_reception_tickets(&mut self, error: YdbError) {
        self.reception_queue.send_error_to_tickets_and_clear(error)
    }

    fn init_flush(&mut self) -> YdbResult<Receiver<()>> {
        let receiver = self.reception_queue.init_flush_op()?;
        Ok(receiver)
    }

    fn reset_progress(&mut self) -> () {
        self.message_queue.reset_progress();
    }

    fn close_for_new_messages(&mut self) {
        self.is_open_for_new_messages = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_topic::topicwriter::message_write_status::{MessageWriteStatus, WriteAck};
    use std::sync::Arc;
    use std::time::Duration;

    fn create_message(seq_no: i64, data: Vec<u8>) -> MessageData {
        MessageData {
            seq_no,
            created_at: None,
            data,
            uncompressed_size: 0,
            metadata_items: vec![],
            partitioning: None,
        }
    }

    fn write_ack(seq_no: i64) -> WriteAck {
        WriteAck {
            seq_no,
            status: MessageWriteStatus::Unknown,
        }
    }

    #[tokio::test]
    async fn add_message_rejects_when_queue_closed_for_new_messages() {
        let q = Queue::new();
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();

        q.close_for_new_messages().await;

        let err = q
            .add_message(create_message(1, vec![]), None)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("closed for new messages"));
    }

    #[tokio::test]
    async fn get_messages_to_send_moves_batch_to_sent_and_can_ack() {
        let q = Arc::new(Queue::new());

        let q_collect = Arc::clone(&q);
        let collect_handle = tokio::spawn(async move {
            q_collect
                .get_messages_to_send(10, Duration::from_millis(50))
                .await
        });
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

        q.acknowledge_message(write_ack(1)).await.unwrap();
        q.acknowledge_message(write_ack(2)).await.unwrap();
    }

    #[tokio::test]
    async fn get_messages_to_send_empty_queue_times_out_empty() {
        let q = Queue::new();
        let msgs = q.get_messages_to_send(10, Duration::from_millis(20)).await;
        assert!(msgs.is_empty());
    }

    #[tokio::test]
    async fn get_messages_to_send_drains_messages_added_before_call() {
        let q = Queue::new();
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        q.add_message(create_message(2, vec![]), None)
            .await
            .unwrap();

        let msgs = q.get_messages_to_send(10, Duration::from_millis(20)).await;

        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].seq_no, 1);
        assert_eq!(msgs[1].seq_no, 2);
    }

    #[tokio::test]
    async fn get_messages_to_send_with_zero_duration_drains_messages() {
        let q = Queue::new();
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();

        let msgs = q.get_messages_to_send(10, Duration::ZERO).await;

        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].seq_no, 1);
    }

    #[tokio::test]
    async fn get_messages_to_send_with_zero_threshold_doesnt_move_messages_to_sent() {
        let q = Queue::new();
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        q.add_message(create_message(2, vec![]), None)
            .await
            .unwrap();

        let msgs = q.get_messages_to_send(0, Duration::from_millis(50)).await;
        assert!(msgs.is_empty());

        let err = q.acknowledge_message(write_ack(1)).await.unwrap_err();
        assert!(err.to_string().contains("queue is empty"));

        let msgs = q.get_messages_to_send(10, Duration::from_millis(20)).await;
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].seq_no, 1);
        assert_eq!(msgs[1].seq_no, 2);
    }

    #[tokio::test]
    async fn get_messages_to_send_collects_messages_added_during_call() {
        let q = Arc::new(Queue::new());
        let q_collect = Arc::clone(&q);
        let collect_handle = tokio::spawn(async move {
            q_collect
                .get_messages_to_send(10, Duration::from_millis(50))
                .await
        });
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();

        let msgs = collect_handle.await.unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].seq_no, 1);
    }

    #[tokio::test]
    async fn get_messages_to_send_respects_threshold() {
        let q = Arc::new(Queue::new());
        let q_collect = Arc::clone(&q);
        let collect_handle = tokio::spawn(async move {
            q_collect
                .get_messages_to_send(2, Duration::from_millis(50))
                .await
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
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].seq_no, 1);
        assert_eq!(msgs[1].seq_no, 2);
    }

    #[tokio::test]
    async fn get_messages_to_send_second_call_drains_remaining() {
        let q = Arc::new(Queue::new());
        let q1 = Arc::clone(&q);
        let h1 =
            tokio::spawn(
                async move { q1.get_messages_to_send(2, Duration::from_millis(50)).await },
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
        assert_eq!(first.len(), 2);

        let q2 = Arc::clone(&q);
        let h2 =
            tokio::spawn(
                async move { q2.get_messages_to_send(10, Duration::from_millis(50)).await },
            );
        let second = h2.await.unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].seq_no, 13);
    }

    #[tokio::test]
    async fn acknowledge_message_returns_error_when_reception_ticket_not_present() {
        let q = Queue::new();

        let err = q.acknowledge_message(write_ack(8)).await.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("expected reception ticket to be actually present"));
    }

    #[tokio::test]
    async fn acknowledge_message_errors_when_seq_no_mismatches() {
        let q = Queue::new();
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        let messages = q.get_messages_to_send(10, Duration::from_millis(20)).await;
        assert_eq!(messages.len(), 1);

        let err = q.acknowledge_message(write_ack(99)).await.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("reception ticket and write ack seq_no mismatch"));
        assert!(err_msg.contains("expected_seq_no: 99"));
        assert!(err_msg.contains("actual_seq_no: 1"));
    }

    #[tokio::test]
    async fn reset_progress_restores_sent_messages_to_pending() {
        let q = Queue::new();
        q.add_message(create_message(1, vec![]), None)
            .await
            .unwrap();
        q.add_message(create_message(2, vec![]), None)
            .await
            .unwrap();
        let first_batch = q.get_messages_to_send(1, Duration::from_millis(20)).await;
        assert_eq!(first_batch.len(), 1);

        q.reset_progress().await;

        let msgs = q.get_messages_to_send(10, Duration::from_millis(20)).await;
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].seq_no, 1);
        assert_eq!(msgs[1].seq_no, 2);
    }

    #[tokio::test]
    async fn wait_for_messages_to_be_acknowledged_completes_when_all_messages_are_acknowledged() {
        let q = Arc::new(Queue::new());
        for i in 1..=5 {
            q.add_message(create_message(i, vec![]), None)
                .await
                .unwrap();
        }

        let q_wait = Arc::clone(&q);
        let wait_handle = tokio::spawn(async move {
            q_wait.wait_for_messages_to_be_acknowledged().await;
        });

        let messages = q.get_messages_to_send(20, Duration::from_millis(50)).await;
        assert_eq!(messages.len(), 5);

        for i in 1..=5 {
            q.acknowledge_message(write_ack(i)).await.unwrap();
        }

        wait_handle.await.unwrap();
    }

    #[tokio::test]
    async fn wait_for_messages_to_be_acknowledged_completes_with_no_messages() {
        let q = Queue::new();

        q.wait_for_messages_to_be_acknowledged().await;
    }
}
