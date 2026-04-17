use std::{collections::VecDeque, mem::swap, sync::Arc, time::Duration};

use tokio::{
    sync::{mpsc, Mutex as TokioMutex, Notify},
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use tracing::log::trace;

use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::{YdbError, YdbResult};

#[derive(Clone)]
pub(crate) struct MessageQueue {
    inner: Arc<TokioMutex<MessageQueueInner>>,

    new_message_added: Arc<Notify>,
    message_acknowledged_rx: Arc<TokioMutex<mpsc::UnboundedReceiver<i64>>>,
}

impl MessageQueue {
    pub(crate) fn new() -> Self {
        let new_message_added = Arc::new(Notify::new());
        let (message_acknowledged_tx, message_acknowledged_rx) = mpsc::unbounded_channel();

        Self {
            inner: Arc::new(TokioMutex::new(MessageQueueInner::new(
                new_message_added.clone(),
                message_acknowledged_tx,
            ))),
            new_message_added,
            message_acknowledged_rx: Arc::new(TokioMutex::new(message_acknowledged_rx)),
        }
    }

    pub(crate) async fn add_message(&self, message: MessageData) -> YdbResult<()> {
        let mut inner = self.inner.lock().await;
        inner.add_message(message)
    }

    async fn get_messages_to_send_without_threshold(&self) -> YdbResult<Vec<MessageData>> {
        let mut inner = self.inner.lock().await;
        inner.get_messages_to_send_without_threshold()
    }

    async fn get_messages_to_send_with_length_threshold(
        &self,
        length_threshold: usize,
    ) -> GetMessagesToSendResult {
        let mut inner = self.inner.lock().await;
        inner.get_messages_to_send_with_length_threshold(length_threshold)
    }

    async fn get_messages_to_send_loop(
        &self,
        length_threshold: usize,
    ) -> YdbResult<Vec<MessageData>> {
        loop {
            _ = self.new_message_added.notified().await;
            match self
                .get_messages_to_send_with_length_threshold(length_threshold)
                .await
            {
                GetMessagesToSendResult::Ok(messages) => return Ok(messages),
                GetMessagesToSendResult::NotEnoughMessages => continue,
                GetMessagesToSendResult::Err(err) => return Err(err),
            }
        }
    }

    pub(crate) async fn get_messages_to_send(
        &self,
        length_threshold: usize,
        duration: Duration,
    ) -> YdbResult<Vec<MessageData>> {
        match timeout(duration, self.get_messages_to_send_loop(length_threshold)).await {
            Ok(result) => result,
            Err(_) => self.get_messages_to_send_without_threshold().await,
        }
    }

    pub(crate) async fn acknowledge_message(&self, seq_no: i64) -> YdbResult<()> {
        let mut inner = self.inner.lock().await;
        inner.acknowledge_message(seq_no)
    }

    pub(crate) async fn reset_progress(&self) {
        let mut inner = self.inner.lock().await;
        inner.reset_progress()
    }

    pub(crate) async fn close_for_new_messages(&self) {
        let mut inner = self.inner.lock().await;
        inner.close_for_new_messages()
    }

    // Waits for the last message to be acknowledged.
    // Note that the "last message" is the last message in the queue at the start of the method call.
    // If more messages are added during the wait, they will not be waited for here.
    //
    // Is used for the flush operation.
    pub(crate) async fn wait_for_messages_to_be_acknowledged(
        &self,
        cancellation_token: &CancellationToken,
    ) {
        let last_seq_no = {
            let inner = self.inner.lock().await;
            inner.last_added_seq_no
        };
        let Some(last_seq_no) = last_seq_no else {
            return;
        };

        let mut message_acknowledged_rx = self.message_acknowledged_rx.lock().await;

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                }
                Some(seq_no) = message_acknowledged_rx.recv() => {
                    if seq_no == last_seq_no {
                        break;
                    }
                }
                else => {
                    trace!("message acknowledged channel is closed");
                    break;
                }
            }
        }
    }
}

struct MessageQueueInner {
    // Messages awaiting to be sent
    messages: VecDeque<MessageData>,
    // Messages awaiting to be acknowledged
    sent_messages: VecDeque<MessageData>,

    // Sequence number of the last message that has been added to the queue
    last_added_seq_no: Option<i64>,

    is_open_for_new_messages: bool,

    new_message_added: Arc<Notify>,
    message_acknowledged_tx: mpsc::UnboundedSender<i64>,
}

#[derive(Debug)]
enum GetMessagesToSendResult {
    Ok(Vec<MessageData>),
    // TODO: In the future, when the batch is checked by the serialized size (not the length), this
    // variant has to be changed accordingly.
    NotEnoughMessages,
    Err(YdbError),
}

impl MessageQueueInner {
    pub(crate) fn new(
        new_message_added: Arc<Notify>,
        message_acknowledged_tx: mpsc::UnboundedSender<i64>,
    ) -> Self {
        Self {
            messages: VecDeque::new(),
            sent_messages: VecDeque::new(),
            last_added_seq_no: None,
            is_open_for_new_messages: true,
            new_message_added,
            message_acknowledged_tx,
        }
    }

    fn add_message(&mut self, message: MessageData) -> YdbResult<()> {
        if !self.is_open_for_new_messages {
            return Err(YdbError::custom("message queue is closed for new messages"));
        }

        let seq_no = message.seq_no;
        self.check_message_seq_no(seq_no)?;

        self.last_added_seq_no = Some(seq_no);

        self.messages.push_back(message);

        self.new_message_added.notify_waiters();

        Ok(())
    }

    fn check_message_seq_no(&self, seq_no: i64) -> YdbResult<()> {
        match self.last_added_seq_no {
            Some(last_added_seq_no) if seq_no <= last_added_seq_no => Err(YdbError::custom(
                format!("message with seq_no={seq_no} is not newer than the last written message",),
            )),
            _ => Ok(()),
        }
    }

    fn get_length_of_messages_to_send(&self) -> usize {
        self.messages.len()
    }

    fn do_get_messages_to_send(&mut self, length: usize) -> YdbResult<Vec<MessageData>> {
        if length > self.messages.len() {
            return Err(YdbError::InternalError(format!(
                "message queue: requested a batch of {} messages but only {} message(s) are available",
                length,
                self.messages.len()
            )));
        }

        let mut result = Vec::with_capacity(length);

        for _ in 0..length {
            let message = self.messages.pop_front().unwrap(); // We check length above, so this is safe.
            result.push(message.clone());
            self.sent_messages.push_back(message);
        }

        Ok(result)
    }

    fn get_messages_to_send_without_threshold(&mut self) -> YdbResult<Vec<MessageData>> {
        let length = self.get_length_of_messages_to_send();
        self.do_get_messages_to_send(length)
    }

    fn get_messages_to_send_with_length_threshold(
        &mut self,
        length_threshold: usize,
    ) -> GetMessagesToSendResult {
        let length = self.get_length_of_messages_to_send();

        if length < length_threshold {
            return GetMessagesToSendResult::NotEnoughMessages;
        }

        self.do_get_messages_to_send(length_threshold)
            .map_or_else(GetMessagesToSendResult::Err, GetMessagesToSendResult::Ok)
    }

    fn acknowledge_message(&mut self, seq_no: i64) -> YdbResult<()> {
        let Some(message) = self.sent_messages.pop_front() else {
            return Err(YdbError::custom(format!(
                "ack unexpected message with seq_no={seq_no}: queue is empty",
            )));
        };

        if message.seq_no != seq_no {
            return Err(YdbError::custom(format!(
                "ack unexpected message with seq_no={seq_no}: message seq_no mismatch",
            )));
        }

        self.message_acknowledged_tx.send(seq_no).unwrap();

        if self.sent_messages.is_empty() && self.messages.is_empty() {
            self.last_added_seq_no = None;
        }

        Ok(())
    }

    fn reset_progress(&mut self) {
        self.sent_messages.append(&mut self.messages);
        swap(&mut self.messages, &mut self.sent_messages);
    }

    fn close_for_new_messages(&mut self) {
        self.is_open_for_new_messages = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

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

    async fn create_queue() -> (MessageQueueInner, tokio::task::JoinHandle<()>) {
        let new_message_added = Arc::new(Notify::new());
        let (message_acknowledged_tx, mut message_acknowledged_rx) = mpsc::unbounded_channel();
        let queue = MessageQueueInner::new(new_message_added, message_acknowledged_tx);
        let indefinite_reader_handle =
            tokio::spawn(async move { while message_acknowledged_rx.recv().await.is_some() {} });
        (queue, indefinite_reader_handle)
    }

    #[tokio::test]
    async fn new_creates_empty_queue() {
        let (q, _reader) = create_queue().await;
        assert!(q.last_added_seq_no.is_none());
        assert!(q.messages.is_empty());
        assert!(q.sent_messages.is_empty());
        assert!(q.is_open_for_new_messages);
    }

    #[tokio::test]
    async fn get_length_of_messages_to_send_empty() {
        let (q, _reader) = create_queue().await;
        assert_eq!(q.get_length_of_messages_to_send(), 0);
    }

    #[tokio::test]
    async fn get_length_of_messages_to_send_with_some_added_and_none_sent() {
        let (mut q, _reader) = create_queue().await;
        for i in 1..=5 {
            q.messages.push_back(create_message(i, vec![]));
        }
        assert_eq!(q.get_length_of_messages_to_send(), 5);
    }

    #[tokio::test]
    async fn get_length_of_messages_to_send_with_some_added_and_one_sent() {
        let (mut q, _reader) = create_queue().await;
        for i in 1..=5 {
            q.messages.push_back(create_message(i, vec![]));
        }
        let one = q.messages.pop_front().unwrap();
        q.sent_messages.push_back(one);
        assert_eq!(q.get_length_of_messages_to_send(), 4);
    }

    #[tokio::test]
    async fn get_length_of_messages_to_send_returns_zero_when_all_pending_are_in_sent() {
        let (mut q, _reader) = create_queue().await;
        for i in 1..=5 {
            q.messages.push_back(create_message(i, vec![]));
        }
        while let Some(m) = q.messages.pop_front() {
            q.sent_messages.push_back(m);
        }
        assert_eq!(q.get_length_of_messages_to_send(), 0);
    }

    #[tokio::test]
    async fn get_length_of_messages_to_send_with_none_added_and_some_sent() {
        let (mut q, _reader) = create_queue().await;
        for i in 1..=3 {
            q.sent_messages.push_back(create_message(i, vec![]));
        }

        assert_eq!(q.get_length_of_messages_to_send(), 0);
    }

    #[tokio::test]
    async fn add_message_appends_and_updates_fields() {
        let (mut q, _reader) = create_queue().await;
        q.add_message(create_message(10, vec![1, 2, 3])).unwrap();
        q.add_message(create_message(11, vec![4, 5])).unwrap();

        assert_eq!(q.get_length_of_messages_to_send(), 2);
        assert_eq!(q.messages.len(), 2);
        assert_eq!(q.messages[0].seq_no, 10);
        assert_eq!(q.messages[0].data, vec![1, 2, 3]);
        assert_eq!(q.messages[1].seq_no, 11);
        assert_eq!(q.messages[1].data, vec![4, 5]);

        assert_eq!(q.last_added_seq_no, Some(11));
    }

    #[tokio::test]
    async fn add_message_rejects_duplicate_seq_no() {
        let (mut q, _reader) = create_queue().await;
        q.add_message(create_message(4, vec![])).unwrap();

        let err = q.add_message(create_message(4, vec![])).unwrap_err();

        assert!(err.to_string().contains("seq_no=4"));
        assert!(err.to_string().contains("not newer than the last written"));
    }

    #[tokio::test]
    async fn add_message_rejects_out_of_order_seq_no() {
        let (mut q, _reader) = create_queue().await;
        q.add_message(create_message(10, vec![])).unwrap();

        let err = q.add_message(create_message(7, vec![])).unwrap_err();

        assert!(err.to_string().contains("seq_no=7"));
        assert!(err.to_string().contains("not newer than the last written"));
    }

    #[tokio::test]
    async fn add_message_rejects_when_queue_closed_for_new_messages() {
        let (mut q, _reader) = create_queue().await;
        q.close_for_new_messages();

        let err = q.add_message(create_message(1, vec![])).unwrap_err();

        assert!(err.to_string().contains("closed for new messages"));
    }

    #[tokio::test]
    async fn get_messages_to_send_without_threshold_moves_all_to_sent_messages() {
        let (mut q, _reader) = create_queue().await;
        q.add_message(create_message(3, vec![10])).unwrap();
        q.add_message(create_message(4, vec![20])).unwrap();

        let batch = q.get_messages_to_send_without_threshold().unwrap();

        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].seq_no, 3);
        assert_eq!(batch[1].seq_no, 4);

        assert!(q.messages.is_empty());
        assert_eq!(q.sent_messages.len(), 2);
        assert_eq!(q.sent_messages[0].seq_no, 3);
        assert_eq!(q.sent_messages[1].seq_no, 4);
        assert_eq!(q.last_added_seq_no, Some(4));
    }

    #[tokio::test]
    async fn get_messages_to_send_empty_queue_returns_empty() {
        let (mut q, _reader) = create_queue().await;
        let msgs = q.get_messages_to_send_without_threshold().unwrap();
        assert!(msgs.is_empty());
    }

    #[tokio::test]
    async fn get_messages_to_send_with_length_threshold_not_enough_messages() {
        let (mut q, _reader) = create_queue().await;
        q.add_message(create_message(1, vec![])).unwrap();

        let result = q.get_messages_to_send_with_length_threshold(2);

        match &result {
            GetMessagesToSendResult::NotEnoughMessages => {}
            _ => panic!("expected NotEnoughMessages, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn get_messages_to_send_with_length_threshold_ok_when_at_threshold() {
        let (mut q, _reader) = create_queue().await;
        q.add_message(create_message(11, vec![])).unwrap();
        q.add_message(create_message(12, vec![])).unwrap();

        let result = q.get_messages_to_send_with_length_threshold(2);

        match &result {
            GetMessagesToSendResult::Ok(msgs) => {
                assert_eq!(msgs.len(), 2);
                assert_eq!(msgs[0].seq_no, 11);
                assert_eq!(msgs[1].seq_no, 12);
            }
            other => panic!("expected Ok(...), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn get_messages_to_send_with_length_threshold_ok_when_above_threshold() {
        let (mut q, _reader) = create_queue().await;
        q.add_message(create_message(1, vec![])).unwrap();
        q.add_message(create_message(2, vec![])).unwrap();
        q.add_message(create_message(3, vec![])).unwrap();

        let result = q.get_messages_to_send_with_length_threshold(2);

        match &result {
            GetMessagesToSendResult::Ok(msgs) => assert_eq!(msgs.len(), 2),
            other => panic!("expected Ok(...), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn get_messages_to_send_returns_batch_when_threshold_met() {
        let q = MessageQueue::new();
        q.add_message(create_message(1, vec![1])).await.unwrap();
        q.add_message(create_message(2, vec![2])).await.unwrap();
        q.add_message(create_message(3, vec![3])).await.unwrap();
        let msgs = q
            .get_messages_to_send(2, std::time::Duration::from_millis(10))
            .await
            .unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0].seq_no, 1);
        assert_eq!(msgs[1].seq_no, 2);
        assert_eq!(msgs[2].seq_no, 3);
    }

    #[tokio::test]
    async fn acknowledge_message_removes_front_when_seq_no_matches() {
        let (mut q, _reader) = create_queue().await;
        q.add_message(create_message(5, vec![])).unwrap();
        q.add_message(create_message(6, vec![])).unwrap();
        q.add_message(create_message(7, vec![])).unwrap();
        let _ = q.get_messages_to_send_without_threshold().unwrap();

        q.acknowledge_message(5).unwrap();

        assert_eq!(q.get_length_of_messages_to_send(), 0);
        assert!(q.messages.is_empty());
        assert_eq!(q.sent_messages.len(), 2);
        assert_eq!(q.sent_messages[0].seq_no, 6);
        assert_eq!(q.sent_messages[1].seq_no, 7);
        assert_eq!(q.last_added_seq_no, Some(7));
    }

    #[tokio::test]
    async fn acknowledge_message_returns_error_when_empty() {
        let (mut q, _reader) = create_queue().await;

        let err = q.acknowledge_message(8).unwrap_err();

        assert!(err.to_string().contains("ack unexpected"));
        assert!(err.to_string().contains("seq_no=8"));
    }

    #[tokio::test]
    async fn acknowledge_message_errors_when_seq_no_mismatches() {
        let (mut q, _reader) = create_queue().await;
        q.add_message(create_message(1, vec![])).unwrap();
        let _ = q.get_messages_to_send_without_threshold().unwrap();

        let err = q.acknowledge_message(99).unwrap_err();

        assert!(err.to_string().contains("ack unexpected"));
        assert!(err.to_string().contains("seq_no=99"));
    }

    #[tokio::test]
    async fn reset_progress_restores_sent_messages_to_pending() {
        let (mut q, _reader) = create_queue().await;

        for i in 1..=2 {
            q.sent_messages.push_back(create_message(i, vec![]));
        }
        for i in 3..=5 {
            q.messages.push_back(create_message(i, vec![]));
        }

        q.reset_progress();

        assert!(q.sent_messages.is_empty());
        assert_eq!(q.messages.len(), 5);
        assert_eq!(q.messages[0].seq_no, 1);
        assert_eq!(q.messages[1].seq_no, 2);
        assert_eq!(q.messages[2].seq_no, 3);
        assert_eq!(q.messages[3].seq_no, 4);
        assert_eq!(q.messages[4].seq_no, 5);
    }

    #[tokio::test]
    async fn close_for_new_messages_prevents_adding_new_messages() {
        let (mut q, _reader) = create_queue().await;
        q.add_message(create_message(1, vec![])).unwrap();

        q.close_for_new_messages();

        assert!(!q.is_open_for_new_messages);
        let err = q.add_message(create_message(2, vec![])).unwrap_err();
        assert!(err.to_string().contains("closed for new messages"));
    }

    #[tokio::test]
    async fn wait_for_messages_to_be_acknowledged_completes_when_all_messages_are_acknowledged() {
        let q = MessageQueue::new();
        for i in 1..=5 {
            q.add_message(create_message(i, vec![])).await.unwrap();
        }
        let messages = q.get_messages_to_send_without_threshold().await.unwrap();
        assert_eq!(messages.len(), 5);

        for i in 1..=5 {
            q.acknowledge_message(i).await.unwrap();
        }

        q.wait_for_messages_to_be_acknowledged(&CancellationToken::new())
            .await;
    }
}
