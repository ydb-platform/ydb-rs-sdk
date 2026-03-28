use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{
    sync::{mpsc, Mutex as TokioMutex, Notify},
    time::timeout,
};
use tracing::log::trace;

use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::{YdbError, YdbResult};

#[derive(Clone)]
pub(crate) struct MessageQueue {
    inner: Arc<Mutex<MessageQueueInner>>,

    new_message_added: Arc<Notify>,
    message_acknowledged_rx: Arc<TokioMutex<mpsc::UnboundedReceiver<()>>>,
}

impl MessageQueue {
    pub(crate) fn new() -> Self {
        let new_message_added = Arc::new(Notify::new());
        let (message_acknowledged_tx, message_acknowledged_rx) = mpsc::unbounded_channel();

        Self {
            inner: Arc::new(Mutex::new(MessageQueueInner::new(
                new_message_added.clone(),
                message_acknowledged_tx,
            ))),
            new_message_added,
            message_acknowledged_rx: Arc::new(TokioMutex::new(message_acknowledged_rx)),
        }
    }

    pub(crate) fn add_message(&self, message: MessageData) -> YdbResult<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.add_message(message)
    }

    fn get_messages_to_send_without_threshold(&self) -> YdbResult<Vec<MessageData>> {
        let mut inner = self.inner.lock().unwrap();
        inner.get_messages_to_send_without_threshold()
    }

    fn get_messages_to_send_with_length_threshold(
        &self,
        length_threshold: usize,
    ) -> GetMessagesToSendResult {
        let mut inner = self.inner.lock().unwrap();
        inner.get_messages_to_send_with_length_threshold(length_threshold)
    }

    async fn get_messages_to_send_loop(
        &self,
        length_threshold: usize,
    ) -> YdbResult<Vec<MessageData>> {
        loop {
            tokio::select! {
                _ = self.new_message_added.notified() => {
                    match self.get_messages_to_send_with_length_threshold(length_threshold) {
                        GetMessagesToSendResult::Ok(messages) => return Ok(messages),
                        GetMessagesToSendResult::NotEnoughMessages => continue,
                        GetMessagesToSendResult::Err(error) => return Err(error),
                    }
                }
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
            Err(_) => self.get_messages_to_send_without_threshold(),
        }
    }

    pub(crate) fn acknowledge_message(&self, seq_no: i64) -> YdbResult<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.acknowledge_message(seq_no)
    }

    pub(crate) fn reset_progress(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.reset_progress()
    }

    pub(crate) fn close_for_new_messages(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.close_for_new_messages()
    }

    fn is_empty(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.is_empty()
    }

    pub(crate) async fn wait_for_all_messages_to_be_acknowledged(&self) {
        if self.is_empty() {
            return;
        }

        let mut message_acknowledged_rx = self.message_acknowledged_rx.lock().await;

        loop {
            tokio::select! {
                Some(_) = message_acknowledged_rx.recv() => {
                    if self.is_empty() {
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
    messages: VecDeque<MessageData>,

    // sequence number of the last message that has been added to the queue
    last_written_seq_no: i64,
    // index of the last message that has been 'sent'
    last_sent_index: Option<usize>,

    is_open_for_new_messages: bool,

    new_message_added: Arc<Notify>,
    message_acknowledged_tx: mpsc::UnboundedSender<()>,
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
        message_acknowledged_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            messages: VecDeque::new(),
            last_written_seq_no: -1,
            last_sent_index: None,
            is_open_for_new_messages: true,
            new_message_added,
            message_acknowledged_tx,
        }
    }

    fn add_message(&mut self, message: MessageData) -> YdbResult<()> {
        if !self.is_open_for_new_messages {
            return Err(YdbError::Custom(
                "message queue is closed for new messages".to_string(),
            ));
        }

        let seq_no = message.seq_no;
        self.check_message_seq_no(seq_no)?;

        self.last_written_seq_no = seq_no;

        self.messages.push_back(message);

        self.new_message_added.notify_waiters();

        Ok(())
    }

    fn check_message_seq_no(&self, seq_no: i64) -> YdbResult<()> {
        if seq_no <= self.last_written_seq_no {
            return Err(YdbError::InternalError(format!(
                "message with seq_no={} is not newer than the last written message",
                seq_no
            )));
        }
        Ok(())
    }

    fn get_length_of_messages_to_send(&self) -> YdbResult<usize> {
        let len = self.messages.len();
        match self.last_sent_index {
            None => Ok(len),
            Some(sent_idx) if sent_idx >= len => Err(YdbError::from(
                "message queue: last_sent_index is bigger than the number of messages",
            )),
            Some(sent_idx) => Ok(len - 1 - sent_idx),
        }
    }

    fn do_get_messages_to_send(
        messages: &VecDeque<MessageData>,
        last_sent_index: &mut Option<usize>,
        length: usize,
    ) -> Vec<MessageData> {
        let mut result = Vec::with_capacity(length);
        for _ in 0..length {
            let idx = last_sent_index.map_or(0, |index| index + 1);
            result.push(messages[idx].clone());
            *last_sent_index = Some(idx);
        }
        result
    }

    fn get_messages_to_send_without_threshold(&mut self) -> YdbResult<Vec<MessageData>> {
        let length = self.get_length_of_messages_to_send()?;

        Ok(MessageQueueInner::do_get_messages_to_send(
            &self.messages,
            &mut self.last_sent_index,
            length,
        ))
    }

    fn get_messages_to_send_with_length_threshold(
        &mut self,
        length_threshold: usize,
    ) -> GetMessagesToSendResult {
        let length = match self.get_length_of_messages_to_send() {
            Ok(length) => length,
            Err(error) => return GetMessagesToSendResult::Err(error),
        };

        if length < length_threshold {
            return GetMessagesToSendResult::NotEnoughMessages;
        }

        GetMessagesToSendResult::Ok(MessageQueueInner::do_get_messages_to_send(
            &self.messages,
            &mut self.last_sent_index,
            length,
        ))
    }

    fn acknowledge_message(&mut self, seq_no: i64) -> YdbResult<()> {
        let Some(message) = self.messages.pop_front() else {
            return Err(YdbError::Custom(format!(
                "ack unexpected message with seq_no={}",
                seq_no
            )));
        };

        if message.seq_no != seq_no {
            return Err(YdbError::Custom(format!(
                "ack unexpected message with seq_no={}",
                seq_no
            )));
        }

        self.last_sent_index = match self.last_sent_index {
            Some(idx) if idx > 0 => Some(idx - 1),
            _ => None,
        };

        self.message_acknowledged_tx.send(()).unwrap();

        Ok(())
    }

    fn reset_progress(&mut self) {
        self.last_sent_index = None;
    }

    fn close_for_new_messages(&mut self) {
        self.is_open_for_new_messages = false;
    }

    fn is_empty(&self) -> bool {
        self.messages.is_empty()
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
        assert_eq!(q.last_written_seq_no, -1);
        assert_eq!(q.last_sent_index, None);
        assert!(q.is_open_for_new_messages);
    }

    #[tokio::test]
    async fn get_length_of_messages_to_send_empty() {
        let (q, _reader) = create_queue().await;
        match q.get_length_of_messages_to_send() {
            Ok(length) => assert_eq!(length, 0),
            Err(error) => panic!("{}", error.to_string()),
        }
    }

    #[tokio::test]
    async fn get_length_of_messages_to_send_with_some_added_and_none_sent() {
        let (mut q, _reader) = create_queue().await;
        q.messages.push_back(create_message(1, vec![]));
        q.messages.push_back(create_message(2, vec![]));
        q.messages.push_back(create_message(3, vec![]));
        q.messages.push_back(create_message(4, vec![]));
        q.messages.push_back(create_message(5, vec![]));
        q.last_sent_index = None;
        assert_eq!(q.get_length_of_messages_to_send().unwrap(), 5);
    }

    #[tokio::test]
    async fn get_length_of_messages_to_send_with_some_added_and_one_sent() {
        let (mut q, _reader) = create_queue().await;
        q.messages.push_back(create_message(1, vec![]));
        q.messages.push_back(create_message(2, vec![]));
        q.messages.push_back(create_message(3, vec![]));
        q.messages.push_back(create_message(4, vec![]));
        q.messages.push_back(create_message(5, vec![]));
        q.last_sent_index = Some(0);
        assert_eq!(q.get_length_of_messages_to_send().unwrap(), 4);
    }

    #[tokio::test]
    async fn get_length_of_messages_to_send_with_some_added_and_same_sent() {
        let (mut q, _reader) = create_queue().await;
        q.messages.push_back(create_message(1, vec![]));
        q.messages.push_back(create_message(2, vec![]));
        q.messages.push_back(create_message(3, vec![]));
        q.messages.push_back(create_message(4, vec![]));
        q.messages.push_back(create_message(5, vec![]));
        q.last_sent_index = Some(4);
        assert_eq!(q.get_length_of_messages_to_send().unwrap(), 0);
    }

    #[tokio::test]
    async fn get_length_of_messages_to_send_with_none_added_and_some_sent() {
        let (mut q, _reader) = create_queue().await;
        q.last_sent_index = Some(4);
        let err = q.get_length_of_messages_to_send().unwrap_err();
        assert!(err
            .to_string()
            .contains("last_sent_index is bigger than the number of messages"));
    }

    #[tokio::test]
    async fn add_message_appends_and_updates_fields() {
        let (mut q, _reader) = create_queue().await;
        q.add_message(create_message(10, vec![1, 2, 3])).unwrap();
        q.add_message(create_message(11, vec![4, 5])).unwrap();

        assert_eq!(q.get_length_of_messages_to_send().unwrap(), 2);
        assert_eq!(q.messages.len(), 2);
        assert_eq!(q.messages[0].seq_no, 10);
        assert_eq!(q.messages[0].data, vec![1, 2, 3]);
        assert_eq!(q.messages[1].seq_no, 11);
        assert_eq!(q.messages[1].data, vec![4, 5]);

        assert_eq!(q.last_written_seq_no, 11);
        assert_eq!(q.last_sent_index, None);
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
    async fn get_messages_to_send_returns_all_and_advances_last_sent_index() {
        let (mut q, _reader) = create_queue().await;
        q.add_message(create_message(3, vec![10])).unwrap();
        q.add_message(create_message(4, vec![20])).unwrap();

        let batch = q.get_messages_to_send_without_threshold().unwrap();

        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].seq_no, 3);
        assert_eq!(batch[1].seq_no, 4);

        assert_eq!(q.messages.len(), 2);
        assert_eq!(q.last_sent_index, Some(1));
        assert_eq!(q.last_written_seq_no, 4);
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
            GetMessagesToSendResult::Ok(msgs) => assert_eq!(msgs.len(), 3),
            other => panic!("expected Ok(...), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn get_messages_to_send_returns_batch_when_threshold_met() {
        let q = MessageQueue::new();
        q.add_message(create_message(1, vec![1])).unwrap();
        q.add_message(create_message(2, vec![2])).unwrap();
        q.add_message(create_message(3, vec![3])).unwrap();
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

        assert_eq!(q.get_length_of_messages_to_send().unwrap(), 0);
        assert_eq!(q.messages.len(), 2);
        assert_eq!(q.last_sent_index, Some(1));
        assert_eq!(q.last_written_seq_no, 7);
    }

    #[tokio::test]
    async fn acknowledge_message_returns_error_when_empty() {
        let (mut q, _reader) = create_queue().await;

        let err = q.acknowledge_message(8).unwrap_err();

        assert!(err.to_string().contains("ack unexpected"));
        assert!(err.to_string().contains("seq_no=8"));
    }

    #[tokio::test]
    async fn acknowledge_message_errors_when_seq_no_mismatch() {
        let (mut q, _reader) = create_queue().await;
        q.add_message(create_message(1, vec![])).unwrap();
        let _ = q.get_messages_to_send_without_threshold().unwrap();

        let err = q.acknowledge_message(99).unwrap_err();

        assert!(err.to_string().contains("ack unexpected"));
        assert!(err.to_string().contains("seq_no=99"));
    }

    #[tokio::test]
    async fn reset_progress_clears_last_sent_so_messages_can_be_resent() {
        let (mut q, _reader) = create_queue().await;
        q.add_message(create_message(8, vec![])).unwrap();
        q.add_message(create_message(9, vec![])).unwrap();

        let msgs = q.get_messages_to_send_without_threshold().unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].seq_no, 8);
        assert_eq!(msgs[1].seq_no, 9);

        assert_eq!(q.last_sent_index, Some(1));
        assert_eq!(q.get_length_of_messages_to_send().unwrap(), 0);
        q.reset_progress();
        assert_eq!(q.last_sent_index, None);
        assert_eq!(q.get_length_of_messages_to_send().unwrap(), 2);

        let again = q.get_messages_to_send_without_threshold().unwrap();
        assert_eq!(again.len(), 2);
        assert_eq!(again[0].seq_no, 8);
        assert_eq!(again[1].seq_no, 9);
        assert_eq!(q.last_sent_index, Some(1));
    }

    #[tokio::test]
    async fn close_for_new_messages_prevents_further_adds() {
        let (mut q, _reader) = create_queue().await;
        q.add_message(create_message(1, vec![])).unwrap();

        q.close_for_new_messages();

        assert!(!q.is_open_for_new_messages);
        let err = q.add_message(create_message(2, vec![])).unwrap_err();
        assert!(err.to_string().contains("closed for new messages"));
    }

    #[tokio::test]
    async fn wait_for_all_messages_to_be_acknowledged_completes_when_empty_after_ack() {
        let q = MessageQueue::new();
        q.add_message(create_message(1, vec![])).unwrap();
        let _ = q
            .get_messages_to_send(1, std::time::Duration::from_millis(10))
            .await
            .unwrap();
        q.acknowledge_message(1).unwrap();
        q.wait_for_all_messages_to_be_acknowledged().await;
    }
}
