use std::{collections::VecDeque, mem::swap, sync::Arc};

use ydb_grpc::ydb_proto::topic::TransactionIdentity;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::client_topic::topicwriter::capacity_limiter::CapacityPermit;
use crate::client_topic::topicwriter::writer_options::AutoFlushSettings;
use crate::{YdbError, YdbResult};

pub(crate) struct QueuedMessage {
    data: MessageData,
    transaction: Option<Arc<TransactionIdentity>>,
    _capacity: CapacityPermit,
}

impl QueuedMessage {
    pub(crate) fn new(
        data: MessageData,
        transaction: Option<Arc<TransactionIdentity>>,
        capacity: CapacityPermit,
    ) -> Self {
        Self {
            data,
            transaction,
            _capacity: capacity,
        }
    }
}

pub(crate) struct MessageQueue {
    // Messages awaiting to be sent
    messages: VecDeque<QueuedMessage>,
    // Messages awaiting to be acknowledged
    sent_messages: VecDeque<QueuedMessage>,
    // Sequence number of the last message that has been added to the queue
    last_added_seq_no: Option<i64>,
}

#[derive(Debug)]
pub(crate) enum AppendMessageToSendBufferResult {
    Full,
    UnderThreshold,
    CouldNotGetMessage,
}

impl MessageQueue {
    pub(crate) fn new() -> Self {
        Self {
            messages: VecDeque::new(),
            sent_messages: VecDeque::new(),
            last_added_seq_no: None,
        }
    }

    pub(crate) fn add_message(&mut self, message: QueuedMessage) -> YdbResult<()> {
        let seq_no = message.data.seq_no;
        self.check_message_seq_no(seq_no)?;

        self.last_added_seq_no = Some(seq_no);

        self.messages.push_back(message);

        Ok(())
    }

    fn check_message_seq_no(&self, seq_no: i64) -> YdbResult<()> {
        match self.last_added_seq_no {
            Some(last_added_seq_no) if seq_no <= last_added_seq_no => {
                Err(YdbError::custom(format!(
                    "message seq_no is not greater than the last written message: seq_no={seq_no}, last_added_seq_no={last_added_seq_no}",
                )))
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn append_message_to_send_buffer(
        &mut self,
        send_buffer: &mut Vec<MessageData>,
        send_buffer_bytes: &mut usize,
        transaction: &mut Option<Arc<TransactionIdentity>>,
        settings: AutoFlushSettings,
    ) -> AppendMessageToSendBufferResult {
        let Some(message) = self.messages.pop_front() else {
            return AppendMessageToSendBufferResult::CouldNotGetMessage;
        };

        if send_buffer.is_empty() {
            // WriterState admits messages only for the buffer's current transaction binding, so
            // every message collected into this batch has the same transaction identity.
            *transaction = message.transaction.clone();
        }

        *send_buffer_bytes += message.data.data.len();
        send_buffer.push(message.data.clone());
        self.sent_messages.push_back(message);

        if send_buffer.len() >= settings.messages() || *send_buffer_bytes >= settings.bytes() {
            AppendMessageToSendBufferResult::Full
        } else {
            AppendMessageToSendBufferResult::UnderThreshold
        }
    }

    pub(crate) fn acknowledge_message(&mut self, seq_no: i64) -> YdbResult<()> {
        let Some(message) = self.sent_messages.pop_front() else {
            return Err(YdbError::custom(format!(
                "acknowledge_message: queue is empty, got unexpected message: seq_no={seq_no}",
            )));
        };

        if message.data.seq_no != seq_no {
            return Err(YdbError::custom(format!(
                "acknowledge_message: seq_no mismatch: expected_seq_no={} actual_seq_no={}",
                message.data.seq_no, seq_no,
            )));
        }

        if self.sent_messages.is_empty() && self.messages.is_empty() {
            self.last_added_seq_no = None;
        }

        Ok(())
    }

    pub(crate) fn reset_progress(&mut self) {
        self.sent_messages.append(&mut self.messages);
        swap(&mut self.messages, &mut self.sent_messages);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.messages.is_empty() && self.sent_messages.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use super::*;
    use crate::client_topic::topicwriter::capacity_limiter::CapacityLimiter;
    use crate::client_topic::topicwriter::test_helpers::{
        auto_flush_settings, create_message as create_message_data,
    };

    fn create_message(seq_no: i64, data: Vec<u8>) -> QueuedMessage {
        let limiter = CapacityLimiter::new(
            NonZeroUsize::MIN,
            NonZeroUsize::new(data.len()).unwrap_or(NonZeroUsize::MIN),
        )
        .unwrap();
        let capacity = limiter.try_acquire(data.len()).unwrap();

        QueuedMessage::new(create_message_data(seq_no, data), None, capacity)
    }

    fn move_all_pending_to_sent(q: &mut MessageQueue) {
        q.sent_messages.append(&mut q.messages);
    }

    fn append_for_test(
        queue: &mut MessageQueue,
        buffer: &mut Vec<MessageData>,
        buffer_bytes: &mut usize,
        settings: AutoFlushSettings,
    ) -> AppendMessageToSendBufferResult {
        queue.append_message_to_send_buffer(buffer, buffer_bytes, &mut None, settings)
    }

    #[test]
    fn new_creates_empty_queue() {
        let q = MessageQueue::new();
        assert!(q.last_added_seq_no.is_none());
        assert!(q.messages.is_empty());
        assert!(q.sent_messages.is_empty());
    }

    #[test]
    fn add_message_appends_and_updates_fields() {
        let mut q = MessageQueue::new();
        q.add_message(create_message(10, vec![1, 2, 3])).unwrap();
        q.add_message(create_message(11, vec![4, 5])).unwrap();

        assert_eq!(q.messages.len(), 2);
        assert_eq!(q.messages[0].data.seq_no, 10);
        assert_eq!(q.messages[0].data.data, vec![1, 2, 3]);
        assert_eq!(q.messages[1].data.seq_no, 11);
        assert_eq!(q.messages[1].data.data, vec![4, 5]);
        assert_eq!(q.last_added_seq_no, Some(11));
    }

    #[test]
    fn add_message_rejects_duplicate_seq_no() {
        let mut q = MessageQueue::new();
        q.add_message(create_message(4, vec![])).unwrap();

        let err = q.add_message(create_message(4, vec![])).unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("message seq_no is not greater than the last written message"));
        assert!(err_msg.contains("seq_no=4"));
        assert!(err_msg.contains("last_added_seq_no=4"));
    }

    #[test]
    fn add_message_rejects_out_of_order_seq_no() {
        let mut q = MessageQueue::new();
        q.add_message(create_message(10, vec![])).unwrap();

        let err = q.add_message(create_message(7, vec![])).unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("message seq_no is not greater than the last written message"));
        assert!(err_msg.contains("seq_no=7"));
        assert!(err_msg.contains("last_added_seq_no=10"));
    }

    #[test]
    fn append_message_to_send_buffer_moves_message_to_sent() {
        let mut q = MessageQueue::new();
        q.add_message(create_message(1, vec![10])).unwrap();

        let mut buffer = Vec::new();
        let mut buffer_bytes = 0;
        let result = append_for_test(
            &mut q,
            &mut buffer,
            &mut buffer_bytes,
            auto_flush_settings(10, 10, Duration::ZERO),
        );

        assert!(matches!(
            result,
            AppendMessageToSendBufferResult::UnderThreshold
        ));
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].seq_no, 1);
        assert_eq!(buffer[0].data, vec![10]);
        assert_eq!(buffer_bytes, 1);
        assert!(q.messages.is_empty());
        assert_eq!(q.sent_messages.len(), 1);
        assert_eq!(q.sent_messages[0].data.seq_no, 1);
    }

    #[test]
    fn append_message_to_send_buffer_returns_could_not_get_message_when_empty() {
        let mut q = MessageQueue::new();
        let mut buffer = Vec::new();
        let mut buffer_bytes = 0;

        let result = append_for_test(
            &mut q,
            &mut buffer,
            &mut buffer_bytes,
            auto_flush_settings(10, 10, Duration::ZERO),
        );

        assert!(matches!(
            result,
            AppendMessageToSendBufferResult::CouldNotGetMessage
        ));
        assert!(buffer.is_empty());
    }

    #[test]
    fn append_message_to_send_buffer_returns_full_when_threshold_reached() {
        let mut q = MessageQueue::new();
        q.add_message(create_message(1, vec![])).unwrap();
        q.add_message(create_message(2, vec![])).unwrap();

        let mut buffer = Vec::new();
        let mut buffer_bytes = 0;
        assert!(matches!(
            append_for_test(
                &mut q,
                &mut buffer,
                &mut buffer_bytes,
                auto_flush_settings(2, 10, Duration::ZERO),
            ),
            AppendMessageToSendBufferResult::UnderThreshold
        ));
        assert!(matches!(
            append_for_test(
                &mut q,
                &mut buffer,
                &mut buffer_bytes,
                auto_flush_settings(2, 10, Duration::ZERO),
            ),
            AppendMessageToSendBufferResult::Full
        ));
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer[0].seq_no, 1);
        assert_eq!(buffer[1].seq_no, 2);
    }

    #[test]
    fn append_message_to_send_buffer_returns_full_at_byte_threshold() {
        let mut q = MessageQueue::new();
        q.add_message(create_message(1, vec![0; 2])).unwrap();
        q.add_message(create_message(2, vec![0; 3])).unwrap();

        let mut buffer = Vec::new();
        let mut buffer_bytes = 0;
        let settings = auto_flush_settings(10, 5, Duration::ZERO);
        assert!(matches!(
            append_for_test(&mut q, &mut buffer, &mut buffer_bytes, settings),
            AppendMessageToSendBufferResult::UnderThreshold
        ));
        assert!(matches!(
            append_for_test(&mut q, &mut buffer, &mut buffer_bytes, settings),
            AppendMessageToSendBufferResult::Full
        ));
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer_bytes, 5);
    }

    #[test]
    fn append_message_to_send_buffer_flushes_after_crossing_byte_threshold() {
        let mut q = MessageQueue::new();
        q.add_message(create_message(1, vec![0; 3])).unwrap();
        q.add_message(create_message(2, vec![0; 3])).unwrap();

        let mut buffer = Vec::new();
        let mut buffer_bytes = 0;
        let settings = auto_flush_settings(10, 5, Duration::ZERO);
        assert!(matches!(
            append_for_test(&mut q, &mut buffer, &mut buffer_bytes, settings),
            AppendMessageToSendBufferResult::UnderThreshold
        ));
        assert!(matches!(
            append_for_test(&mut q, &mut buffer, &mut buffer_bytes, settings),
            AppendMessageToSendBufferResult::Full
        ));
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer_bytes, 6);
        assert!(q.messages.is_empty());
    }

    #[test]
    fn acknowledge_message_removes_front_when_seq_no_matches() {
        let mut q = MessageQueue::new();
        q.add_message(create_message(5, vec![])).unwrap();
        q.add_message(create_message(6, vec![])).unwrap();
        q.add_message(create_message(7, vec![])).unwrap();
        move_all_pending_to_sent(&mut q);

        q.acknowledge_message(5).unwrap();

        assert_eq!(q.messages.len(), 0);
        assert!(q.messages.is_empty());
        assert_eq!(q.sent_messages.len(), 2);
        assert_eq!(q.sent_messages[0].data.seq_no, 6);
        assert_eq!(q.sent_messages[1].data.seq_no, 7);
        assert_eq!(q.last_added_seq_no, Some(7));
    }

    #[test]
    fn acknowledge_message_clears_last_added_seq_no_when_all_queues_empty() {
        let mut q = MessageQueue::new();
        q.add_message(create_message(1, vec![])).unwrap();
        move_all_pending_to_sent(&mut q);

        q.acknowledge_message(1).unwrap();

        assert!(q.messages.is_empty());
        assert!(q.sent_messages.is_empty());
        assert!(q.last_added_seq_no.is_none());
    }

    #[test]
    fn acknowledge_message_returns_error_when_empty() {
        let mut q = MessageQueue::new();

        let err = q.acknowledge_message(8).unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("acknowledge_message: queue is empty, got unexpected message"));
        assert!(err_msg.contains("seq_no=8"));
    }

    #[test]
    fn acknowledge_message_errors_when_seq_no_mismatches() {
        let mut q = MessageQueue::new();
        q.add_message(create_message(1, vec![])).unwrap();
        move_all_pending_to_sent(&mut q);

        let err = q.acknowledge_message(99).unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("acknowledge_message: seq_no mismatch"));
        assert!(err_msg.contains("actual_seq_no=99"));
        assert!(err_msg.contains("expected_seq_no=1"));
    }

    #[test]
    fn reset_progress_restores_sent_messages_to_pending() {
        let mut q = MessageQueue::new();

        for i in 1..=2 {
            q.sent_messages.push_back(create_message(i, vec![]));
        }
        for i in 3..=5 {
            q.messages.push_back(create_message(i, vec![]));
        }

        q.reset_progress();

        assert!(q.sent_messages.is_empty());
        assert_eq!(q.messages.len(), 5);
        assert_eq!(q.messages[0].data.seq_no, 1);
        assert_eq!(q.messages[1].data.seq_no, 2);
        assert_eq!(q.messages[2].data.seq_no, 3);
        assert_eq!(q.messages[3].data.seq_no, 4);
        assert_eq!(q.messages[4].data.seq_no, 5);
    }
}
