use std::{collections::VecDeque, mem::swap};
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::{YdbError, YdbResult};

pub(crate) struct MessageQueueInner {
    // Messages awaiting to be sent
    messages: VecDeque<MessageData>,
    // Messages awaiting to be acknowledged
    sent_messages: VecDeque<MessageData>,

    // Sequence number of the last message that has been added to the queue
    last_added_seq_no: Option<i64>,
}

#[derive(Debug)]
pub(crate) enum AppendMessageToSendBufferResult {
    Full,
    UnderThreshold,
    CouldNotGetMessage,
}

impl MessageQueueInner {
    pub(crate) fn new() -> Self {
        Self {
            messages: VecDeque::new(),
            sent_messages: VecDeque::new(),
            last_added_seq_no: None,
        }
    }

    pub(crate) fn add_message(&mut self, message: MessageData) -> YdbResult<()> {
        let seq_no = message.seq_no;
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
        threshold: usize,
    ) -> AppendMessageToSendBufferResult {
        let Some(_) = self.messages.front() else {
            return AppendMessageToSendBufferResult::CouldNotGetMessage;
        };

        let message = self.messages.pop_front().unwrap();
        send_buffer.push(message.clone());
        self.sent_messages.push_back(message);

        if send_buffer.len() < threshold {
            AppendMessageToSendBufferResult::UnderThreshold
        } else {
            AppendMessageToSendBufferResult::Full
        }
    }

    pub(crate) fn acknowledge_message(&mut self, seq_no: i64) -> YdbResult<()> {
        let Some(message) = self.sent_messages.pop_front() else {
            return Err(YdbError::custom(format!(
                "acknowledge_message: queue is empty, got unexpected message: seq_no={seq_no}",
            )));
        };

        if message.seq_no != seq_no {
            return Err(YdbError::custom(format!(
                "acknowledge_message: seq_no mismatch: expected_seq_no={} actual_seq_no={}",
                message.seq_no, seq_no,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, time::Duration};
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

    fn move_all_pending_to_sent(q: &mut MessageQueueInner) {
        q.sent_messages.append(&mut q.messages);
    }

    #[tokio::test]
    async fn new_creates_empty_queue() {
        let q = MessageQueueInner::new();
        assert!(q.last_added_seq_no.is_none());
        assert!(q.messages.is_empty());
        assert!(q.sent_messages.is_empty());
    }

    #[tokio::test]
    async fn add_message_appends_and_updates_fields() {
        let mut q = MessageQueueInner::new();
        q.add_message(create_message(10, vec![1, 2, 3])).unwrap();
        q.add_message(create_message(11, vec![4, 5])).unwrap();

        assert_eq!(q.messages.len(), 2);
        assert_eq!(q.messages[0].seq_no, 10);
        assert_eq!(q.messages[0].data, vec![1, 2, 3]);
        assert_eq!(q.messages[1].seq_no, 11);
        assert_eq!(q.messages[1].data, vec![4, 5]);
        assert_eq!(q.last_added_seq_no, Some(11));
    }

    #[tokio::test]
    async fn add_message_rejects_duplicate_seq_no() {
        let q = MessageQueue::new();
        q.add_message(create_message(4, vec![])).await.unwrap();

        let err = q.add_message(create_message(4, vec![])).await.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("message seq_no is not greater than the last written message"));
        assert!(err_msg.contains("seq_no=4"));
        assert!(err_msg.contains("last_added_seq_no=4"));
    }

    #[tokio::test]
    async fn add_message_rejects_out_of_order_seq_no() {
        let q = MessageQueue::new();
        q.add_message(create_message(10, vec![])).await.unwrap();

        let err = q.add_message(create_message(7, vec![])).await.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("message seq_no is not greater than the last written message"));
        assert!(err_msg.contains("seq_no=7"));
        assert!(err_msg.contains("last_added_seq_no=10"));
    }

    #[tokio::test]
    async fn add_message_rejects_when_queue_closed_for_new_messages() {
        let q = MessageQueue::new();
        q.close_for_new_messages().await;

        let err = q.add_message(create_message(1, vec![])).await.unwrap_err();

        assert!(err.to_string().contains("closed for new messages"));
    }

    #[tokio::test]
    async fn get_messages_to_send_moves_batch_to_sent_and_can_ack() {
        let q = Arc::new(MessageQueue::new());

        let q_collect = Arc::clone(&q);
        let collect_handle = tokio::spawn(async move {
            q_collect
                .get_messages_to_send(10, Duration::from_millis(50))
                .await
        });
        q.add_message(create_message(1, vec![10])).await.unwrap();
        q.add_message(create_message(2, vec![20])).await.unwrap();

        let batch = collect_handle.await.unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].seq_no, 1);
        assert_eq!(batch[1].seq_no, 2);

        q.acknowledge_message(1).await.unwrap();
        q.acknowledge_message(2).await.unwrap();
    }

    #[tokio::test]
    async fn get_messages_to_send_empty_queue_times_out_empty() {
        let q = MessageQueue::new();
        let msgs = q.get_messages_to_send(10, Duration::from_millis(20)).await;
        assert!(msgs.is_empty());
    }

    #[tokio::test]
    async fn get_messages_to_send_drains_messages_added_before_call() {
        let q = MessageQueue::new();
        q.add_message(create_message(1, vec![])).await.unwrap();
        q.add_message(create_message(2, vec![])).await.unwrap();

        let msgs = q.get_messages_to_send(10, Duration::from_millis(20)).await;

        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].seq_no, 1);
        assert_eq!(msgs[1].seq_no, 2);
    }

    #[tokio::test]
    async fn get_messages_to_send_with_zero_duration_still_drains_existing_messages() {
        let q = MessageQueue::new();
        q.add_message(create_message(1, vec![])).await.unwrap();

        let msgs = q.get_messages_to_send(10, Duration::ZERO).await;

        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].seq_no, 1);
    }

    #[tokio::test]
    async fn get_messages_to_send_with_zero_threshold_doesnt_move_messages_to_sent() {
        let q = MessageQueue::new();
        q.add_message(create_message(1, vec![])).await.unwrap();
        q.add_message(create_message(2, vec![])).await.unwrap();

        let msgs = q.get_messages_to_send(0, Duration::from_millis(50)).await;
        assert!(msgs.is_empty());

        let err = q.acknowledge_message(1).await.unwrap_err();
        assert!(err.to_string().contains("queue is empty"));

        let msgs = q.get_messages_to_send(10, Duration::from_millis(20)).await;
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].seq_no, 1);
        assert_eq!(msgs[1].seq_no, 2);
    }

    #[tokio::test]
    async fn get_messages_to_send_collects_one_message_per_add_notification() {
        let q = Arc::new(MessageQueue::new());
        let q_collect = Arc::clone(&q);
        let collect_handle = tokio::spawn(async move {
            q_collect
                .get_messages_to_send(10, Duration::from_millis(50))
                .await
        });
        q.add_message(create_message(1, vec![])).await.unwrap();

        let msgs = collect_handle.await.unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].seq_no, 1);
    }

    #[tokio::test]
    async fn get_messages_to_send_respects_threshold() {
        let q = Arc::new(MessageQueue::new());
        let q_collect = Arc::clone(&q);
        let collect_handle = tokio::spawn(async move {
            q_collect
                .get_messages_to_send(2, Duration::from_millis(50))
                .await
        });
        q.add_message(create_message(1, vec![])).await.unwrap();
        q.add_message(create_message(2, vec![])).await.unwrap();
        q.add_message(create_message(3, vec![])).await.unwrap();

        let msgs = collect_handle.await.unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].seq_no, 1);
        assert_eq!(msgs[1].seq_no, 2);
    }

    #[tokio::test]
    async fn get_messages_to_send_second_call_drains_remaining_without_new_notify() {
        let q = Arc::new(MessageQueue::new());
        let q1 = Arc::clone(&q);
        let h1 =
            tokio::spawn(
                async move { q1.get_messages_to_send(2, Duration::from_millis(50)).await },
            );
        q.add_message(create_message(11, vec![])).await.unwrap();
        q.add_message(create_message(12, vec![])).await.unwrap();
        q.add_message(create_message(13, vec![])).await.unwrap();
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
    async fn acknowledge_message_removes_front_when_seq_no_matches() {
        let mut q = MessageQueueInner::new();
        q.add_message(create_message(5, vec![])).unwrap();
        q.add_message(create_message(6, vec![])).unwrap();
        q.add_message(create_message(7, vec![])).unwrap();
        move_all_pending_to_sent(&mut q);

        q.acknowledge_message(5).unwrap();

        assert_eq!(q.messages.len(), 0);
        assert!(q.messages.is_empty());
        assert_eq!(q.sent_messages.len(), 2);
        assert_eq!(q.sent_messages[0].seq_no, 6);
        assert_eq!(q.sent_messages[1].seq_no, 7);
        assert_eq!(q.last_added_seq_no, Some(7));
    }

    #[tokio::test]
    async fn acknowledge_message_returns_error_when_empty() {
        let q = MessageQueue::new();

        let err = q.acknowledge_message(8).await.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("acknowledge_message: queue is empty, got unexpected message"));
        assert!(err_msg.contains("seq_no=8"));
    }

    #[tokio::test]
    async fn acknowledge_message_errors_when_seq_no_mismatches() {
        let q = MessageQueue::new();
        q.add_message(create_message(1, vec![])).await.unwrap();
        let messages = q.get_messages_to_send(10, Duration::from_millis(20)).await;
        assert_eq!(messages.len(), 1);

        let err = q.acknowledge_message(99).await.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("acknowledge_message: seq_no mismatch"));
        assert!(err_msg.contains("actual_seq_no=99"));
        assert!(err_msg.contains("expected_seq_no=1"));
    }

    #[tokio::test]
    async fn reset_progress_restores_sent_messages_to_pending() {
        let mut q = MessageQueueInner::new();

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
        let q = MessageQueue::new();
        q.add_message(create_message(1, vec![])).await.unwrap();

        q.close_for_new_messages().await;

        let err = q.add_message(create_message(2, vec![])).await.unwrap_err();
        assert!(err.to_string().contains("closed for new messages"));
    }

    #[tokio::test]
    async fn wait_for_messages_to_be_acknowledged_completes_when_all_messages_are_acknowledged() {
        let q = Arc::new(MessageQueue::new());
        let q_collect = Arc::clone(&q);
        let collect_handle = tokio::spawn(async move {
            q_collect
                .get_messages_to_send(20, Duration::from_millis(50))
                .await
        });
        for i in 1..=5 {
            q.add_message(create_message(i, vec![])).await.unwrap();
        }
        let messages = collect_handle.await.unwrap();
        assert_eq!(messages.len(), 5);

        for i in 1..=5 {
            q.acknowledge_message(i).await.unwrap();
        }

        q.wait_for_messages_to_be_acknowledged(&CancellationToken::new())
            .await;
    }

    #[tokio::test]
    async fn wait_for_messages_to_be_acknowledged_completes_with_no_messages() {
        let q = MessageQueue::new();

        q.wait_for_messages_to_be_acknowledged(&CancellationToken::new())
            .await;
    }
}
