use std::collections::HashMap;

use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::{YdbError, YdbResult};

pub(crate) struct MessageQueue {
    // order_no -> message
    messages_by_order_no: HashMap<u64, MessageData>,
    // seq_no -> order_no
    order_nos_by_seq_no: HashMap<i64, u64>,

    // order number of the last message that has been added to the queue
    last_written_order_no: u64,
    // sequence number of the last message that has been added to the queue
    last_written_seq_no: i64,
    // order number of the last message that has been 'sent'
    last_sent_order_no: u64,

    is_open: bool,
}

// TODO: add a method to wait for all messages to be sent and use this method in stop() / flush()
impl MessageQueue {
    pub(crate) fn new() -> Self {
        Self {
            messages_by_order_no: HashMap::new(),
            order_nos_by_seq_no: HashMap::new(),
            last_written_order_no: 0,
            last_written_seq_no: -1,
            last_sent_order_no: 0,
            is_open: true,
        }
    }

    pub(crate) fn add_message(&mut self, message: MessageData) -> YdbResult<()> {
        if !self.is_open {
            return Err(YdbError::Custom("message queue is closed".to_string()));
        }

        let seq_no = message.seq_no;
        self.check_message_seq_no(seq_no)?;

        self.last_written_order_no += 1;
        self.last_written_seq_no = seq_no;

        self.order_nos_by_seq_no
            .insert(message.seq_no, self.last_written_order_no);
        self.messages_by_order_no
            .insert(self.last_written_order_no, message);

        Ok(())
    }

    fn check_message_seq_no(&self, seq_no: i64) -> YdbResult<()> {
        if seq_no <= self.last_written_seq_no {
            return Err(YdbError::InternalError(format!(
                "message with seq_no={} is older than the last written message",
                seq_no
            )));
        }
        Ok(())
    }

    fn do_get_messages_to_send(
        last_sent_order_no: &mut u64,
        last_written_order_no: u64,
        messages_by_order_no: &HashMap<u64, MessageData>,
    ) -> Vec<MessageData> {
        let length: usize = last_written_order_no as usize - *last_sent_order_no as usize;
        let mut messages = Vec::with_capacity(length);
        while *last_sent_order_no != last_written_order_no {
            *last_sent_order_no += 1;

            let Some(message) = messages_by_order_no.get(last_sent_order_no) else {
                continue;
            };

            messages.push(message.clone());
        }
        messages
    }

    pub(crate) fn get_messages_to_send(&mut self) -> Vec<MessageData> {
        MessageQueue::do_get_messages_to_send(
            &mut self.last_sent_order_no,
            self.last_written_order_no,
            &self.messages_by_order_no,
        )
    }

    pub(crate) fn get_messages_to_send_if_big_enough(
        &mut self,
        target: usize,
    ) -> (Option<Vec<MessageData>>, usize) {
        let length: usize = self.last_written_order_no as usize - self.last_sent_order_no as usize;
        if length < target {
            return (None, length);
        }

        (
            Some(MessageQueue::do_get_messages_to_send(
                &mut self.last_sent_order_no,
                self.last_written_order_no,
                &self.messages_by_order_no,
            )),
            length,
        )
    }

    pub(crate) fn acknowledge_message(&mut self, seq_no: i64) -> YdbResult<()> {
        let Some(order_no) = self.order_nos_by_seq_no.remove(&seq_no) else {
            return Err(YdbError::Custom(format!(
                "ack unexpected message with seq_no={}",
                seq_no
            )));
        };
        self.messages_by_order_no.remove(&order_no);

        Ok(())
    }

    pub(crate) fn reset_progress(&mut self) {
        let Some(min_order_no) = self.order_nos_by_seq_no.values().min() else {
            return;
        };

        self.last_written_order_no = *min_order_no - 1;
    }

    pub(crate) fn close(&mut self) {
        self.is_open = false;
    }
}
