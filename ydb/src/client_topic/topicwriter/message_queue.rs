use std::collections::HashMap;

use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::{YdbError, YdbResult};

pub(crate) struct MessageQueue {
    messages_by_order_no: HashMap<u64, MessageData>,
    order_nos_by_seq_no: HashMap<i64, u64>,

    // order number of the last message that has been added to the queue
    last_written_order_no: u64,
    // order number of the last message that has been 'sent'
    last_sent_order_no: u64,
}

// TODO: mutex!!!
// TODO: check that the new message has a greater seq_no (add last_seq_no)
impl MessageQueue {
    pub(crate) fn new() -> Self {
        Self {
            messages_by_order_no: HashMap::new(),
            order_nos_by_seq_no: HashMap::new(),
            last_written_order_no: 0,
            last_sent_order_no: 0,
        }
    }

    pub(crate) fn add_message(&mut self, message: MessageData) {
        self.last_written_order_no += 1;
        self.order_nos_by_seq_no
            .insert(message.seq_no, self.last_written_order_no);
        self.messages_by_order_no
            .insert(self.last_written_order_no, message);
    }

    pub(crate) fn get_messages_to_be_sent(&mut self) -> Vec<MessageData> {
        let length: usize = self.last_written_order_no as usize - self.last_sent_order_no as usize;
        let mut messages = Vec::with_capacity(length);
        while self.last_sent_order_no != self.last_written_order_no {
            self.last_sent_order_no += 1;

            let Some(message) = self.messages_by_order_no.get(&self.last_sent_order_no) else {
                continue;
            };

            messages.push(message.clone());
        }
        messages
    }

    pub(crate) fn reset_progress(&mut self) {
        let Some(min_order_no) = self.order_nos_by_seq_no.values().min() else {
            return;
        };

        self.last_written_order_no = *min_order_no - 1;
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
}
