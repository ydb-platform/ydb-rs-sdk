use crate::client_topic::topicwriter::message_write_status::MessageWriteStatus;
use crate::{YdbError, YdbResult};

use std::collections::VecDeque;

pub(crate) enum TopicWriterReceptionType {
    AwaitingConfirmation(tokio::sync::oneshot::Sender<YdbResult<MessageWriteStatus>>),
    NoConfirmationExpected,
}

pub(crate) struct TopicWriterReceptionTicket {
    seq_no: i64,
    reception_type: TopicWriterReceptionType,
    flush_flag: bool,
}

impl TopicWriterReceptionTicket {
    pub fn new(seq_no: i64, reception_type: TopicWriterReceptionType) -> Self {
        Self {
            seq_no,
            reception_type,
            flush_flag: false,
        }
    }

    pub fn get_flush_flag(&self) -> bool {
        self.flush_flag
    }

    pub fn enable_flush_flag(&mut self) {
        self.flush_flag = true;
    }

    pub fn get_seq_no(&self) -> i64 {
        self.seq_no
    }

    pub fn send_confirmation_if_needed(self, write_status: MessageWriteStatus) {
        if let TopicWriterReceptionType::AwaitingConfirmation(sender) = self.reception_type {
            // drop is workaround for old rust: destructive assignment was unstable until 1.59
            // E0658
            drop(sender.send(Ok(write_status)));
        }
    }

    pub fn send_error_if_needed(self, error: YdbError) {
        if let TopicWriterReceptionType::AwaitingConfirmation(sender) = self.reception_type {
            drop(sender.send(Err(error)));
        }
    }
}

pub(crate) struct TopicWriterReceptionQueue {
    message_receipt_signals_queue: VecDeque<TopicWriterReceptionTicket>,

    flush_finished_sender: Option<tokio::sync::oneshot::Sender<()>>,
}

impl TopicWriterReceptionQueue {
    pub(crate) fn new() -> Self {
        Self {
            message_receipt_signals_queue: VecDeque::new(),
            flush_finished_sender: None,
        }
    }

    pub(crate) fn init_flush_op(&mut self) -> YdbResult<tokio::sync::oneshot::Receiver<()>> {
        let (tx, rx): (
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        ) = tokio::sync::oneshot::channel();
        if self.message_receipt_signals_queue.is_empty() {
            tx.send(())
                .map_err(|_| YdbError::custom("init_flush_op: channel unexpectedly closed"))?;
            return Ok(rx);
        }
        match self.message_receipt_signals_queue.back_mut() {
            None => Err(YdbError::custom(
                "init_flush_op: programming error, should not be happening",
            )),
            Some(ticket) => {
                ticket.enable_flush_flag();
                self.flush_finished_sender = Some(tx);
                Ok(rx)
            }
        }
    }

    pub(crate) fn peek_ticket_seq_no(&self) -> Option<i64> {
        self.message_receipt_signals_queue
            .front()
            .map(TopicWriterReceptionTicket::get_seq_no)
    }

    pub(crate) fn try_get_ticket(&mut self) -> YdbResult<Option<TopicWriterReceptionTicket>> {
        let maybe_ticket = self.message_receipt_signals_queue.pop_front();
        match maybe_ticket.as_ref() {
            None => self.send_flush_finished()?,
            Some(ticket) => {
                if ticket.get_flush_flag() {
                    self.send_flush_finished()?;
                }
            }
        }
        Ok(maybe_ticket)
    }

    fn send_flush_finished(&mut self) -> YdbResult<()> {
        if let Some(sender) = self.flush_finished_sender.take() {
            sender
                .send(())
                .map_err(|_| YdbError::custom("send_flush_finished: channel is closed"))?;
        }
        Ok(())
    }

    pub(crate) fn add_ticket(&mut self, reception_ticket: TopicWriterReceptionTicket) {
        self.message_receipt_signals_queue
            .push_back(reception_ticket);
    }

    pub(crate) fn send_error_to_tickets_and_clear(&mut self, error: YdbError) {
        while let Some(ticket) = self.message_receipt_signals_queue.pop_front() {
            ticket.send_error_if_needed(error.clone());
        }
    }
}
