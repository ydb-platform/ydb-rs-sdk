use std::collections::VecDeque;

use tokio::sync::oneshot;

use crate::client_topic::topicwriter::message_write_status::MessageWriteStatus;
use crate::{YdbError, YdbResult};

pub(crate) struct ReceptionTicket {
    seq_no: i64,
    ack_sender: Option<oneshot::Sender<YdbResult<MessageWriteStatus>>>,
    flush_flag: bool,
}

impl ReceptionTicket {
    pub(crate) fn new(
        seq_no: i64,
        ack_sender: Option<oneshot::Sender<YdbResult<MessageWriteStatus>>>,
    ) -> Self {
        Self {
            seq_no,
            ack_sender,
            flush_flag: false,
        }
    }

    pub(crate) fn get_flush_flag(&self) -> bool {
        self.flush_flag
    }

    pub(crate) fn enable_flush_flag(&mut self) {
        self.flush_flag = true;
    }

    pub(crate) fn get_seq_no(&self) -> i64 {
        self.seq_no
    }

    pub(crate) fn send_confirmation_if_needed(self, write_status: MessageWriteStatus) {
        if let Some(sender) = self.ack_sender {
            // drop is workaround for old rust: destructive assignment was unstable until 1.59
            // E0658
            drop(sender.send(Ok(write_status)));
        }
    }

    pub(crate) fn send_error_if_needed(self, error: YdbError) {
        if let Some(sender) = self.ack_sender {
            drop(sender.send(Err(error)));
        }
    }
}

pub(crate) struct ReceptionQueue {
    ticket_queue: VecDeque<ReceptionTicket>,

    flush_finished_sender: Option<oneshot::Sender<()>>,
}

impl ReceptionQueue {
    pub(crate) fn new() -> Self {
        Self {
            ticket_queue: VecDeque::new(),
            flush_finished_sender: None,
        }
    }

    pub(crate) fn init_flush(&mut self) -> YdbResult<oneshot::Receiver<()>> {
        let (tx, rx): (oneshot::Sender<()>, oneshot::Receiver<()>) = oneshot::channel();
        if self.ticket_queue.is_empty() {
            tx.send(())
                .map_err(|_| YdbError::custom("init_flush: channel unexpectedly closed"))?;
            return Ok(rx);
        }
        match self.ticket_queue.back_mut() {
            None => Err(YdbError::custom(
                "init_flush: programming error, should not be happening",
            )),
            Some(ticket) => {
                ticket.enable_flush_flag();
                self.flush_finished_sender = Some(tx);
                Ok(rx)
            }
        }
    }

    pub(crate) fn peek_ticket_seq_no(&self) -> Option<i64> {
        self.ticket_queue.front().map(ReceptionTicket::get_seq_no)
    }

    pub(crate) fn try_get_ticket(&mut self) -> YdbResult<Option<ReceptionTicket>> {
        let maybe_ticket = self.ticket_queue.pop_front();
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

    pub(crate) fn add_ticket(&mut self, reception_ticket: ReceptionTicket) {
        self.ticket_queue.push_back(reception_ticket);
    }

    pub(crate) fn send_error_to_tickets_and_clear(&mut self, error: YdbError) {
        while let Some(ticket) = self.ticket_queue.pop_front() {
            ticket.send_error_if_needed(error.clone());
        }
    }
}
