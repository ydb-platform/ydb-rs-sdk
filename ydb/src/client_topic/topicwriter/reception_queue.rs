use std::collections::VecDeque;

use tokio::sync::oneshot;

use crate::client_topic::topicwriter::message_write_status::MessageWriteStatus;
use crate::{YdbError, YdbResult};

pub(crate) struct ReceptionTicket {
    seq_no: i64,
    ack_sender: Option<oneshot::Sender<YdbResult<MessageWriteStatus>>>,
}

impl ReceptionTicket {
    pub(crate) fn new(
        seq_no: i64,
        ack_sender: Option<oneshot::Sender<YdbResult<MessageWriteStatus>>>,
    ) -> Self {
        Self { seq_no, ack_sender }
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

    // Pending flush() calls waiting for all tickets up to `threshold_seq_no` to be popped.
    // Multiple concurrent flush() calls are supported, each with its own sender.
    pending_flushes: Vec<PendingFlush>,
}

struct PendingFlush {
    threshold_seq_no: i64,
    notifier: oneshot::Sender<()>,
}

impl ReceptionQueue {
    pub(crate) fn new() -> Self {
        Self {
            ticket_queue: VecDeque::new(),
            pending_flushes: Vec::new(),
        }
    }

    pub(crate) fn init_flush(&mut self) -> YdbResult<oneshot::Receiver<()>> {
        let (tx, rx): (oneshot::Sender<()>, oneshot::Receiver<()>) = oneshot::channel();
        let Some(last_ticket) = self.ticket_queue.back() else {
            tx.send(())
                .map_err(|_| YdbError::custom("init_flush: channel unexpectedly closed"))?;
            return Ok(rx);
        };
        self.pending_flushes.push(PendingFlush {
            threshold_seq_no: last_ticket.get_seq_no(),
            notifier: tx,
        });
        Ok(rx)
    }

    pub(crate) fn peek_ticket_seq_no(&self) -> Option<i64> {
        self.ticket_queue.front().map(ReceptionTicket::get_seq_no)
    }

    pub(crate) fn try_get_ticket(&mut self) -> YdbResult<Option<ReceptionTicket>> {
        let maybe_ticket = self.ticket_queue.pop_front();
        if let Some(ticket) = maybe_ticket.as_ref() {
            self.notify_flushes_up_to(ticket.get_seq_no());
        } else {
            // Queue is empty: every pending flush is satisfied.
            self.notify_flushes_up_to(i64::MAX);
        }
        Ok(maybe_ticket)
    }

    fn notify_flushes_up_to(&mut self, acked_seq_no: i64) {
        let mut i = 0;
        while i < self.pending_flushes.len() {
            if self.pending_flushes[i].threshold_seq_no <= acked_seq_no {
                let pending = self.pending_flushes.swap_remove(i);
                let _ = pending.notifier.send(());
            } else {
                i += 1;
            }
        }
    }

    pub(crate) fn add_ticket(&mut self, reception_ticket: ReceptionTicket) {
        self.ticket_queue.push_back(reception_ticket);
    }

    pub(crate) fn send_error_to_tickets_and_clear(&mut self, error: YdbError) {
        while let Some(ticket) = self.ticket_queue.pop_front() {
            ticket.send_error_if_needed(error.clone());
        }
        // Drop pending flush notifiers: receivers will observe RecvError and translate it
        // to a flush failure on the calling side.
        self.pending_flushes.clear();
    }
}
