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

    pub(crate) fn seq_no(&self) -> i64 {
        self.seq_no
    }

    pub(crate) fn send_result_if_needed(self, write_result: YdbResult<MessageWriteStatus>) {
        if let Some(sender) = self.ack_sender {
            let _ = sender.send(write_result);
        }
    }

    pub(crate) fn send_error_if_needed(self, error: YdbError) {
        if let Some(sender) = self.ack_sender {
            let _ = sender.send(Err(error));
        }
    }
}

pub(crate) struct ReceptionQueue {
    ticket_queue: VecDeque<ReceptionTicket>,
    unobserved_ack_error: Option<YdbError>,

    // Pending flush() calls waiting for all tickets up to `threshold_seq_no` to be popped.
    // Multiple concurrent flush() calls are supported, each with its own sender.
    pending_flushes: Vec<PendingFlush>,
}

struct PendingFlush {
    threshold_seq_no: i64,
    notifier: oneshot::Sender<YdbResult<()>>,
    first_ack_error: Option<YdbError>,
}

impl ReceptionQueue {
    pub(crate) fn new() -> Self {
        Self {
            ticket_queue: VecDeque::new(),
            unobserved_ack_error: None,
            pending_flushes: Vec::new(),
        }
    }

    pub(crate) fn init_flush(&mut self) -> YdbResult<oneshot::Receiver<YdbResult<()>>> {
        let (tx, rx) = oneshot::channel();
        let first_ack_error = self.unobserved_ack_error.take();
        let Some(last_ticket) = self.ticket_queue.back() else {
            let result = first_ack_error.map_or(Ok(()), Err);
            tx.send(result)
                .map_err(|_| YdbError::custom("init_flush: channel unexpectedly closed"))?;
            return Ok(rx);
        };
        self.pending_flushes.push(PendingFlush {
            threshold_seq_no: last_ticket.seq_no(),
            notifier: tx,
            first_ack_error,
        });
        Ok(rx)
    }

    pub(crate) fn peek_ticket_seq_no(&self) -> Option<i64> {
        self.ticket_queue.front().map(ReceptionTicket::seq_no)
    }

    pub(crate) fn pop_ticket(&mut self) -> Option<ReceptionTicket> {
        self.ticket_queue.pop_front()
    }

    pub(crate) fn notify_ticket_processed(&mut self, seq_no: i64, error: Option<YdbError>) {
        let mut error_observed_by_pending_flush = false;
        let completed = self.pending_flushes.extract_if(.., |flush| {
            if let Some(error) = &error {
                if seq_no <= flush.threshold_seq_no {
                    error_observed_by_pending_flush = true;
                    flush.first_ack_error.get_or_insert_with(|| error.clone());
                }
            }
            flush.threshold_seq_no <= seq_no
        });

        for pending in completed {
            let _ = pending
                .notifier
                .send(pending.first_ack_error.map_or(Ok(()), Err));
        }

        if let Some(error) = error {
            if !error_observed_by_pending_flush && self.unobserved_ack_error.is_none() {
                self.unobserved_ack_error = Some(error);
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
        for pending in self.pending_flushes.drain(..) {
            let _ = pending.notifier.send(Err(error.clone()));
        }
    }
}
