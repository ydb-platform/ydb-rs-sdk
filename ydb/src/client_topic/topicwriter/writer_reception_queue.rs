use crate::client_topic::topicwriter::message_write_status::MessageWriteStatus;
use crate::grpc_wrapper::raw_errors::{RawError, RawResult};

use std::collections::VecDeque;

pub(crate) enum TopicWriterReceptionType {
    AwaitingConfirmation(tokio::sync::oneshot::Sender<MessageWriteStatus>),
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
            // drop is workaround for old rust: destructive assigment was unstable until 1.59
            // E0658
            drop(sender.send(write_status));
        }
    }
}

pub(crate) struct TopicWriterReceptionQueue {
    message_receipt_signals_queue: VecDeque<TopicWriterReceptionTicket>,

    flush_finished_sender: Option<tokio::sync::oneshot::Sender<()>>,
}

impl TopicWriterReceptionQueue {
    pub fn new() -> Self {
        Self {
            message_receipt_signals_queue: VecDeque::new(),
            flush_finished_sender: None,
        }
    }

    pub(crate) fn init_flush_op(&mut self) -> RawResult<tokio::sync::oneshot::Receiver<()>> {
        let (tx, rx): (
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        ) = tokio::sync::oneshot::channel();
        if self.message_receipt_signals_queue.is_empty() {
            tx.send(()).unwrap();
            return Ok(rx);
        }
        match self.message_receipt_signals_queue.back_mut() {
            None => Err(RawError::Custom(
                "Programming error, should not be happening".to_string(),
            )),
            Some(ticket) => {
                ticket.enable_flush_flag();
                self.flush_finished_sender = Some(tx);
                Ok(rx)
            }
        }
    }

    pub fn try_get_ticket(&mut self) -> Option<TopicWriterReceptionTicket> {
        let maybe_ticket = self.message_receipt_signals_queue.pop_front();
        match maybe_ticket.as_ref() {
            None => {
                if self.flush_finished_sender.is_some() {
                    let sender = std::mem::take(&mut self.flush_finished_sender);
                    sender.unwrap().send(()).unwrap();
                }
            }
            Some(ticket) => {
                if ticket.get_flush_flag() {
                    println!("Confirmation sent");
                    let sender = std::mem::take(&mut self.flush_finished_sender);
                    sender.unwrap().send(()).unwrap();
                }
            }
        }
        maybe_ticket
    }

    pub fn add_ticket(&mut self, reception_ticket: TopicWriterReceptionTicket) {
        self.message_receipt_signals_queue
            .push_back(reception_ticket);
    }
}
