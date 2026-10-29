//! Reusable `Handler` implementations for mock-server metric tests.
//!
//! Each handler intercepts `ExecuteQuery` streams and replies with scripted
//! parts; every other message falls through to the service default replies.

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, Reply};
use crate::mock_server::query::{QUERY_TX_ID, QueryIncoming, QueryReply};

use super::parts::{row_with_value, success_part};

/// Replies to `ExecuteQuery` with an empty success part carrying the shared
/// test transaction id, then closes the stream. Drives transaction
/// exec/commit/rollback counters without result rows.
pub struct ExecCountingHandler {
    replies: FromHandlerToService,
}

impl ExecCountingHandler {
    pub fn new(replies: FromHandlerToService) -> Self {
        Self { replies }
    }
}

impl Handler for ExecCountingHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies = tx;
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        let Incoming::Query(QueryIncoming::ExecuteQuery(_, stream_id)) = incoming else {
            return Some(incoming);
        };
        self.replies
            .send(Reply::Query(QueryReply::ExecuteQuery {
                stream_id,
                part: success_part(Some(QUERY_TX_ID)),
            }))
            .expect("mock response channel must remain open");
        self.replies
            .send(Reply::Query(QueryReply::ExecuteQueryClose { stream_id }))
            .expect("mock response channel must remain open");
        None
    }
}

/// Replies to `ExecuteQuery` with one row (`val=42`) and closes the stream.
pub struct QueryRowHandler {
    replies: FromHandlerToService,
}

impl QueryRowHandler {
    pub fn new(replies: FromHandlerToService) -> Self {
        Self { replies }
    }
}

impl Handler for QueryRowHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies = tx;
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        let Incoming::Query(QueryIncoming::ExecuteQuery(_, stream_id)) = incoming else {
            return Some(incoming);
        };
        self.replies
            .send(Reply::Query(QueryReply::ExecuteQuery {
                stream_id,
                part: row_with_value(42),
            }))
            .expect("mock response channel must remain open");
        self.replies
            .send(Reply::Query(QueryReply::ExecuteQueryClose { stream_id }))
            .expect("mock response channel must remain open");
        None
    }
}

/// Transaction `query_row` sequence: an empty success part with the shared test
/// transaction id, then one row, then stream close.
pub struct TxQueryRowHandler {
    replies: FromHandlerToService,
}

impl TxQueryRowHandler {
    pub fn new(replies: FromHandlerToService) -> Self {
        Self { replies }
    }
}

impl Handler for TxQueryRowHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies = tx;
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        let Incoming::Query(QueryIncoming::ExecuteQuery(_, stream_id)) = incoming else {
            return Some(incoming);
        };
        for part in [success_part(Some(QUERY_TX_ID)), row_with_value(42)] {
            self.replies
                .send(Reply::Query(QueryReply::ExecuteQuery { stream_id, part }))
                .expect("mock response channel must remain open");
        }
        self.replies
            .send(Reply::Query(QueryReply::ExecuteQueryClose { stream_id }))
            .expect("mock response channel must remain open");
        None
    }
}
