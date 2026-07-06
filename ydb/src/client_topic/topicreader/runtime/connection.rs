use tokio::sync::mpsc;

use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawFromClientOneOf;
use crate::{YdbError, YdbResult};

pub(crate) struct Connection {
    outgoing_tx: mpsc::UnboundedSender<RawFromClientOneOf>,
    epoch: usize,
}

impl Connection {
    pub(crate) fn new(
        outgoing_tx: mpsc::UnboundedSender<RawFromClientOneOf>,
        epoch: usize,
    ) -> Self {
        Self { outgoing_tx, epoch }
    }

    pub(crate) fn epoch(&self) -> usize {
        self.epoch
    }

    pub(crate) fn send(&self, message: RawFromClientOneOf) -> YdbResult<()> {
        self.outgoing_tx
            .send(message)
            .map_err(|err| YdbError::Transport(format!("topic reader stream send failed: {err}")))
    }
}
