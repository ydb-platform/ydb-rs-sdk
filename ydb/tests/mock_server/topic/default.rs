use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, Reply};

use super::builders::{init_response, start_partition_session_request, update_token_response};
use super::handler::{TopicIncoming, TopicReply};
use tracing::error;
use ydb_grpc::ydb_proto::topic::stream_read_message::from_client::ClientMessage as ReadFromClient;

const SESSION_ID: &str = "mock-session";
const PARTITION_SESSION_ID: i64 = 1;
const PARTITION_ID: i64 = 0;
const COMMITTED_OFFSET: i64 = 0;

pub struct TopicDefaultHandler {
    tx: FromHandlerToService,
}

impl TopicDefaultHandler {
    pub fn with_tx(tx: FromHandlerToService) -> Self {
        Self { tx }
    }
}

impl Handler for TopicDefaultHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.tx = tx;
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        let Incoming::Topic(incoming) = incoming else {
            error!(?incoming, "topic handler got non-topic message");

            return Some(incoming);
        };

        let replies = default_replies(incoming);

        for reply in replies {
            self.tx
                .send(Reply::Topic(reply))
                .expect("topic service closed channel");
        }

        None
    }
}

pub fn default_replies(msg: TopicIncoming) -> Vec<TopicReply> {
    match msg {
        TopicIncoming::StreamRead {
            stream_id,
            msg: ReadFromClient::InitRequest(init),
        } => {
            let topic_path = init
                .topics_read_settings
                .into_iter()
                .next()
                .map(|t| t.path)
                .unwrap_or_default();
            vec![
                init_response(stream_id, SESSION_ID),
                start_partition_session_request(
                    stream_id,
                    PARTITION_SESSION_ID,
                    topic_path,
                    PARTITION_ID,
                    COMMITTED_OFFSET,
                ),
            ]
        }

        TopicIncoming::StreamRead {
            stream_id,
            msg: ReadFromClient::UpdateTokenRequest(_),
        } => vec![update_token_response(stream_id)],

        _ => vec![],
    }
}
