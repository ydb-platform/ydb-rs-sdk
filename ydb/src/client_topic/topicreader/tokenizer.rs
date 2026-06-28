use std::convert::Infallible;
use std::time::Duration;

use secrecy::ExposeSecret;
use tokio::select;
use tokio::sync::mpsc;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::client_common::TokenCache;
use crate::grpc_wrapper::raw_topic_service::common::update_token::RawUpdateTokenRequest;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawFromClientOneOf;
use crate::{YdbError, YdbResult};

use super::reconnector;

const UPDATE_TOKEN_INTERVAL: Duration = Duration::from_secs(3600);

pub(super) struct Tokenizer {
    token_cache: TokenCache,
    outgoing_tx: mpsc::UnboundedSender<RawFromClientOneOf>,
    cancellation: CancellationToken,
}

impl Tokenizer {
    pub(super) fn new(
        ctx: &reconnector::Context,
        outgoing_tx: mpsc::UnboundedSender<RawFromClientOneOf>,
    ) -> Self {
        Self {
            token_cache: ctx.token_cache.clone(),
            outgoing_tx,
            cancellation: ctx.cancellation.clone(),
        }
    }

    pub(super) async fn run(self) -> YdbResult<()> {
        let Self {
            token_cache,
            outgoing_tx,
            cancellation,
        } = self;

        select! {
            _ = cancellation.cancelled() => {
                debug!("topic reader tokenizer cancelled, stopping");
                Ok(())
            }
            result = update_tokens(token_cache, outgoing_tx) => {
                let Err(e) = result;
                Err(e)
            }
        }
    }
}

async fn update_tokens(
    token_cache: TokenCache,
    outgoing_tx: mpsc::UnboundedSender<RawFromClientOneOf>,
) -> YdbResult<Infallible> {
    let mut interval = time::interval(UPDATE_TOKEN_INTERVAL);
    interval.tick().await;

    loop {
        interval.tick().await;
        let token = token_cache.token();
        let request = RawFromClientOneOf::UpdateTokenRequest(RawUpdateTokenRequest {
            token: token.expose_secret().to_string(),
        });
        outgoing_tx.send(request).map_err(|e| {
            YdbError::Transport(format!(
                "topic reader tokenizer outgoing channel closed: {e}"
            ))
        })?;
    }
}
