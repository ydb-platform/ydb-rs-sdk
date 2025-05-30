use tokio_util::sync::CancellationToken as TokioCancellationToken;

#[derive(Clone, Debug)]
pub struct YdbCancellationToken {
    token: TokioCancellationToken,
}

impl YdbCancellationToken {
    pub(crate) fn new() -> Self {
        Self {
            token: TokioCancellationToken::new(),
        }
    }

    pub fn cancel(&self) {
        self.token.cancel()
    }

    pub(crate) fn to_tokio_token(&self) -> TokioCancellationToken {
        self.token.clone()
    }
}
