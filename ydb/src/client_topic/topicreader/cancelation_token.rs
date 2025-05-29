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

    fn from_tokio_token(token: TokioCancellationToken) -> Self {
        Self { token }
    }

    pub fn cancel(&self) {
        self.token.cancel()
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub(crate) fn to_tokio_token(&self) -> TokioCancellationToken {
        self.token.clone()
    }
}
