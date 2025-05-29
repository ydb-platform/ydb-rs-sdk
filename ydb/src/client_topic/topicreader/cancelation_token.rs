use tokio_util::sync::CancellationToken as TokioCancellationToken;

#[derive(Clone, Debug)]
pub struct YdbCancellationToken {
    token: TokioCancellationToken,
}

impl YdbCancellationToken {
    fn new(token: TokioCancellationToken) -> Self {
        Self { token }
    }
    
    fn cancel(&self) {
        self.token.cancel()
    }
    
    fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }
}

impl From<YdbCancellationToken> for TokioCancellationToken {
    fn from(token: YdbCancellationToken) -> Self {
        token.token
    }
}

