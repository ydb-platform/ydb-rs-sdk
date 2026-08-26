use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures_util::Stream;

use crate::errors::YdbResult;
use crate::grpc_wrapper::raw_query_service::stream::RawQueryResultPart;

/// Routes consecutive raw parts into logical result sets without owning transport lifecycle.
pub(super) struct ResultSetCursor<S> {
    source: S,
    pending_result: Option<RawQueryResultPart>,
    // This marker stays set across cancellation points so the next result-set request can finish
    // discarding a partially consumed logical set before exposing the following one.
    active_result_set: Option<i64>,
}

impl<S> ResultSetCursor<S> {
    pub(super) fn new(source: S) -> Self {
        Self {
            source,
            pending_result: None,
            active_result_set: None,
        }
    }

    pub(super) fn source(&self) -> &S {
        &self.source
    }

    pub(super) fn source_mut(&mut self) -> &mut S {
        &mut self.source
    }

    pub(super) fn clear_active_result_set(&mut self) {
        self.active_result_set = None;
    }
}

impl<S> ResultSetCursor<S>
where
    S: Stream<Item = YdbResult<RawQueryResultPart>> + Unpin,
{
    fn poll_next_raw(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<YdbResult<RawQueryResultPart>>> {
        if let Some(part) = self.pending_result.take() {
            return Poll::Ready(Some(Ok(part)));
        }
        Pin::new(&mut self.source).poll_next(cx)
    }

    pub(super) async fn next_raw(&mut self) -> YdbResult<Option<RawQueryResultPart>> {
        std::future::poll_fn(|cx| self.poll_next_raw(cx))
            .await
            .transpose()
    }

    async fn discard_active_result_set(&mut self) -> YdbResult<()> {
        let Some(active_index) = self.active_result_set else {
            return Ok(());
        };

        while let Some(part) = self.next_raw().await? {
            if part.result_set_index != active_index {
                self.pending_result = Some(part);
                break;
            }
        }
        self.active_result_set = None;
        Ok(())
    }

    pub(super) async fn next_result_set_index(&mut self) -> YdbResult<Option<i64>> {
        self.discard_active_result_set().await?;
        let Some(part) = self.next_raw().await? else {
            return Ok(None);
        };
        let result_set_index = part.result_set_index;
        self.pending_result = Some(part);
        self.active_result_set = Some(result_set_index);
        Ok(Some(result_set_index))
    }

    pub(super) fn poll_next_result_set_part(
        &mut self,
        result_set_index: i64,
        cx: &mut Context<'_>,
    ) -> Poll<Option<YdbResult<RawQueryResultPart>>> {
        match ready!(self.poll_next_raw(cx)) {
            Some(Ok(part)) if part.result_set_index == result_set_index => {
                Poll::Ready(Some(Ok(part)))
            }
            Some(Ok(part)) => {
                self.pending_result = Some(part);
                self.active_result_set = None;
                Poll::Ready(None)
            }
            Some(Err(err)) => {
                self.active_result_set = None;
                Poll::Ready(Some(Err(err)))
            }
            None => {
                self.active_result_set = None;
                Poll::Ready(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::{FutureExt, stream};

    use crate::errors::YdbResult;
    use crate::grpc_wrapper::raw_query_service::stream::RawQueryResultPart;
    use crate::grpc_wrapper::raw_table_service::value::RawResultSet;

    use super::ResultSetCursor;

    fn raw_part(result_set_index: i64) -> RawQueryResultPart {
        RawQueryResultPart {
            result_set_index,
            result_set: RawResultSet::default(),
        }
    }

    async fn next_group_part<S>(
        cursor: &mut ResultSetCursor<S>,
        result_set_index: i64,
    ) -> YdbResult<Option<RawQueryResultPart>>
    where
        S: futures_util::Stream<Item = YdbResult<RawQueryResultPart>> + Unpin,
    {
        std::future::poll_fn(|cx| cursor.poll_next_result_set_part(result_set_index, cx))
            .await
            .transpose()
    }

    #[tokio::test]
    async fn groups_consecutive_parts_without_transport() -> YdbResult<()> {
        let source =
            stream::iter([0, 0, 1, 1].map(|result_set_index| Ok(raw_part(result_set_index))));
        let mut cursor = ResultSetCursor::new(source);

        assert_eq!(cursor.next_result_set_index().await?, Some(0));
        assert_eq!(
            next_group_part(&mut cursor, 0)
                .await?
                .expect("first part")
                .result_set_index,
            0
        );
        assert_eq!(
            next_group_part(&mut cursor, 0)
                .await?
                .expect("first continuation")
                .result_set_index,
            0
        );
        assert!(next_group_part(&mut cursor, 0).await?.is_none());

        assert_eq!(cursor.next_result_set_index().await?, Some(1));
        assert_eq!(
            next_group_part(&mut cursor, 1)
                .await?
                .expect("second part")
                .result_set_index,
            1
        );
        assert_eq!(
            next_group_part(&mut cursor, 1)
                .await?
                .expect("second continuation")
                .result_set_index,
            1
        );
        assert!(next_group_part(&mut cursor, 1).await?.is_none());
        assert_eq!(cursor.next_result_set_index().await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn preserves_its_group_when_a_drain_is_cancelled() -> YdbResult<()> {
        let (parts_tx, mut parts_rx) = tokio::sync::mpsc::unbounded_channel();
        parts_tx
            .send(Ok(raw_part(0)))
            .expect("send first result part");
        let source = stream::poll_fn(move |cx| parts_rx.poll_recv(cx));
        let mut cursor = ResultSetCursor::new(source);

        assert_eq!(cursor.next_result_set_index().await?, Some(0));
        assert!(cursor.next_result_set_index().now_or_never().is_none());

        parts_tx
            .send(Ok(raw_part(0)))
            .expect("send first-set continuation");
        parts_tx
            .send(Ok(raw_part(1)))
            .expect("send second result set");
        drop(parts_tx);

        assert_eq!(cursor.next_result_set_index().await?, Some(1));
        Ok(())
    }
}
