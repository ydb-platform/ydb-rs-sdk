use std::{num::NonZeroUsize, sync::Arc};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::{YdbError, YdbResult};

/// Maximum message payload size supported by byte-capacity accounting.
///
/// A Tokio semaphore can hold at most [`Semaphore::MAX_PERMITS`] permits, while
/// [`Semaphore::acquire_many_owned`] accepts a `u32` permit count. Use the smaller bound so the
/// hard message limit is valid on every pointer width.
const MAX_MESSAGE_SIZE_BYTES: usize = if Semaphore::MAX_PERMITS < u32::MAX as usize {
    Semaphore::MAX_PERMITS
} else {
    u32::MAX as usize
};

#[derive(Clone)]
pub(crate) struct CapacityLimiter {
    message_slots: Arc<Semaphore>,
    byte_slots: Arc<Semaphore>,
    max_inflight_bytes: NonZeroUsize,
}

pub(crate) struct CapacityPermit {
    _message: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

pub(crate) struct AdmittedMessage {
    message: TopicWriterMessage,
    capacity: CapacityPermit,
}

impl AdmittedMessage {
    pub(crate) fn message_mut(&mut self) -> &mut TopicWriterMessage {
        &mut self.message
    }

    pub(crate) fn into_parts(self) -> (TopicWriterMessage, CapacityPermit) {
        (self.message, self.capacity)
    }
}

impl CapacityLimiter {
    pub(crate) fn new(
        max_inflight_messages: NonZeroUsize,
        max_inflight_bytes: NonZeroUsize,
    ) -> YdbResult<Self> {
        if max_inflight_messages.get() > Semaphore::MAX_PERMITS {
            return Err(YdbError::custom(format!(
                "max_inflight_messages exceeds the supported limit: value={max_inflight_messages}, limit={}",
                Semaphore::MAX_PERMITS,
            )));
        }
        if max_inflight_bytes.get() > Semaphore::MAX_PERMITS {
            return Err(YdbError::custom(format!(
                "max_inflight_bytes exceeds the supported limit: value={max_inflight_bytes}, limit={}",
                Semaphore::MAX_PERMITS,
            )));
        }

        Ok(Self {
            message_slots: Arc::new(Semaphore::new(max_inflight_messages.get())),
            byte_slots: Arc::new(Semaphore::new(max_inflight_bytes.get())),
            max_inflight_bytes,
        })
    }

    pub(crate) async fn admit(&self, message: TopicWriterMessage) -> YdbResult<AdmittedMessage> {
        let charged_bytes = self.charged_bytes(message.data.len())?;
        let message_capacity = self
            .message_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| YdbError::custom("message queue is closed for new messages"))?;

        // A message larger than the entire buffer occupies all byte slots. This lets one
        // oversized message make progress while still applying backpressure to later writes.
        let byte_capacity = self
            .byte_slots
            .clone()
            .acquire_many_owned(charged_bytes)
            .await
            .map_err(|_| YdbError::custom("message queue is closed for new messages"))?;

        Ok(AdmittedMessage {
            message,
            capacity: CapacityPermit {
                _message: message_capacity,
                _bytes: byte_capacity,
            },
        })
    }

    #[cfg(test)]
    fn try_admit(&self, message: TopicWriterMessage) -> YdbResult<AdmittedMessage> {
        let capacity = self.try_acquire(message.data.len())?;
        Ok(AdmittedMessage { message, capacity })
    }

    #[cfg(test)]
    pub(crate) fn try_acquire(&self, message_size: usize) -> YdbResult<CapacityPermit> {
        let charged_bytes = self.charged_bytes(message_size)?;
        let message = self
            .message_slots
            .clone()
            .try_acquire_owned()
            .map_err(|err| {
                YdbError::custom(format!("failed to acquire message capacity: {err}"))
            })?;
        let bytes = self
            .byte_slots
            .clone()
            .try_acquire_many_owned(charged_bytes)
            .map_err(|err| YdbError::custom(format!("failed to acquire byte capacity: {err}")))?;

        Ok(CapacityPermit {
            _message: message,
            _bytes: bytes,
        })
    }

    fn charged_bytes(&self, message_size: usize) -> YdbResult<u32> {
        if message_size > MAX_MESSAGE_SIZE_BYTES {
            return Err(YdbError::custom(format!(
                "message payload exceeds the supported size: size={message_size}, limit={MAX_MESSAGE_SIZE_BYTES}",
            )));
        }

        Ok(message_size.min(self.max_inflight_bytes.get()) as u32)
    }

    pub(crate) fn close(&self) {
        self.message_slots.close();
        self.byte_slots.close();
    }
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt;

    use super::*;

    fn message(size: usize) -> TopicWriterMessage {
        TopicWriterMessage::builder().data(vec![0; size]).build()
    }

    fn nonzero(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn allows_inflight_capacity_above_single_message_limit() {
        let capacity = MAX_MESSAGE_SIZE_BYTES + 1;
        assert!(CapacityLimiter::new(nonzero(1), nonzero(capacity)).is_ok());
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn rejects_message_above_single_message_limit() {
        let capacity = MAX_MESSAGE_SIZE_BYTES + 1;
        let limiter = CapacityLimiter::new(nonzero(1), nonzero(capacity)).unwrap();

        assert!(limiter.try_acquire(capacity).is_err());
    }

    #[tokio::test]
    async fn waits_for_message_capacity() {
        let limiter = CapacityLimiter::new(nonzero(1), nonzero(2)).unwrap();
        let permit = limiter.admit(message(1)).await.unwrap();

        let mut waiting = Box::pin(limiter.admit(message(1)));
        assert!(waiting.as_mut().now_or_never().is_none());

        drop(permit);
        waiting.await.unwrap();
    }

    #[tokio::test]
    async fn waits_for_byte_capacity() {
        let limiter = CapacityLimiter::new(nonzero(2), nonzero(1)).unwrap();
        let permit = limiter.admit(message(1)).await.unwrap();

        let mut waiting = Box::pin(limiter.admit(message(1)));
        assert!(waiting.as_mut().now_or_never().is_none());

        drop(permit);
        waiting.await.unwrap();
    }

    #[tokio::test]
    async fn oversized_message_occupies_all_byte_capacity() {
        let limiter = CapacityLimiter::new(nonzero(2), nonzero(5)).unwrap();
        let permit = limiter.admit(message(10)).await.unwrap();

        let mut waiting = Box::pin(limiter.admit(message(1)));
        assert!(waiting.as_mut().now_or_never().is_none());

        drop(permit);
        waiting.await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_waiter_releases_message_capacity() {
        let limiter = CapacityLimiter::new(nonzero(2), nonzero(1)).unwrap();
        let byte_capacity = limiter.admit(message(1)).await.unwrap();

        let mut waiting = Box::pin(limiter.admit(message(1)));
        assert!(waiting.as_mut().now_or_never().is_none());
        drop(waiting);

        let permit = limiter.try_admit(message(0)).unwrap();
        drop((permit, byte_capacity));
    }

    #[tokio::test]
    async fn close_wakes_waiters() {
        let limiter = CapacityLimiter::new(nonzero(1), nonzero(1)).unwrap();
        let _permit = limiter.admit(message(1)).await.unwrap();
        let waiting = limiter.admit(message(1));

        limiter.close();

        assert!(waiting.await.is_err());
    }
}
