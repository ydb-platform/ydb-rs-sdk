use std::sync::Arc;

pub(crate) mod names;

/// A metrics backend accepted by [`crate::ClientBuilder::with_metrics_recorder`].
///
/// Wraps any [`metrics::Recorder`] — e.g. a `metrics_prometheus::Recorder` backed by a
/// caller-owned `prometheus::Registry` — so SDK metrics are recorded into non-global,
/// per-caller storage instead of the process-global recorder.
#[derive(Clone)]
pub struct MetricsRecorder {
    inner: Arc<dyn metrics::Recorder + Send + Sync>,
}

impl MetricsRecorder {
    /// Wrap a [`metrics::Recorder`] implementation.
    pub fn new<R>(recorder: R) -> Self
    where
        R: metrics::Recorder + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(recorder),
        }
    }

    /// Borrow the wrapped recorder for handle registration.
    pub(crate) fn as_dyn(&self) -> &dyn metrics::Recorder {
        self.inner.as_ref()
    }
}
