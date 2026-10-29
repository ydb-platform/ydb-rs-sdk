//! Per-recorder handle caches for dynamic metric series.
//!
//! Series whose label values are runtime strings (gRPC transport metrics)
//! cannot pre-register handles at construction time. Their label strings are
//! interned to `usize` ids ([`super::interning`]) and handles are cached per
//! series key: the hot path is a read lock plus a lookup, with no allocation
//! and no `metrics::Key` construction. Handles are built — and registered into
//! the backend, with the recorder's static labels prepended — only on the
//! first emission of each distinct label combination.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::RwLock;

use crate::grpc_wrapper::metrics_interceptor::GrpcCode;

use super::backend::{CounterHandle, GaugeHandle, HistogramHandle};
use super::interning;

/// Cache key component for the `grpc_code` label: the closed gRPC status set
/// as a discriminant, with an interned fallback for values outside it.
///
/// The SDK itself only passes codes from [`GrpcCode::as_label`], so the common
/// path never interns; the fallback keeps arbitrary caller-supplied codes
/// correct.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum GrpcCodeKey {
    Code(GrpcCode),
    Interned(usize),
}

impl GrpcCodeKey {
    pub(crate) fn from_label(label: &str) -> Self {
        match GrpcCode::from_label(label) {
            Some(code) => Self::Code(code),
            None => Self::Interned(interning::intern(label)),
        }
    }

    pub(crate) fn is_ok(self) -> bool {
        self == Self::Code(GrpcCode::Ok)
    }

    pub(crate) fn label_value(self) -> String {
        match self {
            Self::Code(code) => code.as_label().to_string(),
            Self::Interned(id) => interning::resolve(id),
        }
    }
}

// One key type per series, exactly mirroring its label set. Series with
// different label sets must not share a key: `ydb_grpc_request_duration_seconds`
// has no `grpc_code` label, and a shared four-part key would re-record each
// observation through both handles.
pub(crate) type GrpcRequestsKey = (usize, usize, usize, GrpcCodeKey);
pub(crate) type GrpcRequestDurationKey = (usize, usize, usize);
pub(crate) type GrpcErrorsKey = (usize, GrpcCodeKey);
pub(crate) type GrpcStreamMessagesKey = (usize, usize, usize, usize);
pub(crate) type GrpcConnectionsKey = (usize, usize);
pub(crate) type GrpcConnectionEstablishKey = (usize, bool);
pub(crate) type QueryErrorsKey = (usize, usize);
pub(crate) type QueryTransactionRetriesKey = usize;
pub(crate) type QueryNamedOperationsKey = (usize, usize);
pub(crate) type QueryNamedErrorsKey = (usize, usize);

/// Handle caches for the dynamic series (runtime string labels), stored inside
/// [`MetricsNames`](super::names::MetricsNames) (so handles stay bound to its
/// recorder's backend).
#[derive(Default)]
pub(crate) struct DynamicCaches {
    pub(crate) grpc_requests: RwLock<HashMap<GrpcRequestsKey, CounterHandle>>,
    pub(crate) grpc_request_duration: RwLock<HashMap<GrpcRequestDurationKey, HistogramHandle>>,
    pub(crate) grpc_errors: RwLock<HashMap<GrpcErrorsKey, CounterHandle>>,
    pub(crate) grpc_stream_messages: RwLock<HashMap<GrpcStreamMessagesKey, CounterHandle>>,
    pub(crate) grpc_connections: RwLock<HashMap<GrpcConnectionsKey, GaugeHandle>>,
    pub(crate) grpc_connection_establish:
        RwLock<HashMap<GrpcConnectionEstablishKey, HistogramHandle>>,
    /// `ydb_query_errors_total` keyed by (operation index, interned status code).
    pub(crate) query_errors: RwLock<HashMap<QueryErrorsKey, CounterHandle>>,
    /// `ydb_query_transaction_retries_total` keyed by interned status code.
    pub(crate) query_tx_retries: RwLock<HashMap<QueryTransactionRetriesKey, CounterHandle>>,
    /// `ydb_query_named_operations_total` keyed by (canonical interned query
    /// name, result index).
    pub(crate) query_named_operations: RwLock<HashMap<QueryNamedOperationsKey, CounterHandle>>,
    /// `ydb_query_named_operation_duration_seconds` keyed by canonical
    /// interned query name.
    pub(crate) query_named_operation_duration: RwLock<HashMap<usize, HistogramHandle>>,
    /// `ydb_query_named_errors_total` keyed by (canonical interned query name,
    /// interned status code).
    pub(crate) query_named_errors: RwLock<HashMap<QueryNamedErrorsKey, CounterHandle>>,
    /// `ydb_query_named_result_rows` keyed by canonical interned query name.
    pub(crate) query_named_result_rows: RwLock<HashMap<usize, HistogramHandle>>,
    /// `ydb_query_named_result_bytes` keyed by canonical interned query name.
    pub(crate) query_named_result_bytes: RwLock<HashMap<usize, HistogramHandle>>,
}

/// Cached handle lookup: a read lock on hit; on miss, build the handle under
/// the write lock with a double-check, because another thread may have
/// inserted the same series while this thread waited.
pub(crate) fn cached<K, H>(cache: &RwLock<HashMap<K, H>>, key: K, build: impl FnOnce() -> H) -> H
where
    K: Copy + Eq + Hash,
    H: Clone,
{
    if let Some(hit) = cache
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&key)
    {
        return hit.clone();
    }
    let mut map = cache
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(hit) = map.get(&key) {
        return hit.clone();
    }
    let handle = build();
    map.insert(key, handle.clone());
    handle
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grpc_code_key_uses_discriminant_for_known_labels() {
        assert_eq!(
            GrpcCodeKey::from_label("ok"),
            GrpcCodeKey::Code(GrpcCode::Ok)
        );
        assert_eq!(
            GrpcCodeKey::from_label("unavailable"),
            GrpcCodeKey::Code(GrpcCode::Unavailable)
        );
        assert!(GrpcCodeKey::from_label("ok").is_ok());
        assert!(!GrpcCodeKey::from_label("unavailable").is_ok());
    }

    #[test]
    fn grpc_code_key_interns_unknown_labels_round_trip() {
        let key = GrpcCodeKey::from_label("exotic-code");
        assert_eq!(
            key,
            GrpcCodeKey::Interned(interning::intern("exotic-code")),
            "unknown codes must be interned, not remapped"
        );
        assert_eq!(key.label_value(), "exotic-code");
        assert!(!key.is_ok());
    }

    #[test]
    fn cached_registers_once_per_key() {
        let cache = RwLock::new(HashMap::<usize, CounterHandle>::new());
        let registrations = std::sync::atomic::AtomicUsize::new(0);

        let handles: Vec<_> = (0..5)
            .map(|_| {
                cached(&cache, 7, || {
                    registrations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    crate::client_metrics::backend::noop_counter()
                })
            })
            .collect();

        assert_eq!(
            registrations.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "builder must run exactly once for one key"
        );
        assert_eq!(cache.read().unwrap().len(), 1);
        assert_eq!(handles.len(), 5);
    }
}
