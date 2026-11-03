//! Canonicalization of caller-supplied query names into `query_name` label values.
//!
//! Names outside `[a-z0-9_]{1,64}` are recorded under `__invalid__`; every
//! valid name keeps its own series. Resolution is memoized per interned name
//! id, so the hot path is a read lock and a lookup, like the rest of the
//! dynamic-series machinery.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::client_metrics::interning::{intern, resolve};

/// Fallback `query_name` label value for names outside `[a-z0-9_]{1,64}`.
pub(crate) const INVALID_QUERY_NAME: &str = "__invalid__";

/// Whether `name` is a valid `query_name` label value: `[a-z0-9_]{1,64}`.
fn is_valid_query_name(name: &str) -> bool {
    let len = name.len();
    (1..=64).contains(&len)
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

/// Canonical query-name ids owned by one recorder (stored inside
/// [`MetricsNames`](super::names::MetricsNames) alongside its dynamic caches).
///
/// `resolved` memoizes the canonical (possibly fallback) interned id for every
/// seen name, so repeated emissions of one query skip validation entirely.
#[derive(Debug, Default)]
pub(crate) struct QueryNameRegistry {
    resolved: RwLock<HashMap<usize, usize>>,
}

impl QueryNameRegistry {
    /// Canonical interned id for `name`: the name itself when valid or
    /// `__invalid__` for a malformed name.
    ///
    /// The result feeds both the series cache keys and the `query_name` label
    /// value ([`resolve`]), so one resolution drives every named series of the
    /// operation. An invalid name produces one warning per recorder.
    pub(crate) fn resolve(&self, name: &str) -> usize {
        let id = intern(name);
        if let Some(hit) = self
            .resolved
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&id)
        {
            return *hit;
        }

        let valid = is_valid_query_name(name);
        let canonical = if valid {
            id
        } else {
            intern(INVALID_QUERY_NAME)
        };
        let mut resolved = self
            .resolved
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(hit) = resolved.get(&id) {
            return *hit;
        }
        if !valid {
            tracing::warn!(
                query_name = %name,
                "invalid query name: expected [a-z0-9_]{{1,64}}; using `{INVALID_QUERY_NAME}`"
            );
        }
        resolved.insert(id, canonical);
        canonical
    }

    /// Label value for a canonical id returned by [`Self::resolve`].
    pub(crate) fn label_value(canonical: usize) -> String {
        resolve(canonical)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    #[test]
    fn validation_accepts_spec_format() {
        assert!(is_valid_query_name("a"));
        assert!(is_valid_query_name("get_account_balance"));
        assert!(is_valid_query_name("upsert_remains_2"));
        assert!(is_valid_query_name(&"x".repeat(64)));
        assert!(!is_valid_query_name(""));
        assert!(!is_valid_query_name(&"x".repeat(65)));
        assert!(!is_valid_query_name("Get-Account"));
        assert!(!is_valid_query_name("with space"));
        assert!(!is_valid_query_name("юникод"));
    }

    #[test]
    fn static_and_owned_names_resolve_to_the_same_id() {
        let registry = QueryNameRegistry::default();
        let canonical = registry.resolve("get_account_balance");
        assert_eq!(
            QueryNameRegistry::label_value(canonical),
            "get_account_balance"
        );
        assert_eq!(registry.resolve("get_account_balance"), canonical);
        assert_eq!(
            registry.resolve(&String::from("get_account_balance")),
            canonical
        );
    }

    #[test]
    fn every_valid_name_resolves_to_itself() {
        let registry = QueryNameRegistry::default();
        for index in 0..600 {
            let name = format!("query_{index}");
            let canonical = registry.resolve(&name);
            assert_eq!(QueryNameRegistry::label_value(canonical), name);
            assert_eq!(registry.resolve(&name), canonical);
        }
    }

    #[test]
    fn invalid_names_map_to_invalid_bucket() {
        let registry = QueryNameRegistry::default();
        let canonical = registry.resolve("Bad-Name");
        assert_eq!(
            QueryNameRegistry::label_value(canonical),
            INVALID_QUERY_NAME
        );
    }

    #[derive(Clone, Default)]
    struct LogBuffer(Arc<Mutex<Vec<u8>>>);

    impl Write for LogBuffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuffer {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn invalid_name_warns_once_per_name() {
        let registry = QueryNameRegistry::default();
        let logs = LogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_max_level(tracing::Level::WARN)
            .with_writer(logs.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            registry.resolve("Bad-Name");
            registry.resolve("Bad-Name");
            registry.resolve("Other-Bad-Name");
            registry.resolve("Other-Bad-Name");
        });

        let logs = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        assert_eq!(logs.matches("invalid query name").count(), 2);
    }
}
