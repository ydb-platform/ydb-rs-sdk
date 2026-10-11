//! Canonicalization of caller-supplied query names into `query_name` label values.
//!
//! A query name is a `&'static str` constant from application code. Before it
//! becomes a label value it passes through [`QueryNameRegistry::resolve`]:
//!
//! - names outside `[a-z0-9_]{1,64}` are recorded under `__invalid__`
//!   (the API boundary — `CallBuilder::with_name` — additionally carries a
//!   `debug_assert!` so the violation is caught early in debug builds);
//! - valid names are admitted into the registry while the configured unique-name
//!   limit is not reached; once it is, new names collapse into `__other__`,
//!   bounding the series cardinality of the named metric rows.
//!
//! Resolution is memoized per interned name id, so the hot path is a read lock
//! and a lookup, like the rest of the dynamic-series machinery.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use crate::client_metrics::interning::{intern, resolve};

/// Fallback `query_name` label value for names outside `[a-z0-9_]{1,64}`.
pub(crate) const INVALID_QUERY_NAME: &str = "__invalid__";

/// Fallback `query_name` label value for valid names beyond the unique-name limit.
pub(crate) const OVERFLOW_QUERY_NAME: &str = "__other__";

/// Whether `name` is a valid `query_name` label value: `[a-z0-9_]{1,64}`.
pub(crate) fn is_valid_query_name(name: &str) -> bool {
    let len = name.len();
    (1..=64).contains(&len)
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

/// Registry of admitted query names, owned by one recorder (stored inside
/// [`MetricsNames`](super::names::MetricsNames) alongside its dynamic caches).
///
/// `accepted` holds the interned ids of admitted names (bounded by `max_names`);
/// `resolved` memoizes the canonical (possibly fallback) interned id for every
/// seen name, so repeated emissions of one query skip validation entirely.
#[derive(Debug)]
pub(crate) struct QueryNameRegistry {
    max_names: usize,
    accepted: RwLock<HashSet<usize>>,
    resolved: RwLock<HashMap<usize, usize>>,
}

impl Default for QueryNameRegistry {
    fn default() -> Self {
        Self::new(super::names::DEFAULT_MAX_QUERY_NAMES)
    }
}

impl QueryNameRegistry {
    pub(crate) fn new(max_names: usize) -> Self {
        Self {
            max_names,
            accepted: RwLock::new(HashSet::new()),
            resolved: RwLock::new(HashMap::new()),
        }
    }

    /// Canonical interned id for `name`: the name itself when valid and admitted,
    /// `__invalid__` for malformed names, `__other__` beyond the unique-name limit.
    ///
    /// The result feeds both the series cache keys and the `query_name` label
    /// value ([`resolve`]), so one resolution drives every named series of the
    /// operation.
    pub(crate) fn resolve(&self, name: &'static str) -> usize {
        let id = intern(name);
        if let Some(hit) = self
            .resolved
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&id)
        {
            return *hit;
        }

        let canonical = if !is_valid_query_name(name) {
            intern(INVALID_QUERY_NAME)
        } else {
            let mut accepted = self
                .accepted
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if accepted.contains(&id) || accepted.len() < self.max_names {
                accepted.insert(id);
                id
            } else {
                intern(OVERFLOW_QUERY_NAME)
            }
        };

        self.resolved
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(id, canonical);
        canonical
    }

    /// Label value for a canonical id returned by [`Self::resolve`].
    pub(crate) fn label_value(canonical: usize) -> metrics::SharedString {
        metrics::SharedString::from_shared(resolve(canonical))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn valid_names_resolve_to_themselves() {
        let registry = QueryNameRegistry::new(2);
        let canonical = registry.resolve("get_account_balance");
        assert_eq!(
            QueryNameRegistry::label_value(canonical).as_ref(),
            "get_account_balance"
        );
        // Repeat: cached, same canonical id.
        assert_eq!(registry.resolve("get_account_balance"), canonical);
    }

    #[test]
    fn invalid_names_map_to_invalid_bucket() {
        let registry = QueryNameRegistry::new(2);
        let canonical = registry.resolve("Bad-Name");
        assert_eq!(
            QueryNameRegistry::label_value(canonical).as_ref(),
            INVALID_QUERY_NAME
        );
    }

    #[test]
    fn names_beyond_limit_collapse_to_other() {
        let registry = QueryNameRegistry::new(2);
        let first = registry.resolve("first_query");
        let second = registry.resolve("second_query");
        let third = registry.resolve("third_query");
        // Repeated emissions keep their original canonical value.
        assert_eq!(registry.resolve("first_query"), first);
        assert_eq!(registry.resolve("second_query"), second);
        // New names beyond the limit share the overflow bucket.
        assert_eq!(
            QueryNameRegistry::label_value(third).as_ref(),
            OVERFLOW_QUERY_NAME
        );
        assert_eq!(registry.resolve("fourth_query"), third);
        // Fallback buckets do not consume the name budget.
        assert_eq!(
            registry.resolve("also-invalid!"),
            registry.resolve("Bad-Name")
        );
    }
}
