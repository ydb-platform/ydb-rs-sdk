//! Process-global string interner for dynamic metric label values.
//!
//! Dynamic metric series (gRPC transport metrics) carry runtime strings as label
//! values. The hot path maps every such string to a stable `usize` id (a read
//! lock plus a hash lookup, no allocation) and uses id tuples as per-recorder
//! handle cache keys. Interned strings are materialized again only when a
//! handle is first registered for a label combination.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

/// Interned string table shared by all [`DefaultMetricsRecorder`] instances.
///
/// Lock order: `map` is always acquired before `values` when both are needed,
/// so the two locks cannot deadlock. Entries are never removed: label sources
/// (endpoints, gRPC services and methods) are finite for a given cluster.
struct Interner {
    map: RwLock<HashMap<Box<str>, usize>>,
    values: RwLock<Vec<Arc<str>>>,
}

static INTERNER: OnceLock<Interner> = OnceLock::new();

fn interner() -> &'static Interner {
    INTERNER.get_or_init(|| Interner {
        map: RwLock::new(HashMap::new()),
        values: RwLock::new(Vec::new()),
    })
}

/// Map `value` to a stable id, interning it on first use.
///
/// The hit path takes a single read lock; interning happens once per distinct
/// string for the lifetime of the process.
pub(crate) fn intern(value: &str) -> usize {
    let table = interner();
    if let Some(&id) = table
        .map
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(value)
    {
        return id;
    }

    let mut map = table
        .map
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // Double-check under the write lock: another thread may have interned the
    // same value while this thread was waiting for the lock.
    if let Some(&id) = map.get(value) {
        return id;
    }

    let mut values = table
        .values
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let id = values.len();
    values.push(Arc::from(value));
    map.insert(Box::from(value), id);
    id
}

/// Materialize the string interned for `id` (a cheap `Arc` clone).
///
/// Ids are only produced by [`intern`], so the fallback is unreachable in
/// practice; it exists to keep the function total without panicking.
pub(crate) fn resolve(id: usize) -> Arc<str> {
    interner()
        .values
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(id)
        .cloned()
        .unwrap_or_else(|| Arc::from("unknown"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intern_is_idempotent_for_equal_strings() {
        let first = intern("interner-test://equal");
        let second = intern("interner-test://equal");
        assert_eq!(first, second, "equal strings must intern to one id");
    }

    #[test]
    fn intern_assigns_distinct_ids_to_distinct_strings() {
        let first = intern("interner-test://distinct-a");
        let second = intern("interner-test://distinct-b");
        assert_ne!(first, second);
    }

    #[test]
    fn resolve_round_trips_interned_values() {
        let id = intern("interner-test://round-trip");
        assert_eq!(&*resolve(id), "interner-test://round-trip");
    }

    #[test]
    fn concurrent_interning_is_idempotent() {
        let value = "interner-test://concurrent";
        let ids = std::thread::scope(|scope| {
            (0..8)
                .map(|_| {
                    scope
                        .spawn(|| intern(value))
                        .join()
                        .expect("test thread must not panic")
                })
                .collect::<Vec<usize>>()
        });
        assert!(
            ids.iter().all(|id| *id == ids[0]),
            "all threads must observe the same id, got {ids:?}"
        );
    }
}
