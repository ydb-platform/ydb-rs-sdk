//! Endpoint label policy and bounded endpoint admission for gRPC metric series.
//!
//! Basic detail aggregates all endpoints without interning their addresses. In
//! endpoint detail, only admitted addresses are interned; addresses beyond the
//! per-recorder limit share `__other__` without being retained by the SDK.

use std::collections::HashSet;
use std::sync::RwLock;

use super::backend::MetricLabel;
use super::interning::{intern, lookup, resolve};

/// Default maximum number of distinct endpoint values admitted by one recorder.
pub(crate) const DEFAULT_MAX_ENDPOINTS: usize = 64;

/// Shared label value for endpoints beyond the configured limit.
pub(crate) const OVERFLOW_ENDPOINT: &str = "__other__";

/// Level of detail for gRPC transport metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub enum MetricsDetail {
    /// Aggregate gRPC series across endpoints, without an `endpoint` label.
    #[default]
    Basic,
    /// Split gRPC series by endpoint, up to the configured endpoint limit.
    Endpoints,
}

/// Cache key component for the optional `endpoint` label.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum EndpointKey {
    Aggregated,
    Value(usize),
}

impl EndpointKey {
    pub(crate) fn label(self) -> Option<MetricLabel> {
        match self {
            Self::Aggregated => None,
            Self::Value(id) => Some(MetricLabel::new("endpoint", resolve(id))),
        }
    }
}

/// Per-recorder set of admitted endpoint ids.
///
/// Membership checks use the interner's non-allocating lookup first. A new
/// address is interned only after the write lock confirms available capacity,
/// so overflow addresses never enter either the interner or this registry.
pub(crate) struct EndpointRegistry {
    detail: MetricsDetail,
    max_endpoints: usize,
    accepted: RwLock<HashSet<usize>>,
    overflow: usize,
}

impl Default for EndpointRegistry {
    fn default() -> Self {
        Self::new(MetricsDetail::Basic, DEFAULT_MAX_ENDPOINTS)
    }
}

impl EndpointRegistry {
    pub(crate) fn new(detail: MetricsDetail, max_endpoints: usize) -> Self {
        Self {
            detail,
            max_endpoints,
            accepted: RwLock::new(HashSet::new()),
            overflow: intern(OVERFLOW_ENDPOINT),
        }
    }

    pub(crate) fn set_detail(&mut self, detail: MetricsDetail) {
        self.detail = detail;
    }

    pub(crate) fn set_max_endpoints(&mut self, max_endpoints: usize) {
        self.max_endpoints = max_endpoints;
    }

    pub(crate) fn resolve(&self, endpoint: &str) -> EndpointKey {
        if self.detail == MetricsDetail::Basic {
            return EndpointKey::Aggregated;
        }

        {
            let accepted = self
                .accepted
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(id) = lookup(endpoint)
                && accepted.contains(&id)
            {
                return EndpointKey::Value(id);
            }
            // Entries are never removed, so a full registry remains full and
            // overflow does not need the admission write lock.
            if accepted.len() >= self.max_endpoints {
                return EndpointKey::Value(self.overflow);
            }
        }

        let mut accepted = self
            .accepted
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(id) = lookup(endpoint)
            && accepted.contains(&id)
        {
            return EndpointKey::Value(id);
        }
        if accepted.len() >= self.max_endpoints {
            return EndpointKey::Value(self.overflow);
        }

        let id = intern(endpoint);
        accepted.insert(id);
        EndpointKey::Value(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_detail_does_not_intern_endpoints() {
        let registry = EndpointRegistry::default();
        let endpoint = "r3-basic-not-interned:2135";

        assert_eq!(registry.resolve(endpoint), EndpointKey::Aggregated);
        assert_eq!(lookup(endpoint), None);
    }

    #[test]
    fn overflow_is_not_interned_or_admitted() {
        let registry = EndpointRegistry::new(MetricsDetail::Endpoints, 2);
        let first = registry.resolve("r3-endpoint-first:2135");
        let second = registry.resolve("r3-endpoint-second:2135");

        assert_eq!(registry.resolve("r3-endpoint-first:2135"), first);
        assert_eq!(registry.resolve("r3-endpoint-second:2135"), second);
        for index in 0..1_000 {
            let endpoint = format!("r3-overflow-{index}:2135");
            assert_eq!(
                registry.resolve(&endpoint),
                EndpointKey::Value(registry.overflow)
            );
            assert_eq!(lookup(&endpoint), None, "overflow retained {endpoint}");
        }
        assert_eq!(registry.accepted.read().unwrap().len(), 2);
    }

    #[test]
    fn zero_limit_keeps_only_the_overflow_bucket() {
        let registry = EndpointRegistry::new(MetricsDetail::Endpoints, 0);
        let endpoint = "r3-zero-limit:2135";

        assert_eq!(
            registry.resolve(endpoint),
            EndpointKey::Value(registry.overflow)
        );
        assert_eq!(lookup(endpoint), None);
        assert!(registry.accepted.read().unwrap().is_empty());
    }
}
