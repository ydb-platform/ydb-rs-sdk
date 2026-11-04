use std::cmp::Ordering;
use std::sync::Arc;

use crate::errors::{YdbError, YdbResult};

/// A database commit position returned for a successful StrictSerializableRW write.
///
/// The two components are unsigned 64-bit values. A timestamp can only be compared with
/// another timestamp obtained through the same [`Client`](crate::Client) or its derived clients.
/// Separate drivers cannot prove that their timestamps belong to the same database, even when
/// their database paths match.
#[derive(Clone, Debug)]
pub struct VirtualTimestamp {
    plan_step: u64,
    tx_id: u64,
    scope: Arc<str>,
}

impl VirtualTimestamp {
    pub(crate) fn from_proto(
        value: ydb_grpc::ydb_proto::VirtualTimestamp,
        scope: Arc<str>,
    ) -> Self {
        Self {
            plan_step: value.plan_step,
            tx_id: value.tx_id,
            scope,
        }
    }

    pub fn plan_step(&self) -> u64 {
        self.plan_step
    }

    pub fn tx_id(&self) -> u64 {
        self.tx_id
    }

    pub fn database(&self) -> &str {
        &self.scope
    }

    /// Compare positions from the same driver, first by `plan_step`, then by `tx_id`.
    ///
    /// Returns an error for positions from separately built drivers. A database path alone is
    /// not sufficient to establish that two drivers point to the same database.
    pub fn compare(&self, other: &Self) -> YdbResult<Ordering> {
        if !Arc::ptr_eq(&self.scope, &other.scope) {
            return Err(YdbError::Custom(format!(
                "cannot compare virtual timestamps from different drivers ({} and {})",
                self.scope, other.scope
            )));
        }
        Ok((self.plan_step, self.tx_id).cmp(&(other.plan_step, other.tx_id)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamp(plan_step: u64, tx_id: u64, scope: Arc<str>) -> VirtualTimestamp {
        VirtualTimestamp {
            plan_step,
            tx_id,
            scope,
        }
    }

    #[test]
    fn compares_unsigned_components_lexicographically() {
        let scope: Arc<str> = Arc::from("/Root/db");
        let earlier = stamp(1, u64::MAX, scope.clone());
        let later = stamp(2, 0, scope.clone());
        let same_step_later = stamp(2, u64::MAX, scope.clone());
        assert_eq!(earlier.compare(&later).unwrap(), Ordering::Less);
        assert_eq!(later.compare(&same_step_later).unwrap(), Ordering::Less);
        assert_eq!(same_step_later.compare(&later).unwrap(), Ordering::Greater);
        assert_eq!(later.compare(&later).unwrap(), Ordering::Equal);
    }

    #[test]
    fn rejects_different_driver_scopes_even_for_same_path() {
        let first = stamp(1, 1, Arc::from("/Root/db"));
        let same_path_other_driver = stamp(1, 2, Arc::from("/Root/db"));
        let other_database = stamp(1, 2, Arc::from("/Root/other"));
        assert!(first.compare(&same_path_other_driver).is_err());
        assert!(first.compare(&other_database).is_err());
    }
}
