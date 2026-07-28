use std::fmt;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct PartitionSessionId(i64);

impl PartitionSessionId {
    pub(crate) fn from_raw(value: i64) -> Self {
        Self(value)
    }

    pub(crate) fn into_raw(self) -> i64 {
        self.0
    }
}

impl fmt::Display for PartitionSessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct PartitionId(i64);

impl PartitionId {
    pub(crate) fn from_raw(value: i64) -> Self {
        Self(value)
    }

    pub(crate) fn into_raw(self) -> i64 {
        self.0
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
