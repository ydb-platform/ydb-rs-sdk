use ydb_grpc::ydb_proto::feature_flag::Status as FeatureFlagStatus;
use ydb_grpc::ydb_proto::table::PartitioningSettings;

/// Raw form of the table `PartitioningSettings` message.
///
/// Booleans are optional because the wire type is a tri-state feature flag:
/// `None` leaves the setting untouched, which matters for `AlterTable`.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct RawPartitioningSettings {
    pub partition_by: Vec<String>,
    pub partitioning_by_size: Option<bool>,
    pub partition_size_mb: Option<u64>,
    pub partitioning_by_load: Option<bool>,
    pub min_partitions_count: Option<u64>,
    pub max_partitions_count: Option<u64>,
}

fn to_feature_flag(value: Option<bool>) -> i32 {
    match value {
        None => FeatureFlagStatus::Unspecified as i32,
        Some(true) => FeatureFlagStatus::Enabled as i32,
        Some(false) => FeatureFlagStatus::Disabled as i32,
    }
}

fn from_feature_flag(value: i32) -> Option<bool> {
    match FeatureFlagStatus::try_from(value) {
        Ok(FeatureFlagStatus::Enabled) => Some(true),
        Ok(FeatureFlagStatus::Disabled) => Some(false),
        // Unspecified, or a status this SDK version does not know about.
        _ => None,
    }
}

impl From<RawPartitioningSettings> for PartitioningSettings {
    fn from(value: RawPartitioningSettings) -> Self {
        Self {
            partition_by: value.partition_by,
            partitioning_by_size: to_feature_flag(value.partitioning_by_size),
            partition_size_mb: value.partition_size_mb.unwrap_or_default(),
            partitioning_by_load: to_feature_flag(value.partitioning_by_load),
            min_partitions_count: value.min_partitions_count.unwrap_or_default(),
            max_partitions_count: value.max_partitions_count.unwrap_or_default(),
        }
    }
}

impl From<PartitioningSettings> for RawPartitioningSettings {
    fn from(value: PartitioningSettings) -> Self {
        // The server reports unset counts as 0; keep that as `None` so a
        // describe result round-trips back into an alter request unchanged.
        let non_zero = |v: u64| if v == 0 { None } else { Some(v) };

        Self {
            partition_by: value.partition_by,
            partitioning_by_size: from_feature_flag(value.partitioning_by_size),
            partition_size_mb: non_zero(value.partition_size_mb),
            partitioning_by_load: from_feature_flag(value.partitioning_by_load),
            min_partitions_count: non_zero(value.min_partitions_count),
            max_partitions_count: non_zero(value.max_partitions_count),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_flags_round_trip_through_the_tri_state() {
        for value in [None, Some(true), Some(false)] {
            assert_eq!(from_feature_flag(to_feature_flag(value)), value);
        }
    }

    #[test]
    fn unknown_feature_flag_reads_as_unset() {
        assert_eq!(from_feature_flag(9999), None);
    }

    #[test]
    fn settings_round_trip_through_proto() {
        let settings = RawPartitioningSettings {
            partition_by: vec!["id".to_string()],
            partitioning_by_size: Some(true),
            partition_size_mb: Some(256),
            partitioning_by_load: Some(false),
            min_partitions_count: Some(2),
            max_partitions_count: Some(64),
        };

        let restored = RawPartitioningSettings::from(PartitioningSettings::from(settings.clone()));

        assert_eq!(restored, settings);
    }

    /// Zero counts are how the server spells "unset"; they must not come back
    /// as `Some(0)` and get re-sent as an explicit zero.
    #[test]
    fn zero_counts_decode_as_unset() {
        let restored = RawPartitioningSettings::from(PartitioningSettings::default());

        assert_eq!(restored, RawPartitioningSettings::default());
    }
}
