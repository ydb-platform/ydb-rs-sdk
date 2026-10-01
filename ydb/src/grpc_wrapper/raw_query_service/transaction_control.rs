use ydb_grpc::ydb_proto::query::{
    OnlineModeSettings, SerializableModeSettings, SnapshotModeSettings, SnapshotRwModeSettings,
    StaleModeSettings, StrictSerializableRwModeSettings, TransactionControl, TransactionSettings,
    transaction_control, transaction_settings,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RawTxMode {
    SerializableReadWrite,
    StrictSerializableRW,
    SnapshotReadOnly,
    SnapshotReadWrite,
    StaleReadOnly,
    OnlineReadOnly,
    OnlineReadOnlyInconsistent,
}

pub(crate) fn begin_tx_control(mode: RawTxMode, commit_tx: bool) -> TransactionControl {
    TransactionControl {
        commit_tx,
        tx_selector: Some(transaction_control::TxSelector::BeginTx(tx_settings(mode))),
    }
}

pub(crate) fn tx_id_control(tx_id: &str, commit_tx: bool) -> TransactionControl {
    TransactionControl {
        commit_tx,
        tx_selector: Some(transaction_control::TxSelector::TxId(tx_id.to_string())),
    }
}

pub(crate) fn tx_settings_for_mode(mode: RawTxMode) -> TransactionSettings {
    tx_settings(mode)
}

fn tx_settings(mode: RawTxMode) -> TransactionSettings {
    let tx_mode = match mode {
        RawTxMode::SerializableReadWrite => {
            transaction_settings::TxMode::SerializableReadWrite(SerializableModeSettings {})
        }
        RawTxMode::StrictSerializableRW => {
            transaction_settings::TxMode::StrictSerializableReadWrite(
                StrictSerializableRwModeSettings {},
            )
        }
        RawTxMode::SnapshotReadOnly => {
            transaction_settings::TxMode::SnapshotReadOnly(SnapshotModeSettings {})
        }
        RawTxMode::SnapshotReadWrite => {
            transaction_settings::TxMode::SnapshotReadWrite(SnapshotRwModeSettings {})
        }
        RawTxMode::StaleReadOnly => {
            transaction_settings::TxMode::StaleReadOnly(StaleModeSettings {})
        }
        RawTxMode::OnlineReadOnly => {
            transaction_settings::TxMode::OnlineReadOnly(OnlineModeSettings {
                allow_inconsistent_reads: false,
            })
        }
        RawTxMode::OnlineReadOnlyInconsistent => {
            transaction_settings::TxMode::OnlineReadOnly(OnlineModeSettings {
                allow_inconsistent_reads: true,
            })
        }
    };
    TransactionSettings {
        tx_mode: Some(tx_mode),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_serializable_uses_query_proto_field_seven() {
        let settings = tx_settings_for_mode(RawTxMode::StrictSerializableRW);
        assert!(matches!(
            settings.tx_mode,
            Some(transaction_settings::TxMode::StrictSerializableReadWrite(_))
        ));
        let encoded = prost::Message::encode_to_vec(&settings);
        assert_eq!(encoded, vec![0x3a, 0x00]);

        let control = begin_tx_control(RawTxMode::StrictSerializableRW, true);
        assert!(control.commit_tx);
        assert!(matches!(
            control.tx_selector,
            Some(transaction_control::TxSelector::BeginTx(
                TransactionSettings {
                    tx_mode: Some(transaction_settings::TxMode::StrictSerializableReadWrite(_))
                }
            ))
        ));
    }
}
