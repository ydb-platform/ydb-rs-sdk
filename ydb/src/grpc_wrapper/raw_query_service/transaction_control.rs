use ydb_grpc::ydb_proto::query::{
    transaction_control, transaction_settings, OnlineModeSettings, SerializableModeSettings,
    SnapshotModeSettings, SnapshotRwModeSettings, StaleModeSettings, TransactionControl,
    TransactionSettings,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RawQueryTxMode {
    SerializableReadWrite,
    SnapshotReadOnly,
    SnapshotReadWrite,
    StaleReadOnly,
    OnlineReadOnly,
    OnlineReadOnlyInconsistent,
}

pub(crate) fn begin_tx_control(mode: RawQueryTxMode, commit_tx: bool) -> TransactionControl {
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

pub(crate) fn tx_settings_for_mode(mode: RawQueryTxMode) -> TransactionSettings {
    tx_settings(mode)
}

fn tx_settings(mode: RawQueryTxMode) -> TransactionSettings {
    let tx_mode = match mode {
        RawQueryTxMode::SerializableReadWrite => {
            transaction_settings::TxMode::SerializableReadWrite(SerializableModeSettings {})
        }
        RawQueryTxMode::SnapshotReadOnly => {
            transaction_settings::TxMode::SnapshotReadOnly(SnapshotModeSettings {})
        }
        RawQueryTxMode::SnapshotReadWrite => {
            transaction_settings::TxMode::SnapshotReadWrite(SnapshotRwModeSettings {})
        }
        RawQueryTxMode::StaleReadOnly => {
            transaction_settings::TxMode::StaleReadOnly(StaleModeSettings {})
        }
        RawQueryTxMode::OnlineReadOnly => {
            transaction_settings::TxMode::OnlineReadOnly(OnlineModeSettings {
                allow_inconsistent_reads: false,
            })
        }
        RawQueryTxMode::OnlineReadOnlyInconsistent => {
            transaction_settings::TxMode::OnlineReadOnly(OnlineModeSettings {
                allow_inconsistent_reads: true,
            })
        }
    };
    TransactionSettings {
        tx_mode: Some(tx_mode),
    }
}
