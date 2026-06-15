use ydb_grpc::ydb_proto::query::{
    transaction_control, transaction_settings, OnlineModeSettings, SerializableModeSettings,
    SnapshotModeSettings, StaleModeSettings, TransactionControl, TransactionSettings,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RawQueryTxMode {
    SerializableReadWrite,
    SnapshotReadOnly,
    StaleReadOnly,
    OnlineReadOnly,
}

pub(crate) fn implicit_tx_control() -> Option<TransactionControl> {
    None
}

pub(crate) fn begin_tx_control(mode: RawQueryTxMode) -> TransactionControl {
    TransactionControl {
        commit_tx: false,
        tx_selector: Some(transaction_control::TxSelector::BeginTx(tx_settings(mode))),
    }
}

pub(crate) fn tx_id_control(tx_id: &str) -> TransactionControl {
    TransactionControl {
        commit_tx: false,
        tx_selector: Some(transaction_control::TxSelector::TxId(tx_id.to_string())),
    }
}

fn tx_settings(mode: RawQueryTxMode) -> TransactionSettings {
    let tx_mode = match mode {
        RawQueryTxMode::SerializableReadWrite => {
            transaction_settings::TxMode::SerializableReadWrite(SerializableModeSettings {})
        }
        RawQueryTxMode::SnapshotReadOnly => {
            transaction_settings::TxMode::SnapshotReadOnly(SnapshotModeSettings {})
        }
        RawQueryTxMode::StaleReadOnly => {
            transaction_settings::TxMode::StaleReadOnly(StaleModeSettings {})
        }
        RawQueryTxMode::OnlineReadOnly => {
            // Public API does not expose allow_inconsistent_reads; keep stale-replica reads disabled.
            transaction_settings::TxMode::OnlineReadOnly(OnlineModeSettings {
                allow_inconsistent_reads: false,
            })
        }
    };
    TransactionSettings {
        tx_mode: Some(tx_mode),
    }
}
