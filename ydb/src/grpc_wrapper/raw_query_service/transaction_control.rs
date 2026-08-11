use ydb_grpc::ydb_proto::query::{
    OnlineModeSettings, SerializableModeSettings, SnapshotModeSettings, SnapshotRwModeSettings,
    StaleModeSettings, TransactionControl, TransactionSettings, transaction_control,
    transaction_settings,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TransactionId(String);

impl TransactionId {
    pub(crate) fn from_server(value: String) -> Option<Self> {
        (!value.is_empty()).then_some(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RawTxMode {
    SerializableReadWrite,
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

pub(crate) fn tx_id_control(tx_id: &TransactionId, commit_tx: bool) -> TransactionControl {
    TransactionControl {
        commit_tx,
        tx_selector: Some(transaction_control::TxSelector::TxId(
            tx_id.as_str().to_string(),
        )),
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
