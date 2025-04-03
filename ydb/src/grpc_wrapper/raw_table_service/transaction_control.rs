#[derive(serde::Serialize)]
pub(crate) struct RawTransactionControl {
    pub commit_tx: bool,
    pub tx_selector: RawTxSelector,
}

#[derive(serde::Serialize)]
pub(crate) enum RawTxSelector {
    Id(String),
    Begin(RawTxSettings),
}

impl From<RawTxSelector> for ydb_grpc::ydb_proto::table::transaction_control::TxSelector {
    fn from(v: RawTxSelector) -> Self {
        use ydb_grpc::ydb_proto::table::transaction_control::TxSelector;
        match v {
            RawTxSelector::Id(id) => TxSelector::TxId(id),
            RawTxSelector::Begin(tx_settings) => TxSelector::BeginTx(tx_settings.into()),
        }
    }
}

#[derive(serde::Serialize)]
pub(crate) struct RawTxSettings {
    pub mode: RawTxMode,
}

impl From<RawTxSettings> for ydb_grpc::ydb_proto::table::TransactionSettings {
    fn from(v: RawTxSettings) -> Self {
        Self {
            tx_mode: Some(v.mode.into()),
        }
    }
}

#[derive(serde::Serialize)]
pub(crate) enum RawTxMode {
    SerializableReadWrite,
    OnlineReadOnly(RawOnlineReadonlySettings),
    StaleReadOnly,
}

impl From<RawTxMode> for ydb_grpc::ydb_proto::table::transaction_settings::TxMode {
    fn from(v: RawTxMode) -> Self {
        use ydb_grpc::ydb_proto::table;
        use ydb_grpc::ydb_proto::table::transaction_settings::TxMode;
        match v {
            RawTxMode::SerializableReadWrite => {
                TxMode::SerializableReadWrite(table::SerializableModeSettings {})
            }
            RawTxMode::OnlineReadOnly(RawOnlineReadonlySettings {
                allow_inconsistent_reads,
            }) => TxMode::OnlineReadOnly(table::OnlineModeSettings {
                allow_inconsistent_reads,
            }),
            RawTxMode::StaleReadOnly => TxMode::StaleReadOnly(table::StaleModeSettings {}),
        }
    }
}

#[derive(serde::Serialize)]
pub(crate) struct RawOnlineReadonlySettings {
    pub allow_inconsistent_reads: bool,
}

impl From<RawTransactionControl> for ydb_grpc::ydb_proto::table::TransactionControl {
    fn from(v: RawTransactionControl) -> Self {
        Self {
            commit_tx: v.commit_tx,
            tx_selector: Some(v.tx_selector.into()),
        }
    }
}
