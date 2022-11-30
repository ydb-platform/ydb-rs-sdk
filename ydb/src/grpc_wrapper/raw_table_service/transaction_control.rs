pub(crate) struct RawTransactionControl {
    pub commit_tx: bool,
}

pub(crate) enum RawTxSelector {
    Id(String),
    Begin(RawTxMode),
}

pub(crate) struct RawTxSettings {
    pub mode: RawTxMode,
}

pub(crate) enum RawTxMode {
    SerializableReadWrite,
    OnlineReadOnly(RawOnlineReadonlySettings),
    StaleReadOnly,
    SnapshotReadOnly,
}

pub(crate) struct RawOnlineReadonlySettings{
    pub allow_inconsistent_reads: bool,
}

impl From<RawTransactionControl> for ydb_grpc::ydb_proto::table::TransactionControl {
    fn from(v: RawTransactionControl) -> Self {
        Self {
            commit_tx: v.commit_tx,
            tx_selector: unimplemented!(),
        }
    }
}