use ydb_grpc::ydb_proto::coordination::SessionDescription;

#[derive(Debug, serde::Serialize)]
pub(crate) struct RawSessionDescription {
    pub session_id: u64,
    pub timeout_millis: u64,
    pub description: String,
    pub attached: bool,
}

impl From<SessionDescription> for RawSessionDescription {
    fn from(value: SessionDescription) -> Self {
        Self {
            session_id: value.session_id,
            timeout_millis: value.timeout_millis,
            description: value.description,
            attached: value.attached,
        }
    }
}
