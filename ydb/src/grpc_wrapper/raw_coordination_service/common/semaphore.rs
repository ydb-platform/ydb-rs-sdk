use ydb_grpc::ydb_proto::coordination::{SemaphoreDescription, SemaphoreSession};

#[derive(Debug, serde::Serialize)]
pub(crate) struct RawSemaphoreSession {
    pub order_id: u64,
    pub session_id: u64,
    pub timeout_millis: u64,
    pub count: u64,
    pub data: Vec<u8>,
}

impl From<SemaphoreSession> for RawSemaphoreSession {
    fn from(value: SemaphoreSession) -> Self {
        Self {
            order_id: value.order_id,
            session_id: value.session_id,
            timeout_millis: value.timeout_millis,
            count: value.count,
            data: value.data,
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct RawSemaphoreDescription {
    pub name: String,
    pub data: Vec<u8>,
    pub count: u64,
    pub limit: u64,
    pub ephemeral: bool,
    pub owners: Vec<RawSemaphoreSession>,
    pub waiters: Vec<RawSemaphoreSession>,
}

impl From<SemaphoreDescription> for RawSemaphoreDescription {
    fn from(value: SemaphoreDescription) -> Self {
        Self {
            name: value.name,
            data: value.data,
            count: value.count,
            limit: value.limit,
            ephemeral: value.ephemeral,
            owners: value.owners.into_iter().map(|v| v.into()).collect(),
            waiters: value.waiters.into_iter().map(|v| v.into()).collect(),
        }
    }
}
