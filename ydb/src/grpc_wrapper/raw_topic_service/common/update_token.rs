use ydb_grpc::ydb_proto::topic::{UpdateTokenRequest, UpdateTokenResponse};

pub(crate) struct RawUpdateTokenRequest {
    pub(crate) token: String,
}

impl From<RawUpdateTokenRequest> for UpdateTokenRequest {
    fn from(value: RawUpdateTokenRequest) -> Self {
        Self { token: value.token }
    }
}

pub(crate) struct RawUpdateTokenResponse {}

impl From<UpdateTokenResponse> for RawUpdateTokenResponse {
    fn from(value: UpdateTokenResponse) -> Self {
        Self {}
    }
}
