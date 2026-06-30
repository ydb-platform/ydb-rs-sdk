use crate::grpc::proto_issues_to_ydb_issues;
use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_table_service::value::{RawResultSet, RawTypedValue};
use crate::YdbIssue;
use ydb_grpc::ydb_proto::{
    status_ids::StatusCode,
    table::{KeyRange, ReadTableRequest, ReadTableResponse},
};

#[derive(Clone, Debug, Default)]
pub(crate) enum RawReadTableKeyBound {
    #[default]
    Unset,
    Greater(RawTypedValue),
    GreaterOrEqual(RawTypedValue),
    Less(RawTypedValue),
    LessOrEqual(RawTypedValue),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RawReadTableKeyRange {
    pub from: RawReadTableKeyBound,
    pub to: RawReadTableKeyBound,
}

impl RawReadTableKeyRange {
    fn into_proto(self) -> KeyRange {
        let mut key_range = KeyRange::default();
        match self.from {
            RawReadTableKeyBound::Unset => {}
            RawReadTableKeyBound::Greater(v) => {
                key_range.from_bound = Some(ydb_grpc::ydb_proto::table::key_range::FromBound::Greater(
                    v.into(),
                ));
            }
            RawReadTableKeyBound::GreaterOrEqual(v) => {
                key_range.from_bound = Some(
                    ydb_grpc::ydb_proto::table::key_range::FromBound::GreaterOrEqual(v.into()),
                );
            }
            RawReadTableKeyBound::Less(_) | RawReadTableKeyBound::LessOrEqual(_) => {}
        }
        match self.to {
            RawReadTableKeyBound::Unset => {}
            RawReadTableKeyBound::Less(v) => {
                key_range.to_bound = Some(ydb_grpc::ydb_proto::table::key_range::ToBound::Less(
                    v.into(),
                ));
            }
            RawReadTableKeyBound::LessOrEqual(v) => {
                key_range.to_bound = Some(ydb_grpc::ydb_proto::table::key_range::ToBound::LessOrEqual(
                    v.into(),
                ));
            }
            RawReadTableKeyBound::Greater(_) | RawReadTableKeyBound::GreaterOrEqual(_) => {}
        }
        key_range
    }
}

pub(crate) struct RawStreamReadTableRequest {
    pub session_id: String,
    pub path: String,
    pub key_range: Option<RawReadTableKeyRange>,
    pub columns: Vec<String>,
    pub ordered: bool,
    pub row_limit: u64,
}

impl From<RawStreamReadTableRequest> for ReadTableRequest {
    fn from(value: RawStreamReadTableRequest) -> Self {
        Self {
            session_id: value.session_id,
            path: value.path,
            key_range: value.key_range.map(|range| range.into_proto()),
            columns: value.columns,
            ordered: value.ordered,
            row_limit: value.row_limit,
            ..Default::default()
        }
    }
}

pub(crate) struct RawReadTableResponsePart {
    pub status: StatusCode,
    pub issues: Vec<YdbIssue>,
    pub result_set: Option<RawResultSet>,
}

impl TryFrom<ReadTableResponse> for RawReadTableResponsePart {
    type Error = RawError;

    fn try_from(value: ReadTableResponse) -> Result<Self, Self::Error> {
        let status = value.status();
        let result_set = value
            .result
            .and_then(|result| result.result_set)
            .map(|set| set.try_into())
            .transpose()?;

        Ok(Self {
            status,
            issues: proto_issues_to_ydb_issues(value.issues),
            result_set,
        })
    }
}
