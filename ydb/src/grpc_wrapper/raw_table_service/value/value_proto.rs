use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;
use crate::grpc_wrapper::raw_table_service::value::{RawTypedValue, RawValue};

impl TryFrom<ydb_grpc::ydb_proto::TypedValue> for RawTypedValue {
    type Error = RawError;

    fn try_from(value: ydb_grpc::ydb_proto::TypedValue) -> Result<Self, Self::Error> {
        let t = if let Some(t) = value.r#type {
            RawType::try_from(t)?
        } else {
            return Err(RawError::decode_error(format!("empty type in proto typed value")))
        };

        let v = if let Some(v) = value.value {
            RawValue::try_from(v)?
        } else {
            RawValue::NullFlag
        };

        Ok(Self {
            r#type: t,
            value: v,
        })
    }
}
