use ydb_grpc::ydb_proto::Value as ProtoValue;
use ydb_grpc::ydb_proto::value::Value as Primitive;
use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_table_service::value::{RawColumn, RawResultSet, RawTypedValue, RawValue, RawValuePair};
use crate::grpc_wrapper::raw_table_service::value::r#type::{decode_err, RawType};

impl TryFrom<ProtoValue> for RawValue {
    type Error = RawError;

    fn try_from(value: ProtoValue) -> Result<Self, Self::Error> {
        use crate::grpc_wrapper::raw_table_service::value::RawValue::*;
        use crate::grpc_wrapper::raw_table_service::value::RawVariantValue;

        if let Some(simple) = value.value {
            let res = match simple {
                ydb_grpc::ydb_proto::value::Value::BoolValue(v) => Bool(v),
                ydb_grpc::ydb_proto::value::Value::Int32Value(v) => Int32(v),
                ydb_grpc::ydb_proto::value::Value::Uint32Value(v) => UInt32(v),
                ydb_grpc::ydb_proto::value::Value::Int64Value(v) => Int64(v),
                ydb_grpc::ydb_proto::value::Value::Uint64Value(v) => UInt64(v),
                ydb_grpc::ydb_proto::value::Value::FloatValue(v) => Float(v),
                ydb_grpc::ydb_proto::value::Value::DoubleValue(v) => Double(v),
                ydb_grpc::ydb_proto::value::Value::BytesValue(v) => Bytes(v),
                ydb_grpc::ydb_proto::value::Value::TextValue(v) => Text(v),
                ydb_grpc::ydb_proto::value::Value::NullFlagValue(_) => NullFlag,
                ydb_grpc::ydb_proto::value::Value::NestedValue(v) => {
                    Variant(Box::new(RawVariantValue {
                        value: (*v).try_into()?,
                        index: value.variant_index,
                    }))
                }
                ydb_grpc::ydb_proto::value::Value::Low128(v) => HighLow128(value.high_128, v),
            };
            return Ok(res);
        };

        if value.items.len() > 0 {
            let items: Result<_, _> = value
                .items
                .into_iter()
                .map(|item| item.try_into())
                .collect();
            return Ok(Items(items?));
        };

        if value.pairs.len() > 0 {
            let pairs: Result<_, _> = value
                .pairs
                .into_iter()
                .map(|item| item.try_into())
                .collect();
            return Ok(Pairs(pairs?));
        };

        decode_err("empty value item")
    }
}

impl TryFrom<ydb_grpc::ydb_proto::ValuePair> for RawValuePair {
    type Error = RawError;

    fn try_from(value: ydb_grpc::ydb_proto::ValuePair) -> Result<Self, Self::Error> {
        let key = if let Some(k) = value.key {
            k
        } else {
            return decode_err("empty key value in proto pair");
        };

        let payload = if let Some(p) = value.payload {
            p
        } else {
            return decode_err("empty payload value in proto pair");
        };

        Ok(RawValuePair {
            key: key.try_into()?,
            payload: payload.try_into()?,
        })
    }
}

impl From<RawValue> for ProtoValue {
    fn from(v: RawValue) -> Self {
        match v {
            RawValue::Bool(v) => ProtoValue {
                value: Some(Primitive::BoolValue(v)),
                ..ProtoValue::default()
            },
            RawValue::Int32(v) => ProtoValue {
                value: Some(Primitive::Int32Value(v)),
                ..ProtoValue::default()
            },
            RawValue::UInt32(v) => ProtoValue {
                value: Some(Primitive::Uint32Value(v)),
                ..ProtoValue::default()
            },
            RawValue::Int64(v) => ProtoValue {
                value: Some(Primitive::Int64Value(v)),
                ..ProtoValue::default()
            },
            RawValue::UInt64(v) => ProtoValue {
                value: Some(Primitive::Uint64Value(v)),
                ..ProtoValue::default()
            },
            RawValue::HighLow128(h, l) => ProtoValue {
                value: Some(Primitive::Low128(l)),
                high_128: h,
                ..ProtoValue::default()
            },
            RawValue::Float(v) => ProtoValue {
                value: Some(Primitive::FloatValue(v)),
                ..ProtoValue::default()
            },
            RawValue::Double(v) => ProtoValue {
                value: Some(Primitive::DoubleValue(v)),
                ..ProtoValue::default()
            },
            RawValue::Bytes(v) => ProtoValue {
                value: Some(Primitive::BytesValue(v)),
                ..ProtoValue::default()
            },
            RawValue::Text(v) => ProtoValue {
                value: Some(Primitive::TextValue(v)),
                ..ProtoValue::default()
            },
            RawValue::NullFlag => ProtoValue {
                value: Some(Primitive::NullFlagValue(0)),
                ..ProtoValue::default()
            },
            RawValue::Items(v) => ProtoValue {
                items: v.into_iter().map(|item| item.into()).collect(),
                ..ProtoValue::default()
            },
            RawValue::Pairs(v) => ProtoValue {
                pairs: v.into_iter().map(|item| item.into()).collect(),
                ..ProtoValue::default()
            },
            RawValue::Variant(v) => ProtoValue {
                value: Some(ydb_grpc::ydb_proto::value::Value::NestedValue(Box::new(
                    v.value.into(),
                ))),
                variant_index: v.index,
                ..ProtoValue::default()
            },
        }
    }
}

impl From<RawValuePair> for ydb_grpc::ydb_proto::ValuePair {
    fn from(v: RawValuePair) -> Self {
        Self {
            key: Some(v.key.into()),
            payload: Some(v.payload.into()),
        }
    }
}

impl From<RawTypedValue> for ydb_grpc::ydb_proto::TypedValue {
    fn from(v: RawTypedValue) -> Self {
        Self {
            r#type: Some(v.r#type.into()),
            value: Some(v.value.into()),
        }
    }
}

impl TryFrom<ydb_grpc::ydb_proto::ResultSet> for RawResultSet {
    type Error = RawError;

    fn try_from(value: ydb_grpc::ydb_proto::ResultSet) -> Result<Self, Self::Error> {
        let columns_res: Result<Vec<RawColumn>, RawError> = value
            .columns
            .into_iter()
            .map(|item| RawColumn::try_from(item))
            .collect();
        let columns = columns_res?;

        let values_res: Result<Vec<RawValue>, RawError> =
            value.rows.into_iter().map(|item| item.try_into()).collect();
        let values = values_res?;

        let rows: Vec<Vec<RawValue>> = values
            .chunks(columns.len())
            .into_iter()
            .map(|item| item.into())
            .collect();

        Ok(Self { columns, rows })
    }
}

impl TryFrom<ydb_grpc::ydb_proto::Column> for RawColumn {
    type Error = RawError;

    fn try_from(value: ydb_grpc::ydb_proto::Column) -> Result<Self, Self::Error> {
        let t = value
            .r#type
            .ok_or(RawError::custom("empty type at column description"))?;

        Ok(Self {
            name: value.name,
            column_type: RawType::try_from(t)?,
        })
    }
}
