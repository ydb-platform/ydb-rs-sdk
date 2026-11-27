use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::raw_table_service::value::RawTypedValue;
use crate::types::Value;
use ydb_grpc::ydb_proto::TypedValue;

#[test]
fn convert_ydb_raw_grpc() -> RawResult<()> {
    fn check_value(value: Value) -> Result<(), Box<dyn std::error::Error>> {
        let raw_typed: RawTypedValue = RawTypedValue::try_from(value.clone())?;
        let proto_typed_value: TypedValue = TypedValue::from(raw_typed);
        let restored_raw_typed = RawTypedValue::try_from(proto_typed_value)?;
        let restored_value: Value = Value::try_from(restored_raw_typed)?;
        assert_eq!(value, restored_value);
        Ok(())
    }

    let values = Value::examples_for_test();

    for value in values {
        if let Err(err) = check_value(value.clone()) {
            return Err(RawError::custom(format!(
                "bad check for value: '{value:?}': {err}"
            )));
        }
    }

    Ok(())
}

/// YDB has no wire type for Set: it must go out as `Dict<T, Void>` with a
/// null-flag payload per key. Pin that shape - a plain `Dict` type or a
/// non-void payload would be a different YQL type.
#[test]
fn set_is_encoded_as_dict_with_void_payload() -> RawResult<()> {
    use crate::grpc_wrapper::raw_table_service::value::RawValue;
    use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;

    let value = Value::set_from(Value::Int32(0), vec![Value::Int32(1), Value::Int32(2)])
        .expect("int32 members match the example");

    let raw = RawTypedValue::try_from(value)?;

    match raw.r#type {
        RawType::Dict(dict) => {
            assert_eq!(dict.key, RawType::Int32);
            assert_eq!(dict.payload, RawType::Void);
        }
        other => panic!("expected a dict type, got {other:?}"),
    }

    match raw.value {
        RawValue::Pairs(pairs) => {
            assert_eq!(pairs.len(), 2);
            for pair in pairs {
                assert!(
                    matches!(pair.payload, RawValue::NullFlag),
                    "set payloads must be void"
                );
            }
        }
        other => panic!("expected pairs, got {other:?}"),
    }

    Ok(())
}

/// An empty set still carries its element type, so it round-trips as
/// `Dict<T, Void>` with no pairs rather than losing the type.
#[test]
fn empty_set_keeps_its_element_type() -> RawResult<()> {
    use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;

    let value = Value::set_from(Value::Text(String::new()), Vec::new())
        .expect("an empty set of a known type is valid");
    let raw = RawTypedValue::try_from(value.clone())?;

    match &raw.r#type {
        RawType::Dict(dict) => assert_eq!(dict.key, RawType::UTF8),
        other => panic!("expected a dict type, got {other:?}"),
    }

    let restored: Value = Value::try_from(raw)?;
    assert_eq!(restored, value);

    Ok(())
}

/// A dict with a real payload is a `Dict`, not a `Set`. The public `Value`
/// does not model dicts yet, so decoding one must fail loudly rather than
/// silently dropping the payloads and pretending it was a set.
#[test]
fn dict_with_non_void_payload_is_not_decoded_as_a_set() {
    use crate::grpc_wrapper::raw_table_service::value::r#type::{DictType, RawType};
    use crate::grpc_wrapper::raw_table_service::value::{RawTypedValue, RawValue};

    let raw = RawTypedValue {
        r#type: RawType::Dict(Box::new(DictType {
            key: RawType::Int32,
            payload: RawType::UTF8,
        })),
        value: RawValue::Pairs(Vec::new()),
    };

    Value::try_from(raw).expect_err("a dict with a payload must not decode as a set");
}
