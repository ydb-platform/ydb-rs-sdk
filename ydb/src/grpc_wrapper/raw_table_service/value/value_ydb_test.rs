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
                "bad check for value: '{:?}': {}",
                value, err
            )));
        }
    }

    Ok(())
}
