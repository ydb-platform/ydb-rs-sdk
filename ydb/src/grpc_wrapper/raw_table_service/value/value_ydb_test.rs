use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::raw_table_service::value::RawTypedValue;
use crate::types::{Value};

#[test]
fn convert()->RawResult<()>{
    fn check_value(value: Value)->RawResult<()>{
        let raw_typed: RawTypedValue = RawTypedValue::try_from(value.clone())?;
        let restored_value: Value = Value::try_from(raw_typed)?;
        assert_eq!(value, restored_value);
        Ok(())
    }

    let values = Value::examples();

    for value in values {
        if let Err(err) = check_value(value.clone()){
            return Err(RawError::custom(format!("bad check for value: '{:?}': {}", value, err)))
        }
    }

    Ok(())
}