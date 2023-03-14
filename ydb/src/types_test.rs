use crate::{Value, YdbResult};

#[test]
fn test_is_optional()->YdbResult<()>{
    assert!(Value::optional_from(Value::Bool(false), None)?.is_optional());
    assert!(Value::optional_from(Value::Bool(false), Some(Value::Bool(false)))?.is_optional());
    assert!(!Value::Bool(false).is_optional());
    Ok(())
}
