use crate::{test_helpers::test_client_builder, Query, Value, YdbResult};

#[test]
fn test_is_optional()->YdbResult<()>{
    assert!(Value::optional_from(Value::Bool(false), None)?.is_optional());
    assert!(Value::optional_from(Value::Bool(false), Some(Value::Bool(false)))?.is_optional());
    assert!(!Value::Bool(false).is_optional());
    Ok(())
}




#[tokio::test]
#[ignore] // need YDB access
async fn test_decimal() -> YdbResult<()> {
   
    let client = test_client_builder().client()?;
    
    client.wait().await?;

    
let db_value: Option<decimal_rs::Decimal> = client
    .table_client()
    .retry_transaction(|mut t| async move {
        let res = t
        .query(Query::from(
            "select CAST(\"-1233333333333333333333345.34\" AS Decimal(28, 2)) as db_value",
        ))
        .await?;
        Ok(res.into_only_row()?.remove_field_by_name("db_value")?)
    })
    .await?
    .try_into()
    .unwrap();
    let test_value = Some("-1233333333333333333333345.34".parse::<decimal_rs::Decimal>().unwrap());
    assert_eq!(test_value, db_value);



    Ok(())
}

