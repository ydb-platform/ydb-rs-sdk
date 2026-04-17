use crate::client::Client;
use crate::errors::YdbError;
use crate::types::YdbDecimal;
use crate::{test_helpers::test_client_builder, ydb_params, Query, Value, YdbResult};
use uuid::Uuid;

#[test]
fn test_is_optional() -> YdbResult<()> {
    assert!(Value::optional_from(Value::Bool(false), None)?.is_optional());
    assert!(Value::optional_from(Value::Bool(false), Some(Value::Bool(false)))?.is_optional());
    assert!(!Value::Bool(false).is_optional());
    Ok(())
}

#[test]
fn test_ydb_decimal_preserves_precision_and_scale() {
    let value = "123.45".parse::<decimal_rs::Decimal>().unwrap();
    let ydb_dec = YdbDecimal::try_new(value, 22, 9).unwrap();
    assert_eq!(ydb_dec.precision(), 22);
    assert_eq!(ydb_dec.scale(), 9);
}

#[test]
fn test_ydb_decimal_rejects_precision_overflow() {
    // 10 digits, precision 5 — should fail
    let value = "1234567890.12".parse::<decimal_rs::Decimal>().unwrap();
    let result = YdbDecimal::try_new(value, 5, 2);
    assert!(result.is_err());
}

#[test]
fn test_ydb_decimal_rejects_scale_decrease() {
    // value has scale=3, requested scale=1 — should fail
    let value = "1.234".parse::<decimal_rs::Decimal>().unwrap();
    let result = YdbDecimal::try_new(value, 10, 1);
    assert!(result.is_err());
}

#[test]
fn test_ydb_decimal_allows_scale_increase() {
    // value has scale=2, requested scale=9 — should succeed
    let value = "123.45".parse::<decimal_rs::Decimal>().unwrap();
    let ydb_dec = YdbDecimal::try_new(value, 22, 9).unwrap();
    assert_eq!(ydb_dec.scale(), 9);
    assert_eq!(ydb_dec.precision(), 22);
}

#[test]
fn test_ydb_decimal_into_value() {
    let value = "99.99".parse::<decimal_rs::Decimal>().unwrap();
    let ydb_dec = YdbDecimal::try_new(value, 10, 2).unwrap();
    let v: Value = ydb_dec.into();
    match v {
        Value::Decimal(d) => {
            assert_eq!(d.precision(), 10);
            assert_eq!(d.scale(), 2);
        }
        _ => panic!("expected Value::Decimal"),
    }
}

#[test]
fn test_ydb_decimal_new_unchecked() {
    let value = "1.5".parse::<decimal_rs::Decimal>().unwrap();
    let ydb_dec = YdbDecimal::new_unchecked(value, 22, 9);
    assert_eq!(ydb_dec.precision(), 22);
    assert_eq!(ydb_dec.scale(), 9);
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
    let test_value = Some(
        "-1233333333333333333333345.34"
            .parse::<decimal_rs::Decimal>()
            .unwrap(),
    );
    assert_eq!(test_value, db_value);

    Ok(())
}

#[tokio::test]
#[ignore = "needs YDB access"]
async fn test_uuid_serialization() -> YdbResult<()> {
    let client = test_client_builder().client()?;
    client.wait().await?;

    let test_cases: Vec<Uuid> = vec![
        (uuid::Uuid::now_v7()),
        (uuid::Uuid::new_v4()),
        (uuid::Uuid::nil()),
        (Uuid::from_u128(0x1234567890abcdef1234567890abcdef)),
    ];

    for test_uuid in &test_cases {
        check_uuid_as_uuid_serialization(&client, *test_uuid).await?;
    }

    for test_uuid in &test_cases {
        check_uuid_as_utf8_serialization(&client, *test_uuid).await?;
    }

    for test_uuid in &test_cases {
        check_text_as_uuid_serialization(&client, *test_uuid).await?;
    }

    Ok(())
}

async fn check_uuid_as_uuid_serialization(client: &Client, test_uuid: Uuid) -> YdbResult<()> {
    let (db_value,): (Option<Uuid>,) = client
        .table_client()
        .retry_transaction(|mut t| async move {
            let res = t
                .query(
                    Query::new("select $test_uuid as db_value").with_params(ydb_params! {
                        "$test_uuid" => test_uuid,
                    }),
                )
                .await?;
            let mut row = res.into_only_row()?;
            let value: Option<Uuid> = row.remove_field_by_name("db_value")?.try_into()?;
            Ok((value,))
        })
        .await?;

    assert_eq!(Some(test_uuid), db_value);
    Ok(())
}

async fn check_uuid_as_utf8_serialization(client: &Client, test_uuid: Uuid) -> YdbResult<()> {
    let (db_result,): (Option<String>,) = client
        .table_client()
        .retry_transaction(|mut t| async move {
            let res = t
                .query(
                    Query::new(
                        "
                declare $test_uuid AS Uuid;
                select cast($test_uuid AS Utf8) AS db_result",
                    )
                    .with_params(ydb_params! {
                        "$test_uuid" => test_uuid,
                    }),
                )
                .await?;
            let mut row = res.into_only_row()?;

            let value: Option<String> = row.remove_field_by_name("db_result")?.try_into()?;
            Ok((value,))
        })
        .await?;

    assert_eq!(Some(test_uuid.to_string()), db_result);
    Ok(())
}

async fn check_text_as_uuid_serialization(client: &Client, test_uuid: Uuid) -> YdbResult<()> {
    let (db_result,): (Option<Uuid>,) = client
        .table_client()
        .retry_transaction(|mut t| async move {
            let res = t
                .query(
                    Query::new(
                        "
            declare $val AS Text;
            select cast($val AS UUID) AS db_result",
                    )
                    .with_params(ydb_params! {
                        "$val" => test_uuid.to_string(),
                    }),
                )
                .await?;
            let mut row = res.into_only_row()?;

            let value: Option<Uuid> = row.remove_field_by_name("db_result")?.try_into()?;
            Ok((value,))
        })
        .await?;

    assert_eq!(Some(test_uuid), db_result);
    Ok(())
}

#[derive(Debug)]
struct DecimalCase {
    precision: u32,
    scale: u32,
    value: &'static str,
}

const DECIMAL_CASES: &[DecimalCase] = &[
    // Decimal(22, 9) — canonical YDB type
    DecimalCase { precision: 22, scale: 9, value: "0" },
    DecimalCase { precision: 22, scale: 9, value: "1" },
    DecimalCase { precision: 22, scale: 9, value: "-1" },
    DecimalCase { precision: 22, scale: 9, value: "0.000000001" },
    DecimalCase { precision: 22, scale: 9, value: "-0.000000001" },
    DecimalCase { precision: 22, scale: 9, value: "9999999999999.999999999" },
    DecimalCase { precision: 22, scale: 9, value: "-9999999999999.999999999" },

    // Decimal(35, 0) — max precision, integer only
    DecimalCase { precision: 35, scale: 0, value: "0" },
    DecimalCase { precision: 35, scale: 0, value: "1" },
    DecimalCase { precision: 35, scale: 0, value: "-1" },
    DecimalCase { precision: 35, scale: 0,
        value:  "99999999999999999999999999999999999" },
    DecimalCase { precision: 35, scale: 0,
        value: "-99999999999999999999999999999999999" },

    // Decimal(35, 17) — max precision, mixed
    DecimalCase { precision: 35, scale: 17, value: "0" },
    DecimalCase { precision: 35, scale: 17, value: "1" },
    DecimalCase { precision: 35, scale: 17, value: "-1" },
    DecimalCase { precision: 35, scale: 17, value:  "0.00000000000000001" },
    DecimalCase { precision: 35, scale: 17, value: "-0.00000000000000001" },
    DecimalCase { precision: 35, scale: 17,
        value:  "999999999999999999.99999999999999999" },
    DecimalCase { precision: 35, scale: 17,
        value: "-999999999999999999.99999999999999999" },

    // Decimal(35, 35) — max precision, fractional only; |x| < 1
    DecimalCase { precision: 35, scale: 35, value: "0" },
    DecimalCase { precision: 35, scale: 35,
        value:  "0.00000000000000000000000000000000001" },
    DecimalCase { precision: 35, scale: 35,
        value: "-0.00000000000000000000000000000000001" },
    DecimalCase { precision: 35, scale: 35,
        value:  "0.99999999999999999999999999999999999" },
    DecimalCase { precision: 35, scale: 35,
        value: "-0.99999999999999999999999999999999999" },
];

async fn check_decimal_roundtrip(client: &Client, case: &DecimalCase) -> YdbResult<()> {
    let parsed: decimal_rs::Decimal = case
        .value
        .parse()
        .map_err(|e| YdbError::Custom(format!("parse {:?}: {e}", case.value)))?;
    let ydb_dec = YdbDecimal::try_new(parsed, case.precision, case.scale)?;
    let expected_str = ydb_dec.to_string();
    let expected_val: decimal_rs::Decimal = ydb_dec.clone().into();

    let decimal_as_text_query = format!(
        "\
declare $val AS Decimal({p}, {s});
select cast(cast($val AS Decimal({p}, {s})) AS Utf8) AS db_result",
        p = case.precision,
        s = case.scale,
    );
    let (db_text,): (Option<String>,) = client
        .table_client()
        .retry_transaction(|mut t| {
            let q = decimal_as_text_query.clone();
            let d = ydb_dec.clone();
            async move {
                let res = t
                    .query(Query::new(q).with_params(ydb_params! {
                        "$val" => Value::Decimal(d),
                    }))
                    .await?;
                let mut row = res.into_only_row()?;
                let v: Option<String> = row.remove_field_by_name("db_result")?.try_into()?;
                Ok((v,))
            }
        })
        .await?;
    assert_eq!(
        Some(expected_str.clone()),
        db_text,
        "Decimal→Utf8 mismatch for case {case:?}",
    );

    let text_as_decimal_query = format!(
        "\
declare $val AS Text;
select cast($val AS Decimal({p}, {s})) AS db_result",
        p = case.precision,
        s = case.scale,
    );
    let (db_decimal,): (Option<decimal_rs::Decimal>,) = client
        .table_client()
        .retry_transaction(|mut t| {
            let q = text_as_decimal_query.clone();
            let s = case.value.to_string();
            async move {
                let res = t
                    .query(Query::new(q).with_params(ydb_params! {
                        "$val" => s,
                    }))
                    .await?;
                let mut row = res.into_only_row()?;
                let v: Option<decimal_rs::Decimal> =
                    row.remove_field_by_name("db_result")?.try_into()?;
                Ok((v,))
            }
        })
        .await?;
    assert_eq!(
        Some(expected_val),
        db_decimal,
        "Text→Decimal mismatch for case {case:?}",
    );

    Ok(())
}

#[tokio::test]
#[ignore = "needs YDB access"]
async fn test_decimal_serialization() -> YdbResult<()> {
    let client = test_client_builder().client()?;
    client.wait().await?;

    for case in DECIMAL_CASES {
        check_decimal_roundtrip(&client, case).await?;
    }
    Ok(())
}
