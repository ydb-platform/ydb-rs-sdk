use crate::client::Client;
use crate::errors::YdbError;
use crate::types::{Bytes, YdbDecimal};
use crate::{
    test_helpers::test_client_builder, ydb_params, Query, Sign, SignedInterval, Value, YdbResult,
};
use std::time::{Duration, SystemTime};
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

/// One round-trip test case for a YQL primitive type.
///
/// Adding coverage for a new type/value is a single row in `type_cases()`.
#[derive(Debug, Clone)]
struct TypeCase {
    /// YQL type name as used in `DECLARE $x AS <yql_type>`. May contain
    /// parameters (e.g. `"Decimal(22, 9)"`).
    yql_type: String,
    /// Value sent as parameter; also expected back from the server.
    value: Value,
    /// Expected canonical text representation as produced by
    /// `CAST(<value> AS Utf8)` on the server, and accepted by
    /// `CAST(<text> AS <yql_type>)` on the server.
    text: String,
}

impl TypeCase {
    fn new(yql_type: impl Into<String>, value: Value, text: impl Into<String>) -> Self {
        Self {
            yql_type: yql_type.into(),
            value,
            text: text.into(),
        }
    }
}

fn type_cases() -> Vec<TypeCase> {
    let mut cases: Vec<TypeCase> = Vec::new();

    // ---- Bool ----
    cases.push(TypeCase::new("Bool", Value::Bool(true), "true"));
    cases.push(TypeCase::new("Bool", Value::Bool(false), "false"));

    // ---- Signed integers ----
    for (v, s) in [
        (0i8, "0"),
        (1, "1"),
        (-1, "-1"),
        (i8::MAX, "127"),
        (i8::MIN, "-128"),
    ] {
        cases.push(TypeCase::new("Int8", Value::Int8(v), s));
    }
    for (v, s) in [
        (0i16, "0"),
        (1, "1"),
        (-1, "-1"),
        (i16::MAX, "32767"),
        (i16::MIN, "-32768"),
    ] {
        cases.push(TypeCase::new("Int16", Value::Int16(v), s));
    }
    for (v, s) in [
        (0i32, "0"),
        (1, "1"),
        (-1, "-1"),
        (i32::MAX, "2147483647"),
        (i32::MIN, "-2147483648"),
    ] {
        cases.push(TypeCase::new("Int32", Value::Int32(v), s));
    }
    for (v, s) in [
        (0i64, "0"),
        (1, "1"),
        (-1, "-1"),
        (i64::MAX, "9223372036854775807"),
        (i64::MIN, "-9223372036854775808"),
    ] {
        cases.push(TypeCase::new("Int64", Value::Int64(v), s));
    }

    // ---- Unsigned integers ----
    for (v, s) in [(0u8, "0"), (1, "1"), (u8::MAX, "255")] {
        cases.push(TypeCase::new("Uint8", Value::Uint8(v), s));
    }
    for (v, s) in [(0u16, "0"), (1, "1"), (u16::MAX, "65535")] {
        cases.push(TypeCase::new("Uint16", Value::Uint16(v), s));
    }
    for (v, s) in [(0u32, "0"), (1, "1"), (u32::MAX, "4294967295")] {
        cases.push(TypeCase::new("Uint32", Value::Uint32(v), s));
    }
    for (v, s) in [(0u64, "0"), (1, "1"), (u64::MAX, "18446744073709551615")] {
        cases.push(TypeCase::new("Uint64", Value::Uint64(v), s));
    }

    // ---- Floating point ----
    // Ordinary values, IEEE-754 magnitude edges, and special values
    // (±0, ±inf, NaN). NaN equality is handled by `value_roundtrip_eq`.
    for (v, s) in [
        (0.0f32, "0"),
        (-0.0, "-0"),
        (1.0, "1"),
        (-1.0, "-1"),
        (1.5, "1.5"),
        (-1.5, "-1.5"),
        (f32::MIN_POSITIVE, "1.1754944e-38"),
        (-f32::MIN_POSITIVE, "-1.1754944e-38"),
        (f32::MAX, "3.4028235e+38"),
        (f32::MIN, "-3.4028235e+38"),
        (f32::INFINITY, "inf"),
        (f32::NEG_INFINITY, "-inf"),
        (f32::NAN, "nan"),
    ] {
        cases.push(TypeCase::new("Float", Value::Float(v), s));
    }
    for (v, s) in [
        (0.0f64, "0"),
        (-0.0, "-0"),
        (1.0, "1"),
        (-1.0, "-1"),
        (1.5, "1.5"),
        (-1.5, "-1.5"),
        (f64::MIN_POSITIVE, "2.2250738585072014e-308"),
        (-f64::MIN_POSITIVE, "-2.2250738585072014e-308"),
        (f64::MAX, "1.7976931348623157e+308"),
        (f64::MIN, "-1.7976931348623157e+308"),
        (f64::INFINITY, "inf"),
        (f64::NEG_INFINITY, "-inf"),
        (f64::NAN, "nan"),
    ] {
        cases.push(TypeCase::new("Double", Value::Double(v), s));
    }

    // ---- Date (day granularity); YDB range: 1970-01-01 .. 2105-12-31 ----
    // 49672 days from epoch = 2105-12-31
    const DAYS_TO_2105_12_31: u64 = 49_672;
    cases.push(TypeCase::new(
        "Date",
        Value::Date(SystemTime::UNIX_EPOCH),
        "1970-01-01",
    ));
    cases.push(TypeCase::new(
        "Date",
        Value::Date(SystemTime::UNIX_EPOCH + Duration::from_secs(19_737 * 86_400)),
        "2024-01-15",
    ));
    cases.push(TypeCase::new(
        "Date",
        Value::Date(SystemTime::UNIX_EPOCH + Duration::from_secs(DAYS_TO_2105_12_31 * 86_400)),
        "2105-12-31",
    ));

    // ---- Datetime (second granularity); YDB range: 1970-01-01T00:00:00Z .. 2105-12-31T23:59:59Z ----
    const SECS_TO_2105_12_31_END: u64 = DAYS_TO_2105_12_31 * 86_400 + 23 * 3600 + 59 * 60 + 59;
    cases.push(TypeCase::new(
        "Datetime",
        Value::DateTime(SystemTime::UNIX_EPOCH),
        "1970-01-01T00:00:00Z",
    ));
    cases.push(TypeCase::new(
        "Datetime",
        Value::DateTime(SystemTime::UNIX_EPOCH + Duration::from_secs(1_705_320_645)),
        "2024-01-15T12:10:45Z",
    ));
    cases.push(TypeCase::new(
        "Datetime",
        Value::DateTime(SystemTime::UNIX_EPOCH + Duration::from_secs(SECS_TO_2105_12_31_END)),
        "2105-12-31T23:59:59Z",
    ));

    // ---- Timestamp (microsecond granularity); YDB range: 1970-01-01T00:00:00Z .. 2105-12-31T23:59:59.999999Z ----
    cases.push(TypeCase::new(
        "Timestamp",
        Value::Timestamp(SystemTime::UNIX_EPOCH),
        "1970-01-01T00:00:00Z",
    ));
    cases.push(TypeCase::new(
        "Timestamp",
        Value::Timestamp(SystemTime::UNIX_EPOCH + Duration::from_micros(1)),
        "1970-01-01T00:00:00.000001Z",
    ));
    cases.push(TypeCase::new(
        "Timestamp",
        Value::Timestamp(SystemTime::UNIX_EPOCH + Duration::from_micros(1_705_320_645_123_456)),
        "2024-01-15T12:10:45.123456Z",
    ));
    cases.push(TypeCase::new(
        "Timestamp",
        Value::Timestamp(
            SystemTime::UNIX_EPOCH
                + Duration::from_micros(SECS_TO_2105_12_31_END * 1_000_000 + 999_999),
        ),
        "2105-12-31T23:59:59.999999Z",
    ));

    // Interval is intentionally NOT covered here: the SDK currently encodes
    // SignedInterval via `as_nanos()`, but the YDB wire format uses
    // microseconds, so the value the server sees is 1000× the intended one.
    // Adding round-trip cases here would either fail or document the bug.

    // ---- Date32 (signed days from epoch) ----
    // YDB Date32 range per `yql/essentials/public/udf/udf_data_type.h`:
    //   MIN_DATE32 = -53_375_809 (year -144169-01-01)
    //   MAX_DATE32 =  53_375_807 (year 148107-12-31)
    // Cases: epoch, ±1 day, 2024-01-15, 0001-01-01 / 9999-12-31 (4-digit year edge),
    // and the server extreme bounds.
    const DATE32_EPOCH_TO_9999_12_31_SECS: u64 = 2_932_896 * 86_400;
    const DATE32_0001_01_01_TO_EPOCH_SECS: u64 = 719_162 * 86_400;
    const DATE32_MIN_DAYS_TO_EPOCH_SECS: u64 = 53_375_809 * 86_400;
    const DATE32_EPOCH_TO_MAX_DAYS_SECS: u64 = 53_375_807 * 86_400;
    cases.push(TypeCase::new(
        "Date32",
        Value::Date32(SystemTime::UNIX_EPOCH),
        "1970-01-01",
    ));
    cases.push(TypeCase::new(
        "Date32",
        Value::Date32(SystemTime::UNIX_EPOCH + Duration::from_secs(19_737 * 86_400)),
        "2024-01-15",
    ));
    cases.push(TypeCase::new(
        "Date32",
        Value::Date32(SystemTime::UNIX_EPOCH - Duration::from_secs(86_400)),
        "1969-12-31",
    ));
    cases.push(TypeCase::new(
        "Date32",
        Value::Date32(SystemTime::UNIX_EPOCH + Duration::from_secs(86_400)),
        "1970-01-02",
    ));
    cases.push(TypeCase::new(
        "Date32",
        Value::Date32(
            SystemTime::UNIX_EPOCH + Duration::from_secs(DATE32_EPOCH_TO_9999_12_31_SECS),
        ),
        "9999-12-31",
    ));
    cases.push(TypeCase::new(
        "Date32",
        Value::Date32(
            SystemTime::UNIX_EPOCH - Duration::from_secs(DATE32_0001_01_01_TO_EPOCH_SECS),
        ),
        "1-01-01",
    ));
    // Server extreme bounds (MIN_DATE32 / MAX_DATE32)
    cases.push(TypeCase::new(
        "Date32",
        Value::Date32(SystemTime::UNIX_EPOCH - Duration::from_secs(DATE32_MIN_DAYS_TO_EPOCH_SECS)),
        "-144169-01-01",
    ));
    cases.push(TypeCase::new(
        "Date32",
        Value::Date32(SystemTime::UNIX_EPOCH + Duration::from_secs(DATE32_EPOCH_TO_MAX_DAYS_SECS)),
        "148107-12-31",
    ));

    // ---- Datetime64 (signed seconds from epoch) ----
    // YDB Datetime64 range per `udf_data_type.h`:
    //   MIN_DATETIME64 = -4_611_669_897_600 (year -144169-01-01T00:00:00)
    //   MAX_DATETIME64 =  4_611_669_811_199 (year 148107-12-31T23:59:59)
    const DATETIME64_EPOCH_TO_9999_12_31_END_SECS: u64 =
        DATE32_EPOCH_TO_9999_12_31_SECS + 23 * 3600 + 59 * 60 + 59;
    const DATETIME64_MAX: u64 = 4_611_669_811_199;
    const DATETIME64_MIN_ABS: u64 = 4_611_669_897_600;
    cases.push(TypeCase::new(
        "Datetime64",
        Value::Datetime64(SystemTime::UNIX_EPOCH),
        "1970-01-01T00:00:00Z",
    ));
    cases.push(TypeCase::new(
        "Datetime64",
        Value::Datetime64(SystemTime::UNIX_EPOCH + Duration::from_secs(1_705_320_645)),
        "2024-01-15T12:10:45Z",
    ));
    cases.push(TypeCase::new(
        "Datetime64",
        Value::Datetime64(SystemTime::UNIX_EPOCH - Duration::from_secs(1)),
        "1969-12-31T23:59:59Z",
    ));
    cases.push(TypeCase::new(
        "Datetime64",
        Value::Datetime64(SystemTime::UNIX_EPOCH + Duration::from_secs(1)),
        "1970-01-01T00:00:01Z",
    ));
    cases.push(TypeCase::new(
        "Datetime64",
        Value::Datetime64(
            SystemTime::UNIX_EPOCH + Duration::from_secs(DATETIME64_EPOCH_TO_9999_12_31_END_SECS),
        ),
        "9999-12-31T23:59:59Z",
    ));
    cases.push(TypeCase::new(
        "Datetime64",
        Value::Datetime64(
            SystemTime::UNIX_EPOCH - Duration::from_secs(DATE32_0001_01_01_TO_EPOCH_SECS),
        ),
        "1-01-01T00:00:00Z",
    ));
    // Server extreme bounds (MIN_DATETIME64 / MAX_DATETIME64)
    cases.push(TypeCase::new(
        "Datetime64",
        Value::Datetime64(SystemTime::UNIX_EPOCH - Duration::from_secs(DATETIME64_MIN_ABS)),
        "-144169-01-01T00:00:00Z",
    ));
    cases.push(TypeCase::new(
        "Datetime64",
        Value::Datetime64(SystemTime::UNIX_EPOCH + Duration::from_secs(DATETIME64_MAX)),
        "148107-12-31T23:59:59Z",
    ));

    // ---- Timestamp64 (signed microseconds from epoch) ----
    // YDB Timestamp64 range per `udf_data_type.h`:
    //   MIN_TIMESTAMP64 = -4_611_669_897_600_000_000 (year -144169-01-01T00:00:00.000000)
    //   MAX_TIMESTAMP64 =  4_611_669_811_199_999_999 (year 148107-12-31T23:59:59.999999)
    const TIMESTAMP64_EPOCH_TO_9999_12_31_END_MICROS: u64 =
        DATETIME64_EPOCH_TO_9999_12_31_END_SECS * 1_000_000 + 999_999;
    const TIMESTAMP64_0001_01_01_TO_EPOCH_MICROS: u64 = DATE32_0001_01_01_TO_EPOCH_SECS * 1_000_000;
    const TIMESTAMP64_MAX: u64 = 4_611_669_811_199_999_999;
    const TIMESTAMP64_MIN_ABS: u64 = 4_611_669_897_600_000_000;
    cases.push(TypeCase::new(
        "Timestamp64",
        Value::Timestamp64(SystemTime::UNIX_EPOCH),
        "1970-01-01T00:00:00Z",
    ));
    cases.push(TypeCase::new(
        "Timestamp64",
        Value::Timestamp64(SystemTime::UNIX_EPOCH + Duration::from_micros(1)),
        "1970-01-01T00:00:00.000001Z",
    ));
    cases.push(TypeCase::new(
        "Timestamp64",
        Value::Timestamp64(SystemTime::UNIX_EPOCH + Duration::from_micros(1_705_320_645_123_456)),
        "2024-01-15T12:10:45.123456Z",
    ));
    cases.push(TypeCase::new(
        "Timestamp64",
        Value::Timestamp64(SystemTime::UNIX_EPOCH - Duration::from_micros(1)),
        "1969-12-31T23:59:59.999999Z",
    ));
    cases.push(TypeCase::new(
        "Timestamp64",
        Value::Timestamp64(
            SystemTime::UNIX_EPOCH
                + Duration::from_micros(TIMESTAMP64_EPOCH_TO_9999_12_31_END_MICROS),
        ),
        "9999-12-31T23:59:59.999999Z",
    ));
    cases.push(TypeCase::new(
        "Timestamp64",
        Value::Timestamp64(
            SystemTime::UNIX_EPOCH - Duration::from_micros(TIMESTAMP64_0001_01_01_TO_EPOCH_MICROS),
        ),
        "1-01-01T00:00:00Z",
    ));
    // Server extreme bounds (MIN_TIMESTAMP64 / MAX_TIMESTAMP64)
    cases.push(TypeCase::new(
        "Timestamp64",
        Value::Timestamp64(SystemTime::UNIX_EPOCH - Duration::from_micros(TIMESTAMP64_MIN_ABS)),
        "-144169-01-01T00:00:00Z",
    ));
    cases.push(TypeCase::new(
        "Timestamp64",
        Value::Timestamp64(SystemTime::UNIX_EPOCH + Duration::from_micros(TIMESTAMP64_MAX)),
        "148107-12-31T23:59:59.999999Z",
    ));

    // ---- Interval64 (signed microsecond duration) ----
    // YDB Interval64 range per `udf_data_type.h`:
    //   MAX_INTERVAL64 = MAX_TIMESTAMP64 - MIN_TIMESTAMP64 = 9_223_339_708_799_999_999 µs
    //   (≈ ±292 270 years). Range: [-MAX_INTERVAL64, +MAX_INTERVAL64] inclusive.
    const INTERVAL64_MAX: u64 = 9_223_339_708_799_999_999;
    // YDB CAST(Interval64 AS Utf8) produces an ISO-8601 duration string.
    // Values taken from the Go SDK integration tests are used as a known-good
    // reference. Boundaries: ±1 microsecond, ±(1 second − 1 microsecond),
    // and ±(1 day − 1 microsecond) — the largest sub-day interval fitting
    // a predictable H/M/S format.
    cases.push(TypeCase::new(
        "Interval64",
        Value::Interval64(SignedInterval {
            sign: Sign::Plus,
            duration: Duration::from_micros(19_845_000_123),
        }),
        "PT5H30M45.000123S",
    ));
    cases.push(TypeCase::new(
        "Interval64",
        Value::Interval64(SignedInterval {
            sign: Sign::Minus,
            duration: Duration::from_micros(8_130_000_000),
        }),
        "-PT2H15M30S",
    ));
    cases.push(TypeCase::new(
        "Interval64",
        Value::Interval64(SignedInterval {
            sign: Sign::Plus,
            duration: Duration::from_micros(27_930_999_999),
        }),
        "PT7H45M30.999999S",
    ));
    cases.push(TypeCase::new(
        "Interval64",
        Value::Interval64(SignedInterval {
            sign: Sign::Plus,
            duration: Duration::from_micros(1),
        }),
        "PT0.000001S",
    ));
    cases.push(TypeCase::new(
        "Interval64",
        Value::Interval64(SignedInterval {
            sign: Sign::Minus,
            duration: Duration::from_micros(1),
        }),
        "-PT0.000001S",
    ));
    cases.push(TypeCase::new(
        "Interval64",
        Value::Interval64(SignedInterval {
            sign: Sign::Plus,
            duration: Duration::from_micros(999_999),
        }),
        "PT0.999999S",
    ));
    cases.push(TypeCase::new(
        "Interval64",
        Value::Interval64(SignedInterval {
            sign: Sign::Minus,
            duration: Duration::from_micros(999_999),
        }),
        "-PT0.999999S",
    ));
    cases.push(TypeCase::new(
        "Interval64",
        Value::Interval64(SignedInterval {
            sign: Sign::Plus,
            duration: Duration::from_micros(86_399_999_999),
        }),
        "PT23H59M59.999999S",
    ));
    cases.push(TypeCase::new(
        "Interval64",
        Value::Interval64(SignedInterval {
            sign: Sign::Minus,
            duration: Duration::from_micros(86_399_999_999),
        }),
        "-PT23H59M59.999999S",
    ));
    // Server extreme bounds (±MAX_INTERVAL64). Expected text is derived from the
    // ISO-8601 duration format. Fill in once the server returns the canonical form.
    cases.push(TypeCase::new(
        "Interval64",
        Value::Interval64(SignedInterval {
            sign: Sign::Plus,
            duration: Duration::from_micros(INTERVAL64_MAX),
        }),
        "P106751616DT23H59M59.999999S",
    ));
    cases.push(TypeCase::new(
        "Interval64",
        Value::Interval64(SignedInterval {
            sign: Sign::Minus,
            duration: Duration::from_micros(INTERVAL64_MAX),
        }),
        "-P106751616DT23H59M59.999999S",
    ));

    // ---- Utf8 (Text) / String (Bytes) ----
    // Empty, ASCII, multi-byte UTF-8, special chars (newline/tab/quote).
    for s in ["", "hello", "привет", "a\nb", "a\tb", "a\"b"] {
        cases.push(TypeCase::new("Utf8", Value::Text(s.into()), s));
        cases.push(TypeCase::new("String", Value::Bytes(Bytes::from(s)), s));
    }

    // ---- Json (preserves text as-is) ----
    for s in [
        "null",
        "true",
        "false",
        "0",
        "-1",
        r#""hello""#,
        "[]",
        "[1,2,3]",
        "{}",
        r#"{"a":1}"#,
        r#"{"a":[1,2,{"b":"c"}]}"#,
    ] {
        cases.push(TypeCase::new("Json", Value::Json(s.into()), s));
    }

    // ---- JsonDocument (server canonicalizes; we use already-canonical form) ----
    for s in [
        "null",
        "true",
        "false",
        "0",
        "-1",
        r#""hello""#,
        "[]",
        "[1,2,3]",
        "{}",
        r#"{"a":1}"#,
        r#"{"a":[1,2,{"b":"c"}]}"#,
    ] {
        cases.push(TypeCase::new(
            "JsonDocument",
            Value::JsonDocument(s.into()),
            s,
        ));
    }

    // Yson is intentionally NOT covered here: YDB does not implement
    // `CAST(Yson AS Utf8)` / `CAST(Utf8 AS Yson)` (the engine reports
    // "Cannot cast type Yson into Utf8"), so the Utf8-bridged round-trip
    // pattern used by this test does not apply. Yson↔Text conversions
    // would have to go through `Yson::SerializeText` / `Yson::Parse`.

    // ---- Uuid ----
    // Fixed values only — random UUIDs would make test failures non-reproducible.
    // Includes the magnitude edges (nil, max) and synthetic v4-/v7-shaped values
    // that match what `Uuid::new_v4()` / `Uuid::now_v7()` would produce structurally.
    for u in [
        Uuid::nil(),                                         // all-zero
        Uuid::from_u128(u128::MAX),                          // all-ones
        Uuid::from_u128(0x1234567890abcdef1234567890abcdef), // arbitrary fixed
        Uuid::parse_str("12345678-1234-4abc-89ab-1234567890ab").expect("v4-shaped uuid"), // version=4 nibble, variant=10
        Uuid::parse_str("12345678-1234-7abc-89ab-1234567890ab").expect("v7-shaped uuid"), // version=7 nibble, variant=10
    ] {
        let text = u.to_string();
        cases.push(TypeCase::new("Uuid", Value::Uuid(u), text));
    }

    // ---- Decimal(p, s) ----
    let decimal_specs: &[(u32, u32, &[&str])] = &[
        // canonical YDB Decimal
        (
            22,
            9,
            &[
                "0",
                "1",
                "-1",
                "0.000000001",
                "-0.000000001",
                "9999999999999.999999999",
                "-9999999999999.999999999",
            ],
        ),
        // max precision, integer only
        (
            35,
            0,
            &[
                "0",
                "1",
                "-1",
                "99999999999999999999999999999999999",
                "-99999999999999999999999999999999999",
            ],
        ),
        // max precision, mixed
        (
            35,
            17,
            &[
                "0",
                "1",
                "-1",
                "0.00000000000000001",
                "-0.00000000000000001",
                "999999999999999999.99999999999999999",
                "-999999999999999999.99999999999999999",
            ],
        ),
        // max precision, fractional only; |x| < 1
        (
            35,
            35,
            &[
                "0",
                "0.00000000000000000000000000000000001",
                "-0.00000000000000000000000000000000001",
                "0.99999999999999999999999999999999999",
                "-0.99999999999999999999999999999999999",
            ],
        ),
    ];
    for (precision, scale, values) in decimal_specs {
        for raw in *values {
            let parsed: decimal_rs::Decimal = raw
                .parse()
                .unwrap_or_else(|e| panic!("parse decimal {raw:?}: {e}"));
            let ydb_dec = YdbDecimal::try_new(parsed, *precision, *scale).unwrap_or_else(|e| {
                panic!("YdbDecimal::try_new({raw:?}, {precision}, {scale}): {e}")
            });
            let text = ydb_dec.to_string();
            let yql_type = format!("Decimal({precision}, {scale})");
            cases.push(TypeCase::new(yql_type, Value::Decimal(ydb_dec), text));
        }
    }

    cases
}

/// Verifies three independent round-trips for a single case:
///
/// 1. `Value → server (no cast) → Value` — wire-format round-trip.
/// 2. `Value → CAST AS Utf8 → string` — server sees the value as the
///    expected text.
/// 3. `Text → CAST AS yql_type → Value` — server parses the text into
///    the expected value.
async fn check_type_roundtrip(client: &Client, case: &TypeCase) -> YdbResult<()> {
    // --- 1) Value → server (no cast) → Value ---
    let q1 = format!(
        "\
declare $val AS {t};
select $val AS db_result",
        t = case.yql_type,
    );
    let recv_passthrough = client
        .table_client()
        .retry_transaction(|mut t| {
            let q = q1.clone();
            let v = case.value.clone();
            async move {
                let res = t
                    .query(Query::new(q).with_params(ydb_params! {
                        "$val" => v,
                    }))
                    .await?;
                let mut row = res.into_only_row()?;
                Ok(row.remove_field_by_name("db_result")?)
            }
        })
        .await?;
    let inner_passthrough = unwrap_optional(recv_passthrough, &case.yql_type, &case.text)?;
    assert!(
        value_roundtrip_eq(&case.value, &inner_passthrough),
        "Value→Value mismatch for case {case:?}: got {inner_passthrough:?}",
    );

    // --- 2) Value → CAST AS Utf8 → string ---
    let q2 = format!(
        "\
declare $val AS {t};
select cast(cast($val AS {t}) AS Utf8) AS db_result",
        t = case.yql_type,
    );
    let (db_text,): (Option<String>,) = client
        .table_client()
        .retry_transaction(|mut t| {
            let q = q2.clone();
            let v = case.value.clone();
            async move {
                let res = t
                    .query(Query::new(q).with_params(ydb_params! {
                        "$val" => v,
                    }))
                    .await?;
                let mut row = res.into_only_row()?;
                let v: Option<String> = row.remove_field_by_name("db_result")?.try_into()?;
                Ok((v,))
            }
        })
        .await?;
    assert_eq!(
        Some(case.text.clone()),
        db_text,
        "Value→Utf8 mismatch for case {case:?}",
    );

    // --- 3) Text → CAST AS yql_type → Value ---
    let q3 = format!(
        "\
declare $val AS Text;
select cast($val AS {t}) AS db_result",
        t = case.yql_type,
    );
    let recv_parsed = client
        .table_client()
        .retry_transaction(|mut t| {
            let q = q3.clone();
            let s = case.text.clone();
            async move {
                let res = t
                    .query(Query::new(q).with_params(ydb_params! {
                        "$val" => s,
                    }))
                    .await?;
                let mut row = res.into_only_row()?;
                Ok(row.remove_field_by_name("db_result")?)
            }
        })
        .await?;
    let inner_parsed = unwrap_optional(recv_parsed, &case.yql_type, &case.text)?;
    assert!(
        value_roundtrip_eq(&case.value, &inner_parsed),
        "Text→{} mismatch for case {case:?}: got {inner_parsed:?}",
        case.yql_type,
    );

    Ok(())
}

fn unwrap_optional(v: Value, yql_type: &str, text: &str) -> YdbResult<Value> {
    match v {
        Value::Optional(opt) => opt
            .value
            .ok_or_else(|| YdbError::Custom(format!("got NULL for {yql_type} from text {text:?}"))),
        other => Ok(other),
    }
}

/// Value equality that treats NaN as equal to NaN (PartialEq says they aren't).
fn value_roundtrip_eq(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Float(e), Value::Float(a)) if e.is_nan() => a.is_nan(),
        (Value::Double(e), Value::Double(a)) if e.is_nan() => a.is_nan(),
        _ => expected == actual,
    }
}

#[tokio::test]
#[ignore = "needs YDB access"]
async fn test_type_serialization() -> YdbResult<()> {
    let client = test_client_builder().client()?;
    client.wait().await?;

    for case in type_cases() {
        check_type_roundtrip(&client, &case).await?;
    }
    Ok(())
}
