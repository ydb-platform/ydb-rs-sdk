use crate::errors::{YdbError, YdbResult};
use std::collections::HashMap;

use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;
use crate::grpc_wrapper::raw_table_service::value::RawColumn;
use std::convert::TryInto;
use std::fmt::Debug;
use std::num::TryFromIntError;
use std::time::{Duration, SystemTime};
use strum::{EnumCount, EnumDiscriminants, EnumIter, IntoStaticStr};

pub(crate) const SECONDS_PER_DAY: u64 = 60 * 60 * 24;

/// A decimal value with explicit YQL type parameters (precision and scale).
///
/// In YQL, `Decimal(p, s)` values with different precision or scale are
/// considered distinct, incompatible types. This wrapper preserves both
/// parameters alongside the numeric value.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct YdbDecimal {
    inner: decimal_rs::Decimal,
    precision: u32,
    scale: u32,
}

impl YdbDecimal {
    /// Creates a new `YdbDecimal` with validation.
    ///
    /// Rescaling up (increasing scale) is allowed via `normalize_to_scale`.
    /// Returns an error if:
    /// - The value cannot be represented within the given precision
    /// - Rescaling would lose significant digits (decrease in scale or precision)
    pub fn try_new(value: decimal_rs::Decimal, precision: u32, scale: u32) -> YdbResult<Self> {
        let scale_i16: i16 = scale
            .try_into()
            .map_err(|_| YdbError::Convert(format!("scale {} does not fit into i16", scale)))?;
        let current_scale = value.scale();
        let adjusted = if current_scale != scale_i16 {
            if scale_i16 < current_scale {
                return Err(YdbError::Convert(format!(
                    "cannot decrease decimal scale from {} to {}",
                    current_scale, scale
                )));
            }
            value.normalize_to_scale(scale_i16)
        } else {
            value
        };

        let digit_count = adjusted.precision() as u32;
        if digit_count > precision {
            return Err(YdbError::Convert(format!(
                "decimal value has {} digits, which exceeds precision {}",
                digit_count, precision
            )));
        }

        Ok(Self {
            inner: adjusted,
            precision,
            scale,
        })
    }

    /// Creates a new `YdbDecimal` without validation.
    ///
    /// The caller must ensure that the value can be represented within
    /// the given precision and scale. Using incorrect parameters may
    /// cause unexpected behavior when sending values to YDB.
    pub(crate) fn new_unchecked(value: decimal_rs::Decimal, precision: u32, scale: u32) -> Self {
        Self {
            inner: value,
            precision,
            scale,
        }
    }

    /// Returns the precision (maximum number of digits).
    pub fn precision(&self) -> u32 {
        self.precision
    }

    /// Returns the scale (number of digits after the decimal point).
    pub fn scale(&self) -> u32 {
        self.scale
    }

    /// Returns a reference to the underlying decimal value.
    pub fn decimal(&self) -> &decimal_rs::Decimal {
        &self.inner
    }
}

impl std::fmt::Display for YdbDecimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, f)
    }
}

impl From<YdbDecimal> for decimal_rs::Decimal {
    fn from(d: YdbDecimal) -> Self {
        d.inner
    }
}

impl From<YdbDecimal> for Value {
    fn from(d: YdbDecimal) -> Self {
        Value::Decimal(d)
    }
}

/// Internal represent database value for send to or received from database.
///
/// That enum will be grow, when add support of new types
///
/// ## Convert from Value to native types
///
/// ### Primitive values
///
/// #### From Value to native types
/// Convert from Value to primitive rust types do by TryFrom trait
/// Try need because Value can contain any DB value and it can' check at compile time.
/// ```rust
/// # use ydb::YdbResult;
/// # fn main()->YdbResult<()> {
/// # use ydb::{Value, YdbError, YdbResult};
///
/// // Simple convert to native type
/// let v: i16 = Value::Int16(123).try_into()?;
/// assert_eq!(123 as i16, v);
///
/// // Simple types can be extended while convert to native type
/// let v: i32 = Value::Int16(123).try_into()?;
/// assert_eq!(123 as i32, v);
/// # return Ok(())
/// # }
/// ```
///
/// #### From native type to Value
/// ```rust
/// # use ydb::YdbResult;
/// # fn main()->YdbResult<()> {
/// # use ydb::{Value, YdbError, YdbResult};
/// // while convert to Value - value internal type exact same as source type - without auto-extended
/// // because real target type doesn't known in compile time
/// let v: Value = (123 as i16).into();
/// assert_eq!(Value::Int16(123), v);
/// # return Ok(())
/// # }
/// ```
///
/// #### Possible native convertions
///
#[derive(Clone, Debug, EnumCount, EnumDiscriminants, PartialEq)]
#[strum_discriminants(vis(pub(crate)))] // private
#[strum_discriminants(derive(IntoStaticStr, EnumIter, Hash))]
#[strum_discriminants(name(ValueDiscriminants))]
#[allow(dead_code)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub enum Value {
    Void,
    Null,
    Bool(bool),
    Int8(i8),
    Uint8(u8),
    Int16(i16),
    Uint16(u16),
    Int32(i32),
    Uint32(u32),
    Int64(i64),
    Uint64(u64),
    Float(f32),
    Double(f64),
    /// Date, day granularity. Wire format: `uint32` days since UNIX epoch.
    /// Range: 1970-01-01 .. 2105-12-31.
    Date(SystemTime),
    /// Datetime, second granularity. Wire format: `uint32` seconds since UNIX epoch.
    /// Range: 1970-01-01T00:00:00Z .. 2106-02-07T06:28:15Z.
    DateTime(SystemTime),
    /// Timestamp, microsecond granularity. Wire format: `uint64` microseconds since UNIX epoch.
    /// Range: 1970-01-01T00:00:00.000000Z .. 2105-12-31T23:59:59.999999Z.
    Timestamp(SystemTime),
    /// Signed duration, microsecond granularity on the YDB wire.
    /// Wire format: `int64` microseconds.
    /// YDB range (exclusive): `(-MAX_TIMESTAMP, MAX_TIMESTAMP)` where
    /// `MAX_TIMESTAMP = 86_400_000_000 * 49_673 = 4_291_747_200_000_000 µs`;
    /// i.e. valid values are `-4_291_747_199_999_999 .. 4_291_747_199_999_999`
    /// inclusive (≈ ±136 years). Source:
    /// `yql/essentials/public/udf/udf_data_type.h`.
    IntervalMicros(SignedInterval),
    /// Signed date, day granularity. Wire format: `int32` days from UNIX epoch.
    /// YDB range: `-53_375_809` .. `53_375_807` days inclusive (year -144169-01-01 ..
    /// 148107-12-31). Source: `yql/essentials/public/udf/udf_data_type.h`.
    /// On Windows, `SystemTime` cannot represent dates before 1601-01-01.
    Date32(SystemTime),
    /// Signed datetime, second granularity. Wire format: `int64` seconds from UNIX epoch.
    /// YDB range: `-4_611_669_897_600` .. `4_611_669_811_199` seconds inclusive
    /// (year -144169-01-01T00:00:00 .. 148107-12-31T23:59:59). Source:
    /// `yql/essentials/public/udf/udf_data_type.h`.
    /// On Windows, `SystemTime` cannot represent dates before 1601-01-01.
    Datetime64(SystemTime),
    /// Signed timestamp, microsecond granularity. Wire format: `int64` microseconds from UNIX epoch.
    /// YDB range: `-4_611_669_897_600_000_000` .. `4_611_669_811_199_999_999` µs inclusive
    /// (year -144169-01-01T00:00:00.000000 .. 148107-12-31T23:59:59.999999). Source:
    /// `yql/essentials/public/udf/udf_data_type.h`.
    /// On Windows, `SystemTime` cannot represent dates before 1601-01-01.
    Timestamp64(SystemTime),
    /// Signed duration, microsecond granularity. Wire format: `int64` microseconds.
    /// YDB range: ±`9_223_339_708_799_999_999` µs (≈ ±292 270 years), derived as
    /// `MAX_TIMESTAMP64 - MIN_TIMESTAMP64`. Source:
    /// `yql/essentials/public/udf/udf_data_type.h`.
    /// Sub-microsecond precision of the inner `Duration` is truncated on the wire.
    Interval64(SignedInterval),

    // It named String at server, but server String type contains binary data https://ydb.tech/docs/en/yql/reference/types/primitive#string
    Bytes(Bytes),

    /// Text data, encoded to valid utf8
    Text(String),
    Yson(Bytes),
    Json(String),
    JsonDocument(String),

    Optional(Box<ValueOptional>),
    List(Box<ValueList>),
    Struct(ValueStruct),

    Decimal(YdbDecimal),
    Uuid(uuid::Uuid),
}

impl Value {
    pub(crate) fn kind_static(&self) -> &'static str {
        let discriminant: ValueDiscriminants = self.into();
        discriminant.into()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ValueStruct {
    pub(crate) fields_name: Vec<String>,
    pub(crate) values: Vec<Value>,
}

impl ValueStruct {
    pub(crate) fn insert(&mut self, name: String, v: Value) {
        self.fields_name.push(name);
        self.values.push(v);
    }

    pub(crate) fn from_fields(fields: Vec<(String, Value)>) -> ValueStruct {
        let fields_len = fields.len();
        let (names, values) = fields.into_iter().fold(
            (
                Vec::with_capacity(fields_len),
                Vec::with_capacity(fields_len),
            ),
            |(mut names, mut values), (name, value)| {
                names.push(name);
                values.push(value);
                (names, values)
            },
        );

        ValueStruct {
            fields_name: names,
            values,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn from_names_and_values(
        fields_name: Vec<String>,
        values: Vec<Value>,
    ) -> YdbResult<Self> {
        if fields_name.len() != values.len() {
            return Err(YdbError::Custom(format!("different len fields_name and values. fields_name len: {}, values len: {}. fields_name: {:?}, values: {:?}", fields_name.len(), values.len(), fields_name, values)));
        };

        Ok(ValueStruct {
            fields_name,
            values,
        })
    }

    pub(crate) fn new() -> Self {
        Self::with_capacity(0)
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        ValueStruct {
            fields_name: Vec::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
        }
    }
}

impl Default for ValueStruct {
    fn default() -> Self {
        Self::new()
    }
}

impl From<ValueStruct> for HashMap<String, Value> {
    fn from(mut from_value: ValueStruct) -> Self {
        let mut map = HashMap::with_capacity(from_value.fields_name.len());
        from_value.values.into_iter().rev().for_each(|val| {
            let key = from_value.fields_name.pop().unwrap();
            map.insert(key, val);
        });
        map
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ValueList {
    pub(crate) t: Value,
    pub(crate) values: Vec<Value>,
}

impl Default for Box<ValueList> {
    fn default() -> Self {
        Box::new(ValueList {
            t: Value::Bool(false),
            values: Vec::default(),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ValueOptional {
    pub(crate) t: Value,
    pub(crate) value: Option<Value>,
}

impl Default for Box<ValueOptional> {
    fn default() -> Self {
        Box::new(ValueOptional {
            t: Value::Bool(false),
            value: None,
        })
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum Sign {
    #[default]
    Plus,
    Minus,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct SignedInterval {
    pub sign: Sign,
    pub duration: Duration,
}

impl SignedInterval {
    pub(crate) fn as_micros(self) -> std::result::Result<i64, TryFromIntError> {
        let micros: i64 = self.duration.as_micros().try_into()?;
        let res = match self.sign {
            Sign::Plus => micros,
            Sign::Minus => -micros,
        };
        Ok(res)
    }

    pub(crate) fn from_micros(micros: i64) -> Self {
        let (sign, micros) = if micros >= 0 {
            (Sign::Plus, micros as u64)
        } else {
            (Sign::Minus, micros.unsigned_abs())
        };

        Self {
            sign,
            duration: Duration::from_micros(micros),
        }
    }
}

/// Encodes a `SystemTime` as seconds from the UNIX epoch with sign. Seconds are
/// floored, so values before the epoch that fall inside a second still round
/// toward minus infinity (same semantics as YDB signed date types).
pub(crate) fn system_time_to_signed_secs(
    t: SystemTime,
) -> std::result::Result<i64, TryFromIntError> {
    match t.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => d.as_secs().try_into(),
        Err(err) => {
            let back = err.duration();
            let mut secs: i64 = back.as_secs().try_into()?;
            if back.subsec_nanos() > 0 {
                secs += 1;
            }
            Ok(-secs)
        }
    }
}

pub(crate) fn signed_secs_to_system_time(secs: i64) -> SystemTime {
    if secs >= 0 {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs as u64)
    } else {
        SystemTime::UNIX_EPOCH - Duration::from_secs(secs.unsigned_abs())
    }
}

pub(crate) fn system_time_to_signed_micros(
    t: SystemTime,
) -> std::result::Result<i64, TryFromIntError> {
    match t.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => d.as_micros().try_into(),
        Err(err) => {
            let back = err.duration();
            let mut micros: i64 = back.as_micros().try_into()?;
            // Sub-microsecond precision rounds toward minus infinity.
            if (back.subsec_nanos() % 1000) > 0 {
                micros += 1;
            }
            Ok(-micros)
        }
    }
}

pub(crate) fn signed_micros_to_system_time(micros: i64) -> SystemTime {
    if micros >= 0 {
        SystemTime::UNIX_EPOCH + Duration::from_micros(micros as u64)
    } else {
        SystemTime::UNIX_EPOCH - Duration::from_micros(micros.unsigned_abs())
    }
}

pub(crate) fn system_time_to_signed_days(
    t: SystemTime,
) -> std::result::Result<i32, TryFromIntError> {
    let secs = system_time_to_signed_secs(t)?;
    let days = secs.div_euclid(SECONDS_PER_DAY as i64);
    days.try_into()
}

pub(crate) fn signed_days_to_system_time(days: i32) -> SystemTime {
    let secs = (days as i64).saturating_mul(SECONDS_PER_DAY as i64);
    signed_secs_to_system_time(secs)
}

impl Value {
    /// list_from create Value from example of item and values
    /// example value must be same type as items in value
    /// it used for describe type in query.
    ///
    /// It can't use one of values because values can be empty.
    /// Example:
    /// ```
    ///  # use ydb::{Value, YdbResult};
    ///  # fn example() -> YdbResult<()>{
    ///  let v = Value::list_from(0.into(), vec![1.into(), 2.into(), 3.into()])?;
    ///  # Ok(())
    /// }
    /// ```
    pub fn list_from(example_value: Value, values: Vec<Value>) -> YdbResult<Self> {
        for (index, value) in values.iter().enumerate() {
            if std::mem::discriminant(&example_value) != std::mem::discriminant(value) {
                return Err(YdbError::Custom(format!("failed list_from: type and value has different enum-types. index: {index}, type: '{example_value:?}', value: '{value:?}'")));
            }
        }

        if let Value::Struct(example_value_struct) = &example_value {
            for (i, value) in values.iter().enumerate() {
                if let Value::Struct(value_struct) = &value {
                    if value_struct.fields_name != example_value_struct.fields_name {
                        return Err(YdbError::Custom(format!(
                                "failed list_from: fields of value struct with index `{i}`: '{:?}' is not equal to fields of example value struct: '{:?}'",
                                value_struct.fields_name, example_value_struct.fields_name
                            )));
                    }
                }
            }
        }

        Ok(Value::List(Box::new(ValueList {
            t: example_value,
            values,
        })))
    }

    pub(crate) fn optional_from(t: Value, value: Option<Value>) -> YdbResult<Self> {
        if let Some(value) = &value {
            if std::mem::discriminant(&t) != std::mem::discriminant(value) {
                return Err(YdbError::Custom(format!("failed optional_from: type and value has different enum-types. type: '{t:?}', value: '{value:?}'")));
            }
        }
        Ok(Value::Optional(Box::new(ValueOptional { t, value })))
    }

    /// Create struct value from fields in form name, value.
    ///
    /// Example:
    /// ```
    /// # use ydb::Value;
    /// let v = Value::struct_from_fields(vec![
    ///     ("id".to_string(), 1.into()),
    ///     ("value".to_string(), "test-value".into()),
    /// ]);
    /// ```
    pub fn struct_from_fields(fields: Vec<(String, Value)>) -> Value {
        Value::Struct(ValueStruct::from_fields(fields))
    }

    ///  Return true if the Value is optional
    pub fn is_optional(&self) -> bool {
        matches!(self, Self::Optional(_))
    }

    /// present current value as Option
    /// if value is Optional - return inner unwrapper value.
    /// else - return self, wrapped to Option.
    pub fn to_option(self) -> Option<Value> {
        match self {
            Value::Optional(inner_box) => inner_box.value,
            other => Some(other),
        }
    }

    #[cfg(test)]
    pub(crate) fn examples_for_test() -> Vec<Value> {
        use std::{collections::HashSet, ops::Add};

        // test zero, one, minimum and maximum values
        macro_rules! num_tests {
            ($values:ident, $en_name:path, $type_name:ty) => {
                $values.push($en_name(0 as $type_name));
                $values.push($en_name(1 as $type_name));
                $values.push($en_name(<$type_name>::MIN));
                $values.push($en_name(<$type_name>::MAX));
            };
        }

        let mut values = vec![
            Value::Null,
            Value::Bool(false),
            Value::Bool(true),
            Value::Bytes(Bytes::from("asd".to_string())),
            Value::Text("asd".into()),
            Value::Text("фыв".into()),
            Value::Json("{}".into()),
            Value::JsonDocument("{}".into()),
            Value::Yson("1;2;3;".into()),
            Value::Decimal(
                YdbDecimal::try_new(
                    "123456789.987654321"
                        .parse::<decimal_rs::Decimal>()
                        .unwrap(),
                    22,
                    9,
                )
                .unwrap(),
            ),
            Value::Uuid(uuid::Uuid::now_v7()),
            Value::Uuid(uuid::Uuid::new_v4()),
        ];

        num_tests!(values, Value::Int8, i8);
        num_tests!(values, Value::Uint8, u8);
        num_tests!(values, Value::Int16, i16);
        num_tests!(values, Value::Uint16, u16);
        num_tests!(values, Value::Int32, i32);
        num_tests!(values, Value::Uint32, u32);
        num_tests!(values, Value::Int64, i64);
        num_tests!(values, Value::Uint64, u64);
        num_tests!(values, Value::Float, f32);
        num_tests!(values, Value::Double, f64);

        values.push(Value::Void);

        values.push(Value::Date(
            SystemTime::UNIX_EPOCH.add(std::time::Duration::from_secs(1633996800)),
        )); //Tue Oct 12 00:00:00 UTC 2021
        values.push(Value::DateTime(
            SystemTime::UNIX_EPOCH.add(std::time::Duration::from_secs(1634000523)),
        )); //Tue Oct 12 01:02:03 UTC 2021

        values.push(Value::Timestamp(
            SystemTime::UNIX_EPOCH.add(std::time::Duration::from_micros(16340005230000123)),
        )); //Tue Oct 12 00:00:00.000123 UTC 2021

        values.push(Value::IntervalMicros(SignedInterval {
            sign: Sign::Plus,
            duration: Duration::from_secs(1),
        })); // 1 second interval

        values.push(Value::IntervalMicros(SignedInterval {
            sign: Sign::Minus,
            duration: Duration::from_secs(1),
        })); // -1 second interval

        // Date32/Datetime64/Timestamp64 can represent pre-epoch times too.
        // Include a post-epoch and a pre-epoch sample to exercise sign paths.
        values.push(Value::Date32(SystemTime::UNIX_EPOCH));
        values.push(Value::Date32(
            SystemTime::UNIX_EPOCH - Duration::from_secs(SECONDS_PER_DAY),
        ));
        values.push(Value::Datetime64(SystemTime::UNIX_EPOCH));
        values.push(Value::Datetime64(
            SystemTime::UNIX_EPOCH - Duration::from_secs(1),
        ));
        values.push(Value::Timestamp64(SystemTime::UNIX_EPOCH));
        values.push(Value::Timestamp64(
            SystemTime::UNIX_EPOCH - Duration::from_micros(1),
        ));
        values.push(Value::Interval64(SignedInterval {
            sign: Sign::Plus,
            duration: Duration::from_micros(1),
        }));
        values.push(Value::Interval64(SignedInterval {
            sign: Sign::Minus,
            duration: Duration::from_micros(1),
        }));

        values.push(Value::optional_from(Value::Int8(0), None).unwrap());
        values.push(Value::optional_from(Value::Int8(0), Some(Value::Int8(1))).unwrap());

        values.push(
            Value::list_from(
                Value::Int8(0),
                vec![Value::Int8(1), Value::Int8(2), Value::Int8(3)],
            )
            .unwrap(),
        );

        values.push(Value::Struct(ValueStruct {
            fields_name: vec!["a".into(), "b".into()],
            values: vec![
                Value::Int32(1),
                Value::list_from(
                    Value::Int32(0),
                    vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)],
                )
                .unwrap(),
            ],
        }));

        let mut discriminants = HashSet::new();
        for item in values.iter() {
            discriminants.insert(std::mem::discriminant(item));
        }
        assert_eq!(discriminants.len(), Value::COUNT);

        values
    }
}

#[derive(Debug)]
pub(crate) struct Column {
    #[allow(dead_code)]
    pub(crate) name: String,
    pub(crate) v_type: RawType,
}

impl TryFrom<RawColumn> for Column {
    type Error = YdbError;

    fn try_from(value: RawColumn) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name,
            v_type: value.column_type,
        })
    }
}

// Container fot bytes for prevent conflict Vec<u8> - List of values u8 or String type (bytes)
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Bytes {
    vec: Vec<u8>,
}

impl From<Vec<u8>> for Bytes {
    fn from(vec: Vec<u8>) -> Self {
        Bytes { vec }
    }
}

impl From<Bytes> for Vec<u8> {
    fn from(val: Bytes) -> Self {
        val.vec
    }
}

impl From<String> for Bytes {
    fn from(val: String) -> Self {
        Self { vec: val.into() }
    }
}

impl From<&str> for Bytes {
    fn from(val: &str) -> Self {
        Self { vec: val.into() }
    }
}
