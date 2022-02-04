use crate::errors::YdbError;
use crate::types::{YdbOptional, YdbValue};
use std::any::type_name;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

macro_rules! simple_convert {
    ($native_type:ty, $ydb_value_kind_first:path $(,$ydb_value_kind:path)* $(,)?) => {
        impl TryFrom<YdbValue> for $native_type {
            type Error = YdbError;

            fn try_from(value: YdbValue) -> Result<Self, Self::Error> {
                match value {
                    $ydb_value_kind_first(val) => Ok(val.into()),
                    $($ydb_value_kind(val) => Ok(val.into()),)*
                    value => Err(YdbError::Convert(format!(
                        "failed to convert from {} to {}",
                        value.kind_static(),
                        type_name::<Self>(),
                    ))),
                }
            }
        }

        impl From<$native_type> for YdbValue {
            fn from(value: $native_type)->Self {
                $ydb_value_kind_first(value)
            }
        }

        simple_convert_optional!($native_type, from_native);
    };
}

macro_rules! simple_convert_optional {
    ($native_type:ty) => {
        impl TryFrom<YdbValue> for Option<$native_type> {
            type Error = YdbError;

            fn try_from(value: YdbValue) -> Result<Self, Self::Error> {
                match value {
                    YdbValue::Optional(opt_val) => {
                        if let Err(err) = <$native_type as TryFrom<YdbValue>>::try_from(opt_val.t) {
                            return Err(err);
                        };

                        match opt_val.value {
                            Some(val) => Ok(Some(val.try_into()?)),
                            None => Ok(None),
                        }
                    }
                    value => Ok(Some(value.try_into()?)),
                }
            }
        }
    };
    ($native_type:ty, from_native) => {
        simple_convert_optional!($native_type);

        impl From<Option<$native_type>> for YdbValue {
            fn from(from_value: Option<$native_type>) -> Self {
                let t = <$native_type>::default().into();
                let value = match from_value {
                    Some(val) => Some(val.into()),
                    None => None,
                };

                return YdbValue::Optional(Box::new(YdbOptional { t, value }));
            }
        }
    };
}

simple_convert!(i8, YdbValue::Int8);
simple_convert!(u8, YdbValue::Uint8);
simple_convert!(i16, YdbValue::Int16, YdbValue::Int8, YdbValue::Uint8);
simple_convert!(u16, YdbValue::Uint16, YdbValue::Uint8);
simple_convert!(
    i32,
    YdbValue::Int32,
    YdbValue::Int16,
    YdbValue::Uint16,
    YdbValue::Int8,
    YdbValue::Uint8,
);
simple_convert!(u32, YdbValue::Uint32, YdbValue::Uint16, YdbValue::Uint8);
simple_convert!(
    i64,
    YdbValue::Int64,
    YdbValue::Int32,
    YdbValue::Uint32,
    YdbValue::Int16,
    YdbValue::Uint16,
    YdbValue::Int8,
    YdbValue::Uint8,
);
simple_convert!(
    u64,
    YdbValue::Uint64,
    YdbValue::Uint32,
    YdbValue::Uint16,
    YdbValue::Uint8,
);
simple_convert!(
    String,
    YdbValue::Utf8,
    YdbValue::Json,
    YdbValue::JsonDocument,
    YdbValue::Yson
);
simple_convert!(
    Vec<u8>,
    YdbValue::String,
    YdbValue::Utf8,
    YdbValue::Json,
    YdbValue::JsonDocument,
    YdbValue::Yson
);
simple_convert!(f32, YdbValue::Float);
simple_convert!(f64, YdbValue::Double, YdbValue::Float);
simple_convert!(
    Duration,
    YdbValue::Date,
    YdbValue::DateTime,
    YdbValue::Timestamp
);

impl TryFrom<YdbValue> for SystemTime {
    type Error = YdbError;

    fn try_from(value: YdbValue) -> Result<Self, Self::Error> {
        fn duration_to_system_time(val: Duration) -> Result<SystemTime, YdbError> {
            match SystemTime::UNIX_EPOCH.checked_add(val) {
                Some(res) => Ok(res),
                None => Err(YdbError::Convert(format!(
                    "error while convert ydb duration to system time"
                ))),
            }
        }

        match value {
            YdbValue::Date(val) => duration_to_system_time(val),
            YdbValue::DateTime(val) => duration_to_system_time(val),
            YdbValue::Timestamp(val) => duration_to_system_time(val),
            value => Err(YdbError::Convert(format!(
                "failed to convert from {} to {}",
                value.kind_static(),
                type_name::<Self>(),
            ))),
        }
    }
}

impl TryFrom<SystemTime> for YdbValue {
    type Error = YdbError;

    fn try_from(value: SystemTime) -> Result<Self, Self::Error> {
        let unix = value.duration_since(UNIX_EPOCH)?;
        return Ok(unix.into());
    }
}

simple_convert_optional!(SystemTime);

impl TryFrom<Option<SystemTime>> for YdbValue {
    type Error = YdbError;

    fn try_from(from_value: Option<SystemTime>) -> Result<Self, Self::Error> {
        let duration = match from_value {
            Some(val) => Some(val.duration_since(UNIX_EPOCH)?),
            None => None,
        };

        return Ok(duration.into());
    }
}
