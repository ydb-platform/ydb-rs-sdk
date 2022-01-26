use crate::errors::YdbError;
use crate::types::YdbValue;
use std::any::type_name;
use std::time::{Duration, SystemTime};

macro_rules! try_from_simple {
    ($target_type:ty, $($variant:path),+) => {
        impl TryFrom<YdbValue> for $target_type {
            type Error = YdbError;

            fn try_from(value: YdbValue) -> Result<Self, Self::Error> {
                match value {
                    $($variant(val) => Ok(val.into()),)+
                    value => Err(YdbError::Convert(format!(
                        "failed to convert from {} to {}",
                        value.kind_static(),
                        type_name::<Self>(),
                    ))),
                }
            }
        }

        try_from_optional!($target_type, $($variant),+);
    };
}

macro_rules! try_from_optional {
    ($target_type:ty, $($variant:path),+) => {
        impl TryFrom<YdbValue> for Option<$target_type> {
            type Error = YdbError;

            fn try_from(value: YdbValue) -> Result<Self, Self::Error> {
                match value {
                    YdbValue::Optional(opt_val) => match opt_val.value {
                        Some(val) => Ok(Some(val.try_into()?)),
                        None => Ok(None),
                    },
                    value => Ok(Some(value.try_into()?)),
                }
            }
        }
    };
}

try_from_simple!(i8, YdbValue::Int8);
try_from_simple!(u8, YdbValue::Uint8);
try_from_simple!(i16, YdbValue::Int8, YdbValue::Uint8, YdbValue::Int16);
try_from_simple!(u16, YdbValue::Uint8, YdbValue::Uint16);
try_from_simple!(
    i32,
    YdbValue::Int8,
    YdbValue::Uint8,
    YdbValue::Int16,
    YdbValue::Uint16,
    YdbValue::Int32
);
try_from_simple!(u32, YdbValue::Uint8, YdbValue::Uint16, YdbValue::Uint32);
try_from_simple!(
    i64,
    YdbValue::Int8,
    YdbValue::Uint8,
    YdbValue::Int16,
    YdbValue::Uint16,
    YdbValue::Int32,
    YdbValue::Uint32,
    YdbValue::Int64
);
try_from_simple!(
    u64,
    YdbValue::Uint8,
    YdbValue::Uint16,
    YdbValue::Uint32,
    YdbValue::Uint64
);
try_from_simple!(
    String,
    YdbValue::Utf8,
    YdbValue::Json,
    YdbValue::JsonDocument,
    YdbValue::Yson
);
try_from_simple!(
    Vec<u8>,
    YdbValue::String,
    YdbValue::Utf8,
    YdbValue::Json,
    YdbValue::JsonDocument,
    YdbValue::Yson
);
try_from_simple!(f32, YdbValue::Float);
try_from_simple!(f64, YdbValue::Float, YdbValue::Double);
try_from_simple!(
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
try_from_optional!(
    SystemTime,
    YdbValue::Date,
    YdbValue::DateTime,
    YdbValue::Timestamp
);
