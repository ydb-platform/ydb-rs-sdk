use crate::types::YdbValue;

impl TryFrom<YdbValue> for i8 {
    type Error = crate::errors::YdbError;

    fn try_from(value: YdbValue) -> Result<Self, Self::Error> {
        todo!()
    }
}
